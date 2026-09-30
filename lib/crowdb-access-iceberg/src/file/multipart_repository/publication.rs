use crate::catalog::CatalogError;
use crate::error::ValidationError;
use crate::file::{
    file_key, FileContent, FileRecord, FileRepository, FileTree, MultipartPhase, MultipartSelection,
    MultipartSession, MultipartStreamPart, SelectedPart, SelectedStreamPart,
};
use crate::operation::PayloadStore;
use crate::record::StorageRecord;
use crowdb_access_multipart::MultipartComposer;

use super::{check_live, increment, MultipartRepository};

fn raw_md5(etag: &str) -> Result<[u8; 16], ValidationError> {
    let mut raw = [0_u8; 16];
    if etag.len() != 32 {
        return Err(ValidationError::Record);
    }
    for (byte, pair) in raw.iter_mut().zip(etag.as_bytes().chunks_exact(2)) {
        let pair = std::str::from_utf8(pair).map_err(|_| ValidationError::Record)?;
        *byte = u8::from_str_radix(pair, 16).map_err(|_| ValidationError::Record)?;
    }
    Ok(raw)
}

impl MultipartRepository {
    /// Publishes selected durable part locations without reading or rewriting part bytes.
    /// # Errors
    /// Rejects changed parts, mixed storage formats, oversized descriptors and stale sessions.
    pub async fn prepare_stream_publication(
        &self,
        session: &MultipartSession,
        now_ms: u64,
    ) -> Result<Option<bool>, CatalogError> {
        session.validate()?;
        check_live(session, now_ms)?;
        if session.phase != MultipartPhase::Completing {
            return Err(CatalogError::Conflict);
        }
        let completion = session.completion.as_ref().ok_or(ValidationError::Record)?;
        if completion.progress.next_part != 0 {
            return Ok(None);
        }
        if self.load(session.context, session.upload).await?.as_ref() != Some(session) {
            return Ok(Some(false));
        }
        let bytes = PayloadStore::new(self.store.clone())
            .get(&completion.selection)
            .await?;
        let selection = MultipartSelection::decode(&bytes)?;
        let mut composer = MultipartComposer::new(session.limits.max_file_bytes);
        for (index, selected) in selection.parts().iter().enumerate() {
            let snapshot = selection.snapshots().and_then(|snapshots| snapshots.get(index));
            let Some(stream) = self.selected_stream(session, selected, snapshot).await? else {
                return Ok(None);
            };
            let etag = stream.content.etag().ok_or(ValidationError::Record)?;
            let raw_md5 = raw_md5(etag)?;
            let locations = stream
                .content
                .locations(stream.length)?
                .ok_or(ValidationError::Record)?;
            composer
                .push(stream.length, raw_md5, &locations)
                .map_err(|_| ValidationError::Record)?;
        }
        let assembled = composer.finish().map_err(|_| ValidationError::Record)?;
        let record = FileRecord::from_uploaded_locations(
            session.owner.file,
            session.location.clone(),
            &assembled.locations,
            assembled.length,
            assembled.etag,
        )?;
        let value = StorageRecord::File(Box::new(record)).encode()?;
        let publication = PayloadStore::new(self.store.clone())
            .put(session.context.catalog, session.upload, &value)
            .await?;
        let mut next = increment(session)?;
        next.phase = MultipartPhase::Publishing;
        let completion = next.completion.as_mut().ok_or(ValidationError::Record)?;
        completion.progress.next_part = selection.count();
        completion.progress.completed_bytes = assembled.length;
        completion.publication = Some(publication);
        Ok(Some(self.exchange(session, &next).await?))
    }

    async fn selected_stream(
        &self,
        session: &MultipartSession,
        selected: &SelectedPart,
        snapshot: Option<&SelectedStreamPart>,
    ) -> Result<Option<MultipartStreamPart>, CatalogError> {
        if let Some(snapshot) = snapshot {
            return Ok(Some(MultipartStreamPart {
                length: snapshot.length,
                content: FileContent::Locations {
                    bytes: snapshot.bytes.clone(),
                    etag: snapshot.etag.clone(),
                },
            }));
        }
        let part = self
            .part(session, selected.number)
            .await?
            .ok_or(ValidationError::Record)?;
        if part.revision != selected.revision || part.selection_digest() != selected.digest {
            return Err(ValidationError::Record.into());
        }
        Ok(part.stream)
    }
    /// Freezes a semantically sealed record; callers must validate its canonical format first.
    /// # Errors
    /// Rejects incomplete assembly, changed byte identity, expiry and invalid file records.
    pub async fn prepare_publication(
        &self,
        session: &MultipartSession,
        tree: &FileTree,
        sealed: &FileRecord,
        now_ms: u64,
    ) -> Result<bool, CatalogError> {
        session.validate()?;
        check_live(session, now_ms)?;
        if session.phase != MultipartPhase::Completing {
            return Err(CatalogError::Conflict);
        }
        validate_candidate(session, tree, sealed)?;
        session.revision.checked_add(2).ok_or(ValidationError::Record)?;
        let mut next = increment(session)?;
        if self.load(session.context, session.upload).await?.as_ref() != Some(session) {
            return Ok(false);
        }
        let bytes = StorageRecord::File(Box::new(sealed.clone())).encode()?;
        let publication = PayloadStore::new(self.store.clone())
            .put(session.context.catalog, session.upload, &bytes)
            .await?;
        next.phase = MultipartPhase::Publishing;
        let completion = next.completion.as_mut().ok_or(ValidationError::Record)?;
        completion.candidate = Some(tree.clone());
        completion.publication = Some(publication);
        self.exchange(session, &next).await
    }

    /// Publishes a frozen seal or replays the exact previously selected file identity.
    /// # Errors
    /// Rejects changed intent, immutable-location conflicts and uncertain writes.
    pub async fn publish(&self, session: &MultipartSession) -> Result<Option<FileRecord>, CatalogError> {
        session.validate()?;
        if !matches!(
            session.phase,
            MultipartPhase::Publishing | MultipartPhase::Published
        ) {
            return Err(CatalogError::Conflict);
        }
        if self.load(session.context, session.upload).await?.as_ref() != Some(session) {
            return Ok(None);
        }
        let candidate = self.publication_intent(session).await?;
        let files = FileRepository::new(self.store.clone());
        if session.phase == MultipartPhase::Published {
            let selected = files
                .load(session.context, &session.location)
                .await?
                .ok_or(ValidationError::Record)?;
            if Some(selected.file) != session.published
                || selected.length != candidate.length
                || selected.digest != candidate.digest
                || selected.kind != candidate.kind
                || selected.format != candidate.format
                || selected.content != candidate.content
            {
                return Err(ValidationError::Record.into());
            }
            return Ok(Some(selected));
        }
        let mut next = increment(session)?;
        let selected = match files.publish(session.context, &candidate).await {
            Ok(selected) => selected,
            Err(CatalogError::Conflict) => {
                self.retain_location_conflict(session, &candidate, &files).await?;
                return Err(CatalogError::Conflict);
            }
            Err(error) => return Err(error),
        };
        next.phase = MultipartPhase::Published;
        next.published = Some(selected.file);
        Ok(self.exchange(session, &next).await?.then_some(selected))
    }

    async fn retain_location_conflict(
        &self,
        session: &MultipartSession,
        candidate: &FileRecord,
        files: &FileRepository,
    ) -> Result<(), CatalogError> {
        if let Some(selected) = files.load(session.context, &session.location).await? {
            if selected.length != candidate.length
                || selected.digest != candidate.digest
                || selected.kind != candidate.kind
                || selected.format != candidate.format
                || selected.content != candidate.content
            {
                let mut next = increment(session)?;
                next.phase = MultipartPhase::Conflicted;
                self.exchange(session, &next).await?;
            }
        }
        Ok(())
    }

    async fn publication_intent(&self, session: &MultipartSession) -> Result<FileRecord, CatalogError> {
        let completion = session.completion.as_ref().ok_or(ValidationError::Record)?;
        let reference = completion.publication.as_ref().ok_or(ValidationError::Record)?;
        let bytes = PayloadStore::new(self.store.clone()).get(reference).await?;
        let key = file_key(session.context.catalog, session.owner.file);
        let StorageRecord::File(record) = StorageRecord::decode(&key, &bytes)? else {
            return Err(ValidationError::Record.into());
        };
        if let Some(tree) = &completion.candidate {
            validate_candidate(session, tree, &record)?;
        } else if record.file != session.owner.file
            || record.location != session.location
            || record.length != completion.progress.completed_bytes
            || !matches!(record.content, FileContent::Locations { .. })
        {
            return Err(ValidationError::Record.into());
        }
        Ok(*record)
    }
}

fn validate_candidate(
    session: &MultipartSession,
    tree: &FileTree,
    sealed: &FileRecord,
) -> Result<(), ValidationError> {
    sealed.validate()?;
    FileContent::Chunks {
        root: tree.root.clone(),
    }
    .validate(tree.length, &tree.digest)?;
    let completion = session.completion.as_ref().ok_or(ValidationError::Record)?;
    if completion.progress.next_part != completion.selected_parts
        || completion.progress.completed_bytes != tree.length
        || sealed.file != session.owner.file
        || sealed.location != session.location
        || sealed.length != tree.length
        || sealed.digest != tree.digest
    {
        return Err(ValidationError::Record);
    }
    if let FileContent::Chunks { root } = &sealed.content {
        if root != &tree.root {
            return Err(ValidationError::Record);
        }
    }
    Ok(())
}
