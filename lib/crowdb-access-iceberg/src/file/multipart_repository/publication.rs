use crate::catalog::CatalogError;
use crate::error::ValidationError;
use crate::file::{
    file_key, FileContent, FileRecord, FileRepository, FileTree, MultipartPhase, MultipartSession,
};
use crate::operation::PayloadStore;
use crate::record::StorageRecord;

use super::{check_live, increment, MultipartRepository};

impl MultipartRepository {
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
        validate_candidate(
            session,
            completion.candidate.as_ref().ok_or(ValidationError::Record)?,
            &record,
        )?;
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
