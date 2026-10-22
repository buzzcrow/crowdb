// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_protocol::chunk_stream::{
    LegacyExtentPageView, LegacyManifestView, StreamMigrationPhase, StreamMigrationProgress,
    MAX_STREAM_DIRECTORY_LOCATIONS,
};
use crowdb_protocol::key::{BinaryKey, StreamExtentPageKey};

use super::{kv_error, migration_error, DirectoryMetadataStore, GetOutcome, StreamMigrationSnapshot};
use crate::{
    DirectoryHeadSnapshot, DirectoryIndex, DirectoryMutation, Result, StreamChunkStore, StreamError,
};

#[derive(Debug)]
pub enum StreamMigrationStep {
    Progress {
        snapshot: StreamMigrationSnapshot,
        progress: Box<StreamMigrationProgress>,
    },
    Complete(DirectoryHeadSnapshot),
}

impl DirectoryMetadataStore {
    /// Perform at most one bounded conversion batch. History, sealed legacy tail
    /// scanning and final publication remain fenced by this exact session/revision.
    ///
    /// # Errors
    /// Returns stale-session, malformed legacy data, IO, publication or budget errors.
    pub async fn advance_directory_migration(
        &self,
        snapshot: &StreamMigrationSnapshot,
        chunks: &dyn StreamChunkStore,
        max_locations: usize,
    ) -> Result<StreamMigrationStep> {
        if max_locations == 0 || max_locations > MAX_STREAM_DIRECTORY_LOCATIONS {
            return Err(StreamError::InvalidRequest(
                "migration location budget is outside its bound".into(),
            ));
        }
        let previous = self.load_directory_migration_progress(snapshot).await?;
        let mut next = match &previous {
            Some(progress) => progress.clone(),
            None => initial_progress(snapshot)?,
        };
        if next.phase == StreamMigrationPhase::Complete {
            return self
                .finish_directory_migration(snapshot, &next)
                .await
                .map(StreamMigrationStep::Complete);
        }
        let mutation = match next.phase {
            StreamMigrationPhase::History { .. } => {
                self.convert_migration_history(snapshot, &mut next, chunks, max_locations)
                    .await?
            }
            StreamMigrationPhase::Tail { .. } => {
                self.convert_migration_tail(snapshot, &mut next, chunks, max_locations)
                    .await?
            }
            StreamMigrationPhase::Complete => unreachable!(),
        };
        next.sequence = next
            .sequence
            .checked_add(1)
            .ok_or_else(|| migration_error("conversion sequence overflows"))?;
        let snapshot = self
            .store_migration_progress(snapshot, previous.as_ref(), &next, &mutation)
            .await?;
        Ok(StreamMigrationStep::Progress {
            snapshot,
            progress: Box::new(next),
        })
    }

    async fn convert_migration_history(
        &self,
        snapshot: &StreamMigrationSnapshot,
        progress: &mut StreamMigrationProgress,
        chunks: &dyn StreamChunkStore,
        limit: usize,
    ) -> Result<DirectoryMutation> {
        let source = snapshot.source_view()?;
        let StreamMigrationPhase::History {
            mut fence,
            mut extent,
        } = progress.phase
        else {
            unreachable!()
        };
        let index = DirectoryIndex::new(
            snapshot.marker.stream_name,
            snapshot.marker.purpose,
            progress.root,
        )?;
        if fence == source.fence_count() as u64 {
            self.prepare_migration_tail(&source, progress, chunks).await?;
            return index.prepare_append(self, &[]).await;
        }
        let mut locations = Vec::with_capacity(limit);
        while locations.len() < limit && fence < source.fence_count() as u64 {
            let ordinal = usize::try_from(fence).map_err(migration_error)?;
            let expected = source.fence(ordinal).map_err(migration_error)?;
            let key = StreamExtentPageKey {
                stream_name: source.stream_name,
                writer_epoch: source.writer_epoch,
                generation: source.generation,
                page_index: expected.page_index,
            }
            .to_bytes();
            let GetOutcome::Found { value, .. } = self.records.get(&key).await.map_err(kv_error)? else {
                return Err(StreamError::ReadUnavailable(
                    "legacy source extent page is absent".into(),
                ));
            };
            let page = LegacyExtentPageView::from_raw(&value).map_err(migration_error)?;
            validate_source_page(&source, &expected, &page)?;
            let mut row = usize::try_from(extent).map_err(migration_error)?;
            if row >= page.extent_count() {
                return Err(migration_error("conversion row is outside source page"));
            }
            while locations.len() < limit && row < page.extent_count() {
                locations.push(page.location(row).map_err(migration_error)?);
                row += 1;
            }
            if row == page.extent_count() {
                fence += 1;
                extent = 0;
            } else {
                extent = row as u64;
            }
        }
        if locations
            .first()
            .is_some_and(|first| first.logical_start != progress.logical_tail)
        {
            return Err(migration_error("legacy source pages have a logical gap"));
        }
        let mutation = index.prepare_append(self, &locations).await?;
        progress.root = mutation.index().root();
        progress.logical_tail = progress
            .root
            .map_or(progress.logical_tail, |root| root.end_logical);
        progress.phase = StreamMigrationPhase::History { fence, extent };
        if fence == source.fence_count() as u64 {
            progress.legacy_root = progress.root;
        }
        Ok(mutation)
    }
}

fn initial_progress(snapshot: &StreamMigrationSnapshot) -> Result<StreamMigrationProgress> {
    let source = snapshot.source_view()?;
    let logical_tail = if source.fence_count() == 0 {
        source.sealed_tail
    } else {
        source.fence(0).map_err(migration_error)?.first_logical
    };
    Ok(StreamMigrationProgress {
        marker: snapshot.marker.clone(),
        sequence: 0,
        root: None,
        legacy_root: None,
        logical_tail,
        phase: StreamMigrationPhase::History { fence: 0, extent: 0 },
    })
}

fn validate_source_page(
    source: &LegacyManifestView<'_>,
    fence: &crowdb_protocol::chunk_stream::StreamExtentPageFence,
    page: &LegacyExtentPageView<'_>,
) -> Result<()> {
    if page.stream_name != source.stream_name
        || page.writer_epoch != source.writer_epoch
        || page.generation != source.generation
        || page.page_index != fence.page_index
        || page.location(0).map_err(migration_error)?.logical_start != fence.first_logical
        || page
            .location(page.extent_count() - 1)
            .map_err(migration_error)?
            .end_logical()
            .map_err(migration_error)?
            != fence.end_logical
    {
        return Err(migration_error("source extent page identity or fences differ"));
    }
    Ok(())
}
