// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{
    binding::{inspection_error, split_error},
    state::{budget, decode, encode, page_key, BoundSplitSpec, IdPage, PreparationProgress},
    ReadPreparation, SessionSpecV2,
};
use crate::{
    inspection::work::InspectionWork, AuthorityError, DatasetError, DatasetIdentity, DatasetSessions,
    ReadCancellation,
};
use sha2::{Digest, Sha256};

impl DatasetSessions {
    /// Resolves aliases once and advances one bounded effective-selection/Split window.
    /// Complete preparation freezes ordering and worker membership before payload reads.
    /// # Errors
    /// Rejects conflicting specs/pages, unavailable revisions, invalid rules and work limits.
    pub async fn prepare_split_read(
        &self,
        identity: &DatasetIdentity,
        spec: &SessionSpecV2,
        expected_page: u64,
        now: u64,
        cancel: &ReadCancellation,
    ) -> Result<ReadPreparation, AuthorityError> {
        let result = self
            .prepare_split_read_step(identity, spec, expected_page, now, cancel)
            .await;
        if cancel.is_cancelled() {
            match self
                .bound_split_read_spec(identity, spec.read.id, &ReadCancellation::new())
                .await
            {
                Ok(bound) => {
                    self.authority
                        .release_read_handle(identity, bound.snapshot, spec.read.id)
                        .await?;
                }
                Err(AuthorityError::NotFound) => {}
                Err(error) => return Err(error),
            }
            return Err(DatasetError::ReadCancelled.into());
        }
        result
    }
    async fn prepare_split_read_step(
        &self,
        identity: &DatasetIdentity,
        spec: &SessionSpecV2,
        expected_page: u64,
        now: u64,
        cancel: &ReadCancellation,
    ) -> Result<ReadPreparation, AuthorityError> {
        if cancel.is_cancelled() {
            return Err(DatasetError::ReadCancelled.into());
        }
        let bound = self.bind_split_read_spec(identity, spec, now, cancel).await?;
        let (old, progress) = self
            .split_preparation_progress(identity, spec.read.id, cancel)
            .await?;
        if progress.cancelled {
            return Err(DatasetError::ReadCancelled.into());
        }
        self.authority
            .require_committed_snapshot(identity, bound.snapshot)
            .await?;
        self.authority
            .touch_read_handle(identity, bound.snapshot, spec.read.id, now, super::super::TTL)
            .await?;
        match self.record(identity, spec.read.id).await {
            Ok(_) => return self.prepared_split_read(identity, &bound, &progress, now).await,
            Err(AuthorityError::NotFound) => {}
            Err(error) => return Err(error),
        }
        if expected_page > progress.next_page {
            return Err(AuthorityError::CursorConflict);
        }
        if progress.complete {
            return self
                .finish_split_read(identity, &bound, &progress, now, cancel)
                .await;
        }
        if expected_page < progress.next_page {
            return Ok(preparation(&bound, &progress));
        }
        let page_key = page_key(identity, spec.read.id, expected_page)?;
        let stored = self
            .authority
            .store
            .get_bounded(&page_key, budget(), cancel)
            .await?;
        let page = if let Some(stored) = stored {
            decode::<IdPage>(&stored.bytes)?
        } else {
            let page = self
                .select_split_read_window(identity, &bound, &progress, cancel)
                .await?;
            self.immutable_session_metadata(&page_key, &encode(&page)?)
                .await?;
            page
        };
        let bytes = encode(&page)?;
        let mut next = progress.clone();
        next.admitted = next
            .admitted
            .checked_add(page.ids.len() as u64)
            .ok_or(DatasetError::ReadLimitExceeded)?;
        next.bytes = next
            .bytes
            .checked_add(bytes.len() as u64)
            .ok_or(DatasetError::ReadLimitExceeded)?;
        if next.admitted > 100_000 || next.bytes > 8 * 1024 * 1024 {
            return Err(DatasetError::ReadLimitExceeded.into());
        }
        let mut hash = Sha256::new();
        hash.update(next.binding);
        hash.update(Sha256::digest(&bytes));
        next.binding = hash.finalize().into();
        next.next_page = next
            .next_page
            .checked_add(1)
            .ok_or(DatasetError::ReadLimitExceeded)?;
        next.after = page.after;
        next.complete = next.after.is_none();
        self.replace_split_preparation(identity, spec.read.id, old.as_ref(), &next)
            .await?;
        if next.complete {
            self.finish_split_read(identity, &bound, &next, now, cancel).await
        } else {
            Ok(preparation(&bound, &next))
        }
    }
    async fn select_split_read_window(
        &self,
        identity: &DatasetIdentity,
        bound: &BoundSplitSpec,
        progress: &PreparationProgress,
        cancel: &ReadCancellation,
    ) -> Result<IdPage, AuthorityError> {
        let mut page = self
            .authority
            .inspection_selection_page(
                identity,
                bound.snapshot,
                &bound.spec.read.selection,
                progress.after.clone(),
                budget(),
                cancel,
            )
            .await
            .map_err(inspection_error)?;
        if let Some(source) = &bound.split {
            let mut work = InspectionWork::new(self.authority.store.as_ref(), cancel, budget())
                .map_err(inspection_error)?;
            let revision = self
                .authority
                .read_split_revision(identity, source.snapshot, source.revision, &mut work)
                .await
                .map_err(split_error)?;
            let mut admitted = Vec::new();
            for id in page.entries {
                if self
                    .authority
                    .frozen_split_contains(identity, &revision, &source.name, &id, &mut work)
                    .await
                    .map_err(split_error)?
                {
                    admitted.push(id);
                }
            }
            page.entries = admitted;
        }
        Ok(IdPage {
            ids: page.entries,
            after: page.after,
        })
    }
    pub(super) async fn prepared_split_read(
        &self,
        identity: &DatasetIdentity,
        bound: &BoundSplitSpec,
        progress: &PreparationProgress,
        now: u64,
    ) -> Result<ReadPreparation, AuthorityError> {
        let mut preparation = preparation(bound, progress);
        preparation.session = Some(self.resume(identity, bound.spec.read.id, now).await?);
        preparation.complete = true;
        Ok(preparation)
    }
}
pub(super) fn preparation(bound: &BoundSplitSpec, progress: &PreparationProgress) -> ReadPreparation {
    ReadPreparation {
        snapshot: bound.snapshot,
        split: bound.split.clone(),
        next_page: progress.next_page,
        admitted: progress.admitted,
        complete: false,
        session: None,
    }
}
