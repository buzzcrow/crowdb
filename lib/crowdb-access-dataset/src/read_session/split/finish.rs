// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{
    binding::inspection_error,
    state::{decode, page_key, BoundSplitSpec, IdPage, PreparationProgress},
    ReadPreparation,
};
use crate::{
    inspection::work::InspectionWork, AuthorityError, CasOutcome, DatasetError, DatasetIdentity,
    DatasetSessions, MetadataScanBudget, ReadCancellation,
};
use sha2::{Digest, Sha256};
use std::time::Duration;

impl DatasetSessions {
    pub(super) async fn finish_split_read(
        &self,
        identity: &DatasetIdentity,
        bound: &BoundSplitSpec,
        progress: &PreparationProgress,
        now: u64,
        cancel: &ReadCancellation,
    ) -> Result<ReadPreparation, AuthorityError> {
        let ids = self
            .collect_split_read_ids(identity, bound, progress, cancel)
            .await?;
        let ids = super::super::planning::arrange_ids(&bound.spec.read, bound.snapshot, ids)?;
        self.save_ids(identity, bound.spec.read.id, &ids).await?;
        let record = split_session_record(identity, bound, ids.len(), now, false)?;
        let key = super::super::session_key(identity, bound.spec.read.id)?;
        match self
            .authority
            .store
            .compare_exchange(&key, None, &super::super::encode(&record)?)
            .await?
        {
            CasOutcome::Applied(_) | CasOutcome::Conflict(Some(_)) => {
                self.prepared_split_read(identity, bound, progress, now).await
            }
            CasOutcome::Conflict(None) => Err(AuthorityError::Corrupt),
        }
    }
    async fn collect_split_read_ids(
        &self,
        identity: &DatasetIdentity,
        bound: &BoundSplitSpec,
        progress: &PreparationProgress,
        cancel: &ReadCancellation,
    ) -> Result<Vec<Vec<u8>>, AuthorityError> {
        let mut work = InspectionWork::new(
            self.authority.store.as_ref(),
            cancel,
            MetadataScanBudget {
                items: 20,
                bytes: 16 * 1024 * 1024,
                timeout: Duration::from_secs(30),
            },
        )
        .map_err(inspection_error)?;
        let mut ids = Vec::new();
        let mut binding = [0; 32];
        for index in 0..progress.next_page {
            let stored = work
                .get(&page_key(identity, bound.spec.read.id, index)?, 1024 * 1024)
                .await
                .map_err(inspection_error)?
                .ok_or(AuthorityError::Corrupt)?;
            let page: IdPage = decode(&stored.bytes)?;
            let mut hash = Sha256::new();
            hash.update(binding);
            hash.update(Sha256::digest(&stored.bytes));
            binding = hash.finalize().into();
            ids.extend(page.ids);
            if ids.len() > 100_000 {
                return Err(DatasetError::ReadLimitExceeded.into());
            }
        }
        if binding != progress.binding
            || ids.len() as u64 != progress.admitted
            || ids.windows(2).any(|pair| pair[0] >= pair[1])
        {
            return Err(AuthorityError::Corrupt);
        }
        Ok(ids)
    }
    /// Cancels preparation or delivery and fences a late finalizer with a closed session record.
    /// # Errors
    /// Returns unknown preparation, corrupt metadata or storage failures.
    pub async fn cancel_split_read(
        &self,
        identity: &DatasetIdentity,
        id: crate::OperationId,
    ) -> Result<crate::ReadSession, AuthorityError> {
        let cancel = ReadCancellation::new();
        let bound = self.bound_split_read_spec(identity, id, &cancel).await?;
        let (old, mut progress) = self.split_preparation_progress(identity, id, &cancel).await?;
        progress.cancelled = true;
        self.replace_split_preparation(identity, id, old.as_ref(), &progress)
            .await?;
        let record = split_session_record(identity, &bound, 0, 0, true)?;
        let key = super::super::session_key(identity, id)?;
        match self
            .authority
            .store
            .compare_exchange(&key, None, &super::super::encode(&record)?)
            .await?
        {
            CasOutcome::Applied(_) | CasOutcome::Conflict(Some(_)) => self.close(identity, id).await,
            CasOutcome::Conflict(None) => Err(AuthorityError::Corrupt),
        }
    }
}
fn split_session_record(
    identity: &DatasetIdentity,
    bound: &BoundSplitSpec,
    count: usize,
    now: u64,
    closed: bool,
) -> Result<super::super::SessionRecord, AuthorityError> {
    let plan = md5::Md5::digest(
        bincode::serialize(&(
            "dataset-read-v2",
            identity.key()?,
            bound.snapshot,
            &bound.split,
            &bound.spec,
        ))
        .map_err(|_| AuthorityError::Corrupt)?,
    )
    .into();
    Ok(super::super::SessionRecord {
        version: 2,
        id: bound.spec.read.id,
        fields: bound.spec.read.fields.clone(),
        batch_size: bound.spec.read.batch_size,
        count,
        snapshot: bound.snapshot,
        plan,
        cursor: crate::OperationId::random().to_string(),
        position: 0,
        issued: None,
        deadline: now.saturating_add(super::super::TTL),
        closed,
    })
}
