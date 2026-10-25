// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crate::{
    key, AuthorityError, CasOutcome, DatasetAuthority, DatasetIdentity, MetadataScanBudget, OperationId,
    ReadCancellation, SnapshotId, SnapshotRecord,
};
use std::time::Duration;

pub(super) const RETIRED: &[u8] = b"retired-candidate-operation-v1";

impl DatasetAuthority {
    /// Resolves a candidate operation or seals the first missing preparation record.
    /// A delayed publisher cannot reserve the operation or create a Snapshot afterward.
    pub(crate) async fn fence_candidate_reservation(
        &self,
        identity: &DatasetIdentity,
        operation: OperationId,
    ) -> Result<Option<SnapshotRecord>, AuthorityError> {
        tokio::time::timeout(
            Duration::from_secs(2),
            self.fence_candidate_reservation_inner(identity, operation),
        )
        .await
        .map_err(|_| crate::DatasetError::ReadLimitExceeded)?
    }
    async fn fence_candidate_reservation_inner(
        &self,
        identity: &DatasetIdentity,
        operation: OperationId,
    ) -> Result<Option<SnapshotRecord>, AuthorityError> {
        let operation_key = key::operation(identity, operation)?;
        let snapshot = match self.seal_missing_candidate_key(&operation_key, 1024).await? {
            Some(value) => SnapshotId::from_bytes(&value)?,
            None => SnapshotId::from_bytes(operation.as_bytes())?,
        };
        let Some(value) = self
            .seal_missing_candidate_key(&key::snapshot(identity, snapshot)?, 9 * 1024 * 1024)
            .await?
        else {
            return Ok(None);
        };
        let record = crate::authority::snapshot_resource::decode_snapshot(&value)?;
        if record.publication.snapshot != snapshot || record.publication.operation != operation {
            return Err(AuthorityError::Corrupt);
        }
        Ok(Some(record))
    }
    async fn seal_missing_candidate_key(
        &self,
        key: &[u8],
        bytes: usize,
    ) -> Result<Option<Vec<u8>>, AuthorityError> {
        let stored = self
            .store
            .get_bounded(
                key,
                MetadataScanBudget {
                    items: 1,
                    bytes,
                    timeout: Duration::from_secs(2),
                },
                &ReadCancellation::new(),
            )
            .await?;
        if let Some(value) = stored {
            return Ok((value.bytes != RETIRED).then_some(value.bytes));
        }
        match self.store.compare_exchange(key, None, RETIRED).await? {
            CasOutcome::Applied(_) => Ok(None),
            CasOutcome::Conflict(Some(value)) if value.bytes == RETIRED => Ok(None),
            CasOutcome::Conflict(_) => Err(AuthorityError::HeadConflict),
        }
    }
}
