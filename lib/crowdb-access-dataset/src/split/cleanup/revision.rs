// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crate::{
    inspection::work::InspectionWork, split::SplitLifecycleError, split::SplitRevisionRecord, AuthorityError,
    CasOutcome, DatasetAuthority, DatasetIdentity, OperationId, SnapshotId, SplitRevisionId,
};

impl DatasetAuthority {
    pub(in crate::split::cleanup) async fn fence_split_cleanup_revision(
        &self,
        identity: &DatasetIdentity,
        summary: &crate::split::SplitRevisionSummary,
        retired: bool,
        work: &mut InspectionWork<'_>,
    ) -> Result<bool, SplitLifecycleError> {
        let preserve = if retired {
            let key = crate::split::key::revision(identity, summary.snapshot, summary.revision)
                .map_err(AuthorityError::from)?;
            self.seal_split_key(&key, work).await?;
            false
        } else {
            self.fence_incomplete_split_revision(
                identity,
                summary.snapshot,
                summary.revision,
                summary.operation,
                work,
            )
            .await?
        };
        if !preserve {
            let key = crate::split::key::revision_summary(identity, summary.snapshot, summary.revision)
                .map_err(AuthorityError::from)?;
            self.seal_split_value(&key, &crate::split::codec::encode(summary)?, work)
                .await?;
        }
        Ok(preserve)
    }
    /// A complete root and a cleanup fence compete on the same immutable key.
    /// Once the root wins, scratch cleanup cannot retire its member pages.
    async fn fence_incomplete_split_revision(
        &self,
        identity: &DatasetIdentity,
        snapshot: SnapshotId,
        revision: SplitRevisionId,
        operation: OperationId,
        work: &mut InspectionWork<'_>,
    ) -> Result<bool, SplitLifecycleError> {
        let key = crate::split::key::revision(identity, snapshot, revision).map_err(AuthorityError::from)?;
        if let Some(value) = work.get(&key, super::RECORD_CAP).await? {
            return complete(&value.bytes, snapshot, revision, operation);
        }
        match self.store.compare_exchange(&key, None, super::SEALED).await? {
            CasOutcome::Applied(_) => Ok(false),
            CasOutcome::Conflict(Some(observed)) => complete(&observed.bytes, snapshot, revision, operation),
            CasOutcome::Conflict(None) => Err(SplitLifecycleError::Conflict),
        }
    }
}

fn complete(
    bytes: &[u8],
    snapshot: SnapshotId,
    revision: SplitRevisionId,
    operation: OperationId,
) -> Result<bool, SplitLifecycleError> {
    if bytes == super::SEALED {
        return Ok(false);
    }
    let record = SplitRevisionRecord::decode(bytes)?;
    if record.snapshot != snapshot || record.revision != revision || record.operation != operation {
        return Err(SplitLifecycleError::Corrupt);
    }
    Ok(true)
}
