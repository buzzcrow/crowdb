// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crate::{
    authority::snapshot_resource::{SnapshotResource, CANDIDATE_LIFETIME_SECONDS},
    key, AuthorityError, CasOutcome, DatasetAuthority, DatasetIdentity, SnapshotId, StoredValue,
};

impl DatasetAuthority {
    pub(in crate::authority::publication) async fn migrate_snapshot_deadline(
        &self,
        identity: &DatasetIdentity,
        snapshot: SnapshotId,
        mut observed: StoredValue,
    ) -> Result<u64, AuthorityError> {
        let candidate = self.snapshot_candidate(identity, snapshot).await?;
        let deadline = candidate
            .as_ref()
            .and_then(|candidate| candidate.expires_at)
            .unwrap_or_else(|| self.reclaim_time().saturating_add(CANDIDATE_LIFETIME_SECONDS));
        for _ in 0..64 {
            let mut resource = SnapshotResource::decode(&observed.bytes)?;
            let publication = &resource.record.publication;
            if publication.snapshot != snapshot {
                return Err(AuthorityError::Corrupt);
            }
            if let Some(deadline) = resource.gc_after {
                return Ok(deadline);
            }
            if let Some(candidate) = &candidate {
                let expected = super::super::candidate::binding(
                    snapshot,
                    publication.parent,
                    publication.operation,
                    &publication.manifest,
                    candidate.initial_split,
                );
                if !candidate.same_content(&expected) {
                    return Err(AuthorityError::Corrupt);
                }
            }
            resource.gc_after = Some(deadline);
            match self
                .store
                .compare_exchange(
                    &key::snapshot(identity, snapshot)?,
                    Some(&observed.bytes),
                    &resource.encode()?,
                )
                .await?
            {
                CasOutcome::Applied(_) => return Ok(deadline),
                CasOutcome::Conflict(Some(value)) => observed = value,
                CasOutcome::Conflict(None) => return Err(AuthorityError::SnapshotNotFound),
            }
        }
        Err(AuthorityError::HeadConflict)
    }
}
