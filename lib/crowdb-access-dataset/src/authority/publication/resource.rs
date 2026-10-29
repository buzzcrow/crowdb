// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{candidate, identity::PublicationIdentity, retirement, PublicationMode};
use crate::{
    authority::snapshot_resource::{encode_snapshot, SnapshotResource, CANDIDATE_LIFETIME_SECONDS},
    key, AuthorityError, CasOutcome, DatasetAuthority, DatasetError, DatasetIdentity, PublicationState,
    SnapshotId, SnapshotPublication, SnapshotRecord,
};

pub(super) struct PublicationResource {
    pub reused: Option<SnapshotId>,
    pub gc_after: Option<u64>,
}

impl DatasetAuthority {
    pub(super) async fn ensure_publication_resource(
        &self,
        identity: &DatasetIdentity,
        prepared: &SnapshotPublication,
        resolved: &PublicationIdentity,
        mode: PublicationMode,
    ) -> Result<PublicationResource, AuthorityError> {
        let snapshot = prepared.snapshot;
        let snapshot_key = key::snapshot(identity, snapshot)?;
        if let Some(value) = self.store.get(&snapshot_key).await? {
            if value.bytes == retirement::RETIRED {
                return Err(DatasetError::InvalidPublicationState.into());
            }
            let stored = SnapshotResource::decode(&value.bytes)?;
            return Ok(PublicationResource {
                reused: self
                    .reuse_publication_resource(identity, prepared, &stored.record, mode)
                    .await?,
                gc_after: stored.gc_after,
            });
        }
        if resolved.retired {
            return Err(DatasetError::InvalidPublicationState.into());
        }
        if !resolved.legacy
            && matches!(
                mode,
                PublicationMode::LegacyPrepare | PublicationMode::LegacyPublish
            )
        {
            let current = self.store.get(&key::head(identity)?).await?;
            let observed = super::super::head::reference(current.as_ref())?;
            if observed.latest != prepared.parent {
                return Err(AuthorityError::SnapshotHeadConflict {
                    observed: Box::new(observed),
                });
            }
        }
        let gc_after = Some(self.reclaim_time().saturating_add(CANDIDATE_LIFETIME_SECONDS));
        let bytes = encode_snapshot(prepared, gc_after)?;
        self.reserve_dataset_resource(identity, snapshot_key.clone())
            .await?;
        match self.store.compare_exchange(&snapshot_key, None, &bytes).await? {
            CasOutcome::Applied(_) => Ok(PublicationResource {
                reused: None,
                gc_after,
            }),
            CasOutcome::Conflict(_) => Err(AuthorityError::AlreadyExists),
        }
    }

    async fn reuse_publication_resource(
        &self,
        identity: &DatasetIdentity,
        prepared: &SnapshotPublication,
        existing: &SnapshotRecord,
        mode: PublicationMode,
    ) -> Result<Option<SnapshotId>, AuthorityError> {
        let snapshot = prepared.snapshot;
        let parent = prepared.parent;
        let operation = prepared.operation;
        let manifest_bytes = &prepared.manifest;

        if existing.publication.operation != operation
            || existing.publication.parent != parent
            || existing.publication.manifest != *manifest_bytes
        {
            return Err(AuthorityError::AlreadyExists);
        }
        if existing.publication.state == PublicationState::Aborted {
            return Err(AuthorityError::Invalid(DatasetError::InvalidPublicationState));
        }
        if let PublicationMode::Resource(initial) = mode {
            if let Some(content) = self.snapshot_candidate(identity, snapshot).await? {
                let reusable = matches!(content.version, 2 | 3);
                let expected = candidate::binding(snapshot, parent, operation, manifest_bytes, initial);
                if !content.same_content(&expected) {
                    return Err(AuthorityError::AlreadyExists);
                }
                // The content binding follows all immutable Manifest and inspection rows.
                // Initial membership and payload owners are checked by final completion.
                if reusable {
                    return Ok(Some(snapshot));
                }
            }
        } else {
            if existing.publication.state == PublicationState::Published
                && (self
                    .snapshot_candidate(identity, snapshot)
                    .await?
                    .is_some_and(|c| c.version == 2)
                    || self.snapshot_commit(identity, snapshot).await?.is_some())
            {
                if matches!(mode, PublicationMode::LegacyPublish) {
                    return self
                        .advance_head(identity, None, parent, snapshot)
                        .await
                        .map(Some);
                }
                return Ok(Some(snapshot));
            }
            if existing.publication.state == PublicationState::Published
                && self.snapshot_candidate(identity, snapshot).await?.is_none()
            {
                match self.require_committed_snapshot(identity, snapshot).await {
                    Ok(()) => return Ok(Some(snapshot)),
                    Err(AuthorityError::SnapshotNotFound) => {}
                    Err(error) => return Err(error),
                }
            }
        }

        Ok(None)
    }
}
