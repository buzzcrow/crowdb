// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{decode_manifest, PublicationMode};
use crate::{
    AuthorityError, CasOutcome, DatasetAuthority, DatasetError, DatasetIdentity, OperationId, SnapshotId,
    SplitRevisionId,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CandidateBinding {
    pub version: u16,
    pub snapshot: SnapshotId,
    pub parent: Option<SnapshotId>,
    pub operation: OperationId,
    pub manifest_sha256: [u8; 32],
    pub initial_split: Option<SplitRevisionId>,
}

impl DatasetAuthority {
    /// Prepares complete immutable content from an available parent or a new root.
    /// Callers supplying external chunk locators must acquire their payload owners first.
    /// The expected Dataset head belongs to a later commit attempt.
    /// # Errors
    /// Rejects invalid content, unavailable parents and divergent operation retries.
    pub async fn prepare_snapshot_candidate(
        &self,
        identity: &DatasetIdentity,
        parent: Option<SnapshotId>,
        manifest: Vec<u8>,
        operation: OperationId,
    ) -> Result<SnapshotId, AuthorityError> {
        if manifest.len() > 8 * 1024 * 1024 {
            return Err(DatasetError::ReadLimitExceeded.into());
        }
        decode_manifest(&manifest)?.validate()?;
        if let Some(parent) = parent {
            self.require_committed_snapshot(identity, parent).await?;
        }
        let snapshot = self
            .write_publication(
                identity,
                parent,
                manifest,
                operation,
                PublicationMode::Candidate(None),
            )
            .await?;
        self.complete_snapshot_candidate(identity, snapshot).await?;
        Ok(snapshot)
    }

    pub(crate) async fn prepare_candidate_with_split(
        &self,
        identity: &DatasetIdentity,
        parent: Option<SnapshotId>,
        manifest: Vec<u8>,
        operation: OperationId,
        initial: Option<SplitRevisionId>,
    ) -> Result<SnapshotId, AuthorityError> {
        if manifest.len() > 8 * 1024 * 1024 {
            return Err(DatasetError::ReadLimitExceeded.into());
        }
        decode_manifest(&manifest)?.validate()?;
        if let Some(parent) = parent {
            self.require_committed_snapshot(identity, parent).await?;
        }
        self.write_publication(
            identity,
            parent,
            manifest,
            operation,
            PublicationMode::Candidate(initial),
        )
        .await
    }

    pub(in crate::authority) async fn bind_snapshot_candidate(
        &self,
        identity: &DatasetIdentity,
        binding: &CandidateBinding,
    ) -> Result<(), AuthorityError> {
        let bytes = serde_json::to_vec(binding).map_err(|_| AuthorityError::Corrupt)?;
        let key = candidate_key(identity, binding.snapshot)?;
        match self.store.compare_exchange(&key, None, &bytes).await? {
            CasOutcome::Applied(_) => Ok(()),
            CasOutcome::Conflict(Some(value)) if value.bytes == bytes => Ok(()),
            CasOutcome::Conflict(Some(value)) => {
                let mut existing = decode_candidate(&value.bytes, binding.snapshot)?;
                existing.version = binding.version;
                if existing == *binding {
                    Ok(())
                } else {
                    Err(AuthorityError::AlreadyExists)
                }
            }
            CasOutcome::Conflict(_) => Err(AuthorityError::AlreadyExists),
        }
    }

    pub(crate) async fn snapshot_candidate(
        &self,
        identity: &DatasetIdentity,
        snapshot: SnapshotId,
    ) -> Result<Option<CandidateBinding>, AuthorityError> {
        let Some(value) = self.store.get(&candidate_key(identity, snapshot)?).await? else {
            return Ok(None);
        };
        decode_candidate(&value.bytes, snapshot).map(Some)
    }
}

pub(crate) fn decode_candidate(
    bytes: &[u8],
    snapshot: SnapshotId,
) -> Result<CandidateBinding, AuthorityError> {
    if bytes == super::complete::RETIRED {
        return Err(AuthorityError::SnapshotProtected);
    }
    if bytes.len() > 4096 {
        return Err(AuthorityError::Corrupt);
    }
    let binding: CandidateBinding = serde_json::from_slice(bytes).map_err(|_| AuthorityError::Corrupt)?;
    if !matches!(binding.version, 1 | 2) || binding.snapshot != snapshot || binding.parent == Some(snapshot) {
        return Err(AuthorityError::Corrupt);
    }
    SnapshotId::from_bytes(binding.snapshot.as_bytes())?;
    OperationId::from_bytes(binding.operation.as_bytes())?;
    if let Some(parent) = binding.parent {
        SnapshotId::from_bytes(parent.as_bytes())?;
    }
    if let Some(revision) = binding.initial_split {
        SplitRevisionId::from_bytes(revision.as_bytes())?;
    }
    Ok(binding)
}

pub(in crate::authority) fn binding(
    snapshot: SnapshotId,
    parent: Option<SnapshotId>,
    operation: OperationId,
    manifest: &[u8],
    initial_split: Option<SplitRevisionId>,
) -> CandidateBinding {
    CandidateBinding {
        version: 1,
        snapshot,
        parent,
        operation,
        manifest_sha256: Sha256::digest(manifest).into(),
        initial_split,
    }
}
pub(crate) fn candidate_key(
    identity: &DatasetIdentity,
    snapshot: SnapshotId,
) -> Result<Vec<u8>, DatasetError> {
    let mut key = b"DS\0CB\0".to_vec();
    key.extend_from_slice(&identity.key()?);
    key.extend_from_slice(snapshot.as_bytes());
    Ok(key)
}

impl DatasetAuthority {
    pub(in crate::authority) async fn bind_completed_manifest(
        &self,
        identity: &DatasetIdentity,
        snapshot_record: &crate::SnapshotRecord,
        manifest: &crate::ManifestRecord,
    ) -> Result<(), AuthorityError> {
        use super::super::encode;
        use crate::{key, PublicationState};
        let snapshot = snapshot_record.publication.snapshot;
        if let Some(head) = self.store.get(&key::head(identity)?).await? {
            self.help_head_commit(identity, &head).await?;
        }
        if snapshot_record.publication.state == PublicationState::Published
            && self.snapshot_candidate(identity, snapshot).await?.is_none()
        {
            match self.require_committed_snapshot(identity, snapshot).await {
                Ok(()) | Err(AuthorityError::SnapshotNotFound) => {}
                Err(error) => return Err(error),
            }
        }
        let candidate_manifest = if snapshot_record.publication.state == PublicationState::Prepared {
            encode(manifest)?
        } else {
            snapshot_record.publication.manifest.clone()
        };
        self.bind_snapshot_candidate(
            identity,
            &binding(
                snapshot,
                snapshot_record.publication.parent,
                snapshot_record.publication.operation,
                &candidate_manifest,
                None,
            ),
        )
        .await?;
        Ok(())
    }
}
