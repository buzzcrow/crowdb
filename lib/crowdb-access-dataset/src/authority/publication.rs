// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

mod candidate;
mod commit;
mod complete;
mod expiry;
pub use expiry::SnapshotExpirySweep;
mod identity;
mod legacy;
mod legacy_candidate;
mod legacy_reclaim;
mod opaque;
mod partitions;
mod receipt;
mod resource;
mod retirement;
mod visibility;

#[derive(Clone, Copy)]
enum PublicationMode {
    LegacyPrepare,
    LegacyPublish,
    Candidate(Option<crate::SplitRevisionId>),
    Resource(Option<crate::SplitRevisionId>),
}

use super::snapshot_resource::decode_snapshot;
use super::{decode, decode_head, decode_manifest, DatasetAuthority};
use crate::{
    key, AuthorityError, DatasetError, DatasetIdentity, ManifestBinding, OperationId, PublicationState,
    SnapshotId, SnapshotPublication, SnapshotRecord,
};

impl DatasetAuthority {
    /// # Errors
    /// Rejects missing Dataset, invalid Manifest, invalid parent, duplicate
    /// candidate, or a publication/head race.
    pub async fn publish_snapshot(
        &self,
        identity: &DatasetIdentity,
        parent: Option<SnapshotId>,
        manifest: Vec<u8>,
    ) -> Result<SnapshotId, AuthorityError> {
        self.publish_snapshot_with_operation(identity, parent, manifest, OperationId::random())
            .await
    }

    /// Publishes complete content using the caller token as the Snapshot identity.
    /// Existing legacy token mappings remain readable; new requests create no mapping.
    /// Latest changes only after the immutable content is complete.
    ///
    /// # Errors
    /// Rejects missing Dataset, invalid Manifest, invalid parent, duplicate
    /// candidate, or a publication/head race.
    #[allow(clippy::too_many_lines)]
    pub async fn publish_snapshot_with_operation(
        &self,
        identity: &DatasetIdentity,
        parent: Option<SnapshotId>,
        manifest_bytes: Vec<u8>,
        operation: OperationId,
    ) -> Result<SnapshotId, AuthorityError> {
        self.write_publication(
            identity,
            parent,
            manifest_bytes,
            operation,
            PublicationMode::LegacyPublish,
        )
        .await
    }

    /// Prepares a validated snapshot without making it visible through latest.
    ///
    /// # Errors
    /// Returns validation, storage, or conflicting operation errors.
    pub async fn prepare_snapshot_with_operation(
        &self,
        identity: &DatasetIdentity,
        parent: Option<SnapshotId>,
        manifest_bytes: Vec<u8>,
        operation: OperationId,
    ) -> Result<SnapshotId, AuthorityError> {
        decode_manifest(&manifest_bytes)?.validate()?;
        self.write_publication(
            identity,
            parent,
            manifest_bytes,
            operation,
            PublicationMode::LegacyPrepare,
        )
        .await
    }

    /// Compatibility selection of pre-built content using the v1 latest-parent condition.
    /// The Snapshot's declared parent is immutable and is not implicitly rebased.
    /// # Errors
    /// Rejects unavailable content, stale conditions and unknown storage outcomes.
    pub async fn publish_prepared_snapshot(
        &self,
        identity: &DatasetIdentity,
        snapshot: SnapshotId,
        expected_parent: Option<SnapshotId>,
    ) -> Result<SnapshotId, AuthorityError> {
        self.advance_head(identity, None, expected_parent, snapshot).await
    }

    /// Returns the Snapshot selected by a caller token, including prepared content.
    ///
    /// # Errors
    /// Returns missing, malformed, or unavailable Snapshot records.
    pub async fn publication_status(
        &self,
        identity: &DatasetIdentity,
        operation: OperationId,
    ) -> Result<SnapshotRecord, AuthorityError> {
        self.get_dataset(identity).await?;
        let resolved = self.publication_identity(identity, operation).await?;
        let record = self
            .store
            .get(&key::snapshot(identity, resolved.snapshot)?)
            .await?
            .ok_or(AuthorityError::SnapshotNotFound)?;
        if record.bytes == retirement::RETIRED {
            return Err(DatasetError::InvalidPublicationState.into());
        }
        decode_snapshot(&record.bytes)
    }

    async fn write_publication(
        &self,
        identity: &DatasetIdentity,
        parent: Option<SnapshotId>,
        manifest_bytes: Vec<u8>,
        operation: OperationId,
        mode: PublicationMode,
    ) -> Result<SnapshotId, AuthorityError> {
        if matches!(mode, PublicationMode::Resource(_)) {
            self.read_dataset_record(identity).await?;
        } else {
            self.get_dataset(identity).await?;
        }
        let mut manifest = match decode_manifest(&manifest_bytes) {
            Ok(manifest) => manifest,
            Err(AuthorityError::Invalid(DatasetError::InvalidManifest))
                if !matches!(mode, PublicationMode::Candidate(_) | PublicationMode::Resource(_)) =>
            {
                return self
                    .prepare_opaque_snapshot(identity, parent, manifest_bytes, operation)
                    .await;
            }
            Err(error) => return Err(error),
        };
        if manifest.samples.is_empty() {
            return Err(DatasetError::InvalidManifest.into());
        }
        manifest.validate()?;
        if let Some(parent) = parent {
            if let Some(schema) = self.manifest_schema(identity, parent).await? {
                if schema.fields.iter().filter(|f| f.required).any(|required| {
                    !manifest
                        .schema
                        .fields
                        .iter()
                        .any(|field| field.name == required.name && field.required)
                }) {
                    return Err(DatasetError::InvalidManifest.into());
                }
            }
        }
        let resolved = if matches!(mode, PublicationMode::Resource(_)) {
            identity::PublicationIdentity::direct(operation)?
        } else {
            self.publication_identity(identity, operation).await?
        };
        let snapshot = resolved.snapshot;

        manifest.snapshot = snapshot;
        manifest.validate()?;
        self.validate_parent(identity, parent).await?;

        let prepared = SnapshotPublication {
            snapshot,
            parent,
            operation,
            manifest: manifest_bytes,
            state: PublicationState::Prepared,
        };
        prepared.validate()?;
        let resource = self
            .ensure_publication_resource(identity, &prepared, &resolved, mode)
            .await?;
        if let Some(snapshot) = resource.reused {
            return Ok(snapshot);
        }

        self.persist_manifest_parts(identity, snapshot, &manifest).await?;
        self.persist_inspection_index(identity, parent, &manifest).await?;
        self.bind_snapshot_candidate(
            identity,
            &candidate::binding(
                snapshot,
                parent,
                operation,
                &prepared.manifest,
                match mode {
                    PublicationMode::Candidate(initial) | PublicationMode::Resource(initial) => initial,
                    _ => None,
                },
            ),
            resource.gc_after,
        )
        .await?;
        if !matches!(mode, PublicationMode::LegacyPublish) {
            return Ok(snapshot);
        }
        self.complete_snapshot_candidate(identity, snapshot).await?;
        self.advance_head(identity, None, parent, snapshot).await
    }

    pub(super) async fn current_latest(
        &self,
        identity: &DatasetIdentity,
    ) -> Result<Option<SnapshotId>, AuthorityError> {
        Ok(self
            .store
            .get(&key::head(identity)?)
            .await?
            .map(|value| decode_head(&value.bytes))
            .transpose()?
            .and_then(|head| head.latest))
    }

    async fn validate_parent(
        &self,
        identity: &DatasetIdentity,
        parent: Option<SnapshotId>,
    ) -> Result<(), AuthorityError> {
        let Some(parent) = parent else { return Ok(()) };
        let value = self
            .store
            .get(&key::snapshot(identity, parent)?)
            .await?
            .ok_or(AuthorityError::SnapshotNotFound)?;
        let record = decode_snapshot(&value.bytes)?;
        if record.publication.state != PublicationState::Published {
            return Err(AuthorityError::ParentConflict);
        }
        let binding = self
            .store
            .get(&key::manifest_binding(identity, parent)?)
            .await?
            .ok_or(AuthorityError::Corrupt)?;
        let binding: ManifestBinding = decode(&binding.bytes)?;
        if binding.snapshot != parent || binding.partitions == 0 {
            return Err(AuthorityError::Corrupt);
        }
        Ok(())
    }

    pub(super) async fn advance_head(
        &self,
        identity: &DatasetIdentity,
        stable: Option<SnapshotId>,
        expected_parent: Option<SnapshotId>,
        snapshot: SnapshotId,
    ) -> Result<SnapshotId, AuthorityError> {
        let current_head = self.store.get(&key::head(identity)?).await?;
        let current_latest = current_head
            .as_ref()
            .map(|value| decode_head(&value.bytes))
            .transpose()?
            .and_then(|head| head.latest);
        if current_latest != expected_parent && current_latest != Some(snapshot) {
            return Err(AuthorityError::SnapshotHeadConflict {
                observed: Box::new(super::head::reference(current_head.as_ref())?),
            });
        }
        let update = crate::SnapshotHeadUpdate {
            snapshot,
            expected_head: current_head
                .as_ref()
                .map(|value| crate::CommitGeneration::from_head(&value.bytes)),
        };
        self.commit_snapshot_head(identity, &update, stable, current_head)
            .await?;
        Ok(snapshot)
    }
}
