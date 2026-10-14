// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

mod opaque;

use super::{decode, decode_manifest, encode, DatasetAuthority};
use crate::{
    key, AuthorityError, CasOutcome, DatasetError, DatasetIdentity, HeadRecord, ManifestBinding,
    ManifestRecord, OperationId, PublicationState, SnapshotId, SnapshotPublication, SnapshotRecord,
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

    /// Publishes or recovers one complete Manifest publication using its
    /// durable operation token. The operation is visible through `latest` only
    /// after all Manifest partitions, the binding, and the Published record are
    /// durable.
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
        self.write_publication(identity, parent, manifest_bytes, operation, true)
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
        self.write_publication(identity, parent, manifest_bytes, operation, false)
            .await
    }

    /// Returns the candidate associated with a durable publication token.
    ///
    /// # Errors
    /// Returns missing, malformed, or unavailable operation records.
    pub async fn publication_status(
        &self,
        identity: &DatasetIdentity,
        operation: OperationId,
    ) -> Result<SnapshotRecord, AuthorityError> {
        self.get_dataset(identity).await?;
        let record = self
            .store
            .get(&key::operation(identity, operation)?)
            .await?
            .ok_or(AuthorityError::SnapshotNotFound)?;
        let snapshot = SnapshotId::from_bytes(&record.bytes)?;
        let record = self
            .store
            .get(&key::snapshot(identity, snapshot)?)
            .await?
            .ok_or(AuthorityError::SnapshotNotFound)?;
        decode(&record.bytes)
    }

    #[allow(clippy::too_many_lines)]
    async fn write_publication(
        &self,
        identity: &DatasetIdentity,
        parent: Option<SnapshotId>,
        manifest_bytes: Vec<u8>,
        operation: OperationId,
        publish: bool,
    ) -> Result<SnapshotId, AuthorityError> {
        self.get_dataset(identity).await?;
        let mut manifest = match decode_manifest(&manifest_bytes) {
            Ok(manifest) => manifest,
            Err(AuthorityError::Invalid(DatasetError::InvalidManifest)) => {
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
        let operation_key = key::operation(identity, operation)?;
        let snapshot = if let Some(value) = self.store.get(&operation_key).await? {
            SnapshotId::from_bytes(&value.bytes)?
        } else {
            let current_latest = self.current_latest(identity).await?;
            if current_latest != parent {
                return Err(AuthorityError::ParentConflict);
            }
            let snapshot = SnapshotId::random();
            match self
                .store
                .compare_exchange(&operation_key, None, snapshot.as_bytes())
                .await?
            {
                CasOutcome::Applied(_) => snapshot,
                CasOutcome::Conflict(Some(value)) => SnapshotId::from_bytes(&value.bytes)?,
                CasOutcome::Conflict(None) => return Err(AuthorityError::Corrupt),
            }
        };

        manifest.snapshot = snapshot;
        manifest.validate()?;
        self.validate_parent(identity, parent).await?;

        let snapshot_key = key::snapshot(identity, snapshot)?;
        let prepared = SnapshotPublication {
            snapshot,
            parent,
            operation,
            manifest: manifest_bytes.clone(),
            state: PublicationState::Prepared,
        };
        prepared.validate()?;
        let prepared_bytes = encode(&SnapshotRecord {
            publication: prepared.clone(),
        })?;
        match self.store.get(&snapshot_key).await? {
            Some(value) => {
                let existing: SnapshotRecord = decode(&value.bytes)?;
                if existing.publication.operation != operation
                    || existing.publication.parent != parent
                    || existing.publication.manifest != manifest_bytes
                {
                    return Err(AuthorityError::AlreadyExists);
                }
                if existing.publication.state == PublicationState::Aborted {
                    return Err(AuthorityError::Invalid(DatasetError::InvalidPublicationState));
                }
            }
            None => match self
                .store
                .compare_exchange(&snapshot_key, None, &prepared_bytes)
                .await?
            {
                CasOutcome::Applied(_) => {}
                CasOutcome::Conflict(_) => return Err(AuthorityError::AlreadyExists),
            },
        }

        self.persist_manifest_parts(identity, snapshot, &manifest).await?;
        if !publish {
            return Ok(snapshot);
        }
        let published = SnapshotPublication {
            state: PublicationState::Published,
            ..prepared
        };
        let published_bytes = encode(&SnapshotRecord {
            publication: published,
        })?;
        match self.store.get(&snapshot_key).await? {
            Some(value) => {
                let existing: SnapshotRecord = decode(&value.bytes)?;
                if existing.publication.state == PublicationState::Prepared {
                    match self
                        .store
                        .compare_exchange(&snapshot_key, Some(&value.bytes), &published_bytes)
                        .await?
                    {
                        CasOutcome::Applied(_) => {}
                        CasOutcome::Conflict(_) => return Err(AuthorityError::HeadConflict),
                    }
                } else if existing.publication.state != PublicationState::Published {
                    return Err(AuthorityError::Invalid(DatasetError::InvalidPublicationState));
                }
            }
            None => return Err(AuthorityError::Corrupt),
        }
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
            .map(|value| decode::<HeadRecord>(&value.bytes))
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
        let record: SnapshotRecord = decode(&value.bytes)?;
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

    async fn persist_manifest_parts(
        &self,
        identity: &DatasetIdentity,
        snapshot: SnapshotId,
        manifest: &ManifestRecord,
    ) -> Result<u32, AuthorityError> {
        let partitions = manifest.partition(1024)?;
        if partitions.is_empty() {
            return Err(DatasetError::InvalidManifest.into());
        }
        for partition in &partitions {
            let partition_key = key::manifest_partition(identity, snapshot, partition.partition)?;
            let bytes = encode(partition)?;
            match self.store.compare_exchange(&partition_key, None, &bytes).await? {
                CasOutcome::Applied(_) => {}
                CasOutcome::Conflict(Some(existing)) if existing.bytes == bytes => {}
                CasOutcome::Conflict(_) => return Err(AuthorityError::AlreadyExists),
            }
        }
        let binding = encode(&ManifestBinding {
            snapshot,
            partitions: u32::try_from(partitions.len()).map_err(|_| DatasetError::InvalidManifest)?,
        })?;
        let binding_key = key::manifest_binding(identity, snapshot)?;
        match self.store.compare_exchange(&binding_key, None, &binding).await? {
            CasOutcome::Applied(_) => {}
            CasOutcome::Conflict(Some(existing)) if existing.bytes == binding => {}
            CasOutcome::Conflict(_) => return Err(AuthorityError::AlreadyExists),
        }
        Ok(decode::<ManifestBinding>(&binding)?.partitions)
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
            .map(|value| decode::<HeadRecord>(&value.bytes))
            .transpose()?
            .and_then(|head| head.latest);
        if current_latest == Some(snapshot) {
            return Ok(snapshot);
        }
        if current_latest != expected_parent {
            return Err(AuthorityError::ParentConflict);
        }
        let stable = stable.or_else(|| {
            current_head.as_ref().and_then(|value| {
                decode::<HeadRecord>(&value.bytes)
                    .ok()
                    .and_then(|head| head.stable)
            })
        });
        let new_head = encode(&HeadRecord {
            latest: Some(snapshot),
            stable,
        })?;
        let old_head = current_head.as_ref().map(|value| value.bytes.as_slice());
        match self
            .store
            .compare_exchange(&key::head(identity)?, old_head, &new_head)
            .await?
        {
            CasOutcome::Applied(_) => Ok(snapshot),
            CasOutcome::Conflict(_) => Err(AuthorityError::HeadConflict),
        }
    }
}
