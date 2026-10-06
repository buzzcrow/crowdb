use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;

use crate::chunk_store::{unavailable_chunk_reader, ChunkReadError, ChunkReader, ReadCancellation};
use crate::key;
use crate::manifest::{ManifestPartition, ManifestRecord};
use crate::record::{DatasetRecord, HeadRecord, ManifestBinding, SnapshotRecord};
use crate::store::{CasOutcome, DatasetStore, StoreError};
use crate::ReadCursor;
use crate::{
    DatasetError, DatasetId, DatasetIdentity, OperationId, PublicationState, SnapshotId, SnapshotPublication,
};

const RECORD_VERSION: u16 = 1;

fn map_chunk_error(error: &ChunkReadError) -> AuthorityError {
    match error {
        ChunkReadError::Cancelled => DatasetError::ReadCancelled.into(),
        ChunkReadError::NotFound => DatasetError::PayloadNotFound.into(),
        ChunkReadError::Transient(_) => DatasetError::PayloadTransient.into(),
        ChunkReadError::Truncated => DatasetError::PayloadTruncated.into(),
        ChunkReadError::Failed(_) => DatasetError::InvalidManifest.into(),
    }
}

#[derive(serde::Serialize, serde::Deserialize)]
struct VersionedRecord<T> {
    version: u16,
    payload: T,
}

#[derive(Debug, thiserror::Error)]
pub enum AuthorityError {
    #[error(transparent)]
    Invalid(#[from] DatasetError),
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error("dataset already exists")]
    AlreadyExists,
    #[error("dataset was not found")]
    NotFound,
    #[error("snapshot parent does not match the current head")]
    ParentConflict,
    #[error("publication lost its head race")]
    HeadConflict,
    #[error("stored dataset record is malformed")]
    Corrupt,
    #[error("snapshot was not found")]
    SnapshotNotFound,
    #[error("snapshot ancestry contains a cycle")]
    AncestryCycle,
    #[error("read cursor lost its compare-and-exchange race")]
    CursorConflict,
    #[error("snapshot is protected by a head, retention marker, parent chain, or active read")]
    SnapshotProtected,
}

pub struct DatasetAuthority {
    store: Arc<dyn DatasetStore>,
    chunk_reader: Arc<dyn ChunkReader>,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct SampleView {
    pub sample_id: Vec<u8>,
    pub fields: BTreeMap<String, crate::FieldLocator>,
    pub values: BTreeMap<String, Vec<u8>>,
}

impl DatasetAuthority {
    #[must_use]
    /// # Panics
    /// The built-in non-zero lease duration is always valid.
    pub fn new(store: Arc<dyn DatasetStore>) -> Self {
        Self {
            store,
            chunk_reader: unavailable_chunk_reader(),
        }
    }

    #[must_use]
    /// # Panics
    /// The built-in non-zero lease duration is always valid.
    pub fn with_chunk_reader(store: Arc<dyn DatasetStore>, chunk_reader: Arc<dyn ChunkReader>) -> Self {
        Self { store, chunk_reader }
    }

    /// # Errors
    /// Returns a store error or an existing Dataset mapping conflict.
    pub async fn create_dataset(&self, identity: DatasetIdentity) -> Result<DatasetId, AuthorityError> {
        if !identity.namespace().is_default() {
            return Err(DatasetError::InvalidDefaultNamespace.into());
        }
        let id = DatasetId::random();
        let record = DatasetRecord {
            id,
            identity: identity.clone(),
            latest: None,
            stable: None,
        };
        record.validate()?;
        let bytes = encode(&record)?;
        match self
            .store
            .compare_exchange(&key::dataset(&identity)?, None, &bytes)
            .await?
        {
            CasOutcome::Applied(_) => Ok(id),
            CasOutcome::Conflict(_) => Err(AuthorityError::AlreadyExists),
        }
    }

    /// # Errors
    /// Returns malformed storage or a missing Dataset.
    pub async fn get_dataset(&self, identity: &DatasetIdentity) -> Result<DatasetRecord, AuthorityError> {
        let value = self
            .store
            .get(&key::dataset(identity)?)
            .await?
            .ok_or(AuthorityError::NotFound)?;
        decode(&value.bytes)
    }

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

    async fn current_latest(&self, identity: &DatasetIdentity) -> Result<Option<SnapshotId>, AuthorityError> {
        Ok(self
            .store
            .get(&key::head(identity)?)
            .await?
            .map(|value| decode::<HeadRecord>(&value.bytes))
            .transpose()?
            .and_then(|head| head.latest))
    }

    async fn prepare_opaque_snapshot(
        &self,
        identity: &DatasetIdentity,
        parent: Option<SnapshotId>,
        manifest: Vec<u8>,
        operation: OperationId,
    ) -> Result<SnapshotId, AuthorityError> {
        let operation_key = key::operation(identity, operation)?;
        if let Some(value) = self.store.get(&operation_key).await? {
            let snapshot = SnapshotId::from_bytes(&value.bytes)?;
            if let Some(value) = self.store.get(&key::snapshot(identity, snapshot)?).await? {
                let record: SnapshotRecord = decode(&value.bytes)?;
                if record.publication.parent != parent || record.publication.manifest != manifest {
                    return Err(AuthorityError::AlreadyExists);
                }
                return Ok(snapshot);
            }
            let publication = SnapshotPublication {
                snapshot,
                parent,
                operation,
                manifest,
                state: PublicationState::Prepared,
            };
            publication.validate()?;
            let bytes = encode(&SnapshotRecord { publication })?;
            match self
                .store
                .compare_exchange(&key::snapshot(identity, snapshot)?, None, &bytes)
                .await?
            {
                CasOutcome::Applied(_) => return Ok(snapshot),
                CasOutcome::Conflict(_) => return Err(AuthorityError::AlreadyExists),
            }
        }
        if self.current_latest(identity).await? != parent {
            return Err(AuthorityError::ParentConflict);
        }
        let snapshot = SnapshotId::random();
        match self
            .store
            .compare_exchange(&operation_key, None, snapshot.as_bytes())
            .await?
        {
            CasOutcome::Applied(_) => {}
            CasOutcome::Conflict(Some(value)) => return Ok(SnapshotId::from_bytes(&value.bytes)?),
            CasOutcome::Conflict(None) => return Err(AuthorityError::Corrupt),
        }
        let publication = SnapshotPublication {
            snapshot,
            parent,
            operation,
            manifest,
            state: PublicationState::Prepared,
        };
        publication.validate()?;
        let bytes = encode(&SnapshotRecord { publication })?;
        match self
            .store
            .compare_exchange(&key::snapshot(identity, snapshot)?, None, &bytes)
            .await?
        {
            CasOutcome::Applied(_) => Ok(snapshot),
            CasOutcome::Conflict(_) => Err(AuthorityError::AlreadyExists),
        }
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

    async fn advance_head(
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

    /// # Errors
    /// Returns a missing or malformed head.
    pub async fn latest(&self, identity: &DatasetIdentity) -> Result<Option<SnapshotId>, AuthorityError> {
        let Some(value) = self.store.get(&key::head(identity)?).await? else {
            self.get_dataset(identity).await?;
            return Ok(None);
        };
        Ok(decode::<HeadRecord>(&value.bytes)?.latest)
    }

    /// Retains a published snapshot until the matching release operation.
    /// Repeating the operation is idempotent.
    ///
    /// # Errors
    /// Returns a missing or unpublished snapshot, storage failure, or corrupt record.
    pub async fn retain_snapshot(
        &self,
        identity: &DatasetIdentity,
        snapshot: SnapshotId,
    ) -> Result<(), AuthorityError> {
        let value = self
            .store
            .get(&key::snapshot(identity, snapshot)?)
            .await?
            .ok_or(AuthorityError::SnapshotNotFound)?;
        let record: SnapshotRecord = decode(&value.bytes)?;
        if record.publication.state != PublicationState::Published {
            return Err(AuthorityError::SnapshotNotFound);
        }
        let marker = encode(&snapshot)?;
        match self
            .store
            .compare_exchange(&key::retention(identity, snapshot)?, None, &marker)
            .await?
        {
            CasOutcome::Applied(_) | CasOutcome::Conflict(Some(_)) => Ok(()),
            CasOutcome::Conflict(None) => Err(AuthorityError::Corrupt),
        }
    }

    /// Releases a retention marker. Repeating the operation is idempotent.
    ///
    /// # Errors
    /// Returns a storage failure.
    pub async fn release_snapshot(
        &self,
        identity: &DatasetIdentity,
        snapshot: SnapshotId,
    ) -> Result<(), AuthorityError> {
        self.store.delete(&key::retention(identity, snapshot)?).await?;
        Ok(())
    }

    /// Moves the mutable stable binding to a published Snapshot.
    ///
    /// # Errors
    /// Returns a missing/unpublished Snapshot or a concurrent head update.
    pub async fn set_stable(
        &self,
        identity: &DatasetIdentity,
        snapshot: SnapshotId,
    ) -> Result<(), AuthorityError> {
        self.get_snapshot(identity, snapshot).await?;
        let key = key::head(identity)?;
        let current = self.store.get(&key).await?;
        let mut head = current
            .as_ref()
            .map(|value| decode::<HeadRecord>(&value.bytes))
            .transpose()?
            .unwrap_or(HeadRecord {
                latest: None,
                stable: None,
            });
        if head.stable == Some(snapshot) {
            return Ok(());
        }
        head.stable = Some(snapshot);
        let bytes = encode(&head)?;
        let expected = current.as_ref().map(|value| value.bytes.as_slice());
        match self.store.compare_exchange(&key, expected, &bytes).await? {
            CasOutcome::Applied(_) => Ok(()),
            CasOutcome::Conflict(_) => Err(AuthorityError::HeadConflict),
        }
    }

    /// Increments the active-read count for one Dataset/Snapshot lease using
    /// a CAS record. Expired records are treated as zero.
    ///
    /// # Errors
    /// Returns invalid lease configuration, storage, or malformed lease data.
    pub async fn acquire_snapshot_read(
        &self,
        identity: &DatasetIdentity,
        snapshot: SnapshotId,
        now_seconds: u64,
        ttl_seconds: u64,
    ) -> Result<(), AuthorityError> {
        if ttl_seconds == 0 {
            return Err(DatasetError::InvalidManifest.into());
        }
        let key = key::read_lease(identity, snapshot)?;
        loop {
            let current = self.store.get(&key).await?;
            let (active, expected) = match current.as_ref() {
                Some(value) => {
                    let record: crate::ActiveReadLease = decode(&value.bytes)?;
                    if now_seconds >= record.deadline {
                        (0, Some(value.bytes.as_slice()))
                    } else {
                        (record.active, Some(value.bytes.as_slice()))
                    }
                }
                None => (0, None),
            };
            let record = crate::ActiveReadLease {
                snapshot,
                active: active.saturating_add(1),
                deadline: now_seconds.saturating_add(ttl_seconds),
            };
            match self
                .store
                .compare_exchange(&key, expected, &encode(&record)?)
                .await?
            {
                CasOutcome::Applied(_) => return Ok(()),
                CasOutcome::Conflict(_) => {}
            }
        }
    }

    /// Decrements one active-read lease. Releasing a missing/expired record is idempotent.
    ///
    /// # Errors
    /// Returns storage or malformed lease data errors.
    pub async fn release_snapshot_read(
        &self,
        identity: &DatasetIdentity,
        snapshot: SnapshotId,
    ) -> Result<(), AuthorityError> {
        let key = key::read_lease(identity, snapshot)?;
        loop {
            let Some(current) = self.store.get(&key).await? else {
                return Ok(());
            };
            let mut record: crate::ActiveReadLease = decode(&current.bytes)?;
            if record.active == 0 {
                return Ok(());
            }
            record.active -= 1;
            let result = if record.active == 0 {
                self.store.delete(&key).await.map(|()| CasOutcome::Applied(0))
            } else {
                self.store
                    .compare_exchange(&key, Some(&current.bytes), &encode(&record)?)
                    .await
            };
            match result? {
                CasOutcome::Applied(_) => return Ok(()),
                CasOutcome::Conflict(_) => {}
            }
        }
    }

    /// Returns the selected snapshot followed by each explicitly declared
    /// parent. Snapshot identifier ordering is never used to infer ancestry.
    ///
    /// # Errors
    /// Returns a missing or malformed snapshot, or a cycle in the parent chain.
    pub async fn snapshot_chain(
        &self,
        identity: &DatasetIdentity,
        snapshot: SnapshotId,
    ) -> Result<Vec<SnapshotRecord>, AuthorityError> {
        let mut chain = Vec::new();
        let mut current = Some(snapshot);
        let mut seen = HashSet::new();
        while let Some(id) = current {
            if !seen.insert(id) {
                return Err(AuthorityError::AncestryCycle);
            }
            let value = self
                .store
                .get(&key::snapshot(identity, id)?)
                .await?
                .ok_or(AuthorityError::SnapshotNotFound)?;
            let record: SnapshotRecord = decode(&value.bytes)?;
            if record.publication.state != PublicationState::Published {
                return Err(AuthorityError::SnapshotNotFound);
            }
            current = record.publication.parent;
            chain.push(record);
        }
        Ok(chain)
    }

    /// Opens one published Snapshot without changing the current head.
    ///
    /// # Errors
    /// Returns a missing, unpublished, or malformed Snapshot.
    pub async fn get_snapshot(
        &self,
        identity: &DatasetIdentity,
        snapshot: SnapshotId,
    ) -> Result<SnapshotRecord, AuthorityError> {
        let value = self
            .store
            .get(&key::snapshot(identity, snapshot)?)
            .await?
            .ok_or(AuthorityError::SnapshotNotFound)?;
        let record: SnapshotRecord = decode(&value.bytes)?;
        if record.publication.state != PublicationState::Published {
            return Err(AuthorityError::SnapshotNotFound);
        }
        Ok(record)
    }

    /// Lists the published snapshots reachable from the current latest head,
    /// newest first. This is the durable branch visible to Dataset readers.
    ///
    /// # Errors
    /// Returns malformed head/ancestry records or storage failures.
    pub async fn list_snapshots(
        &self,
        identity: &DatasetIdentity,
    ) -> Result<Vec<SnapshotRecord>, AuthorityError> {
        let Some(snapshot) = self.latest(identity).await? else {
            return Ok(Vec::new());
        };
        self.snapshot_chain(identity, snapshot).await
    }

    /// Persists validated manifest partitions under a published snapshot.
    /// Existing identical partitions are idempotent; divergent bytes are a
    /// conflict and never overwrite an earlier partition.
    ///
    /// # Errors
    /// Returns a missing snapshot, a snapshot binding or manifest validation
    /// error, or a storage conflict.
    pub async fn persist_manifest(
        &self,
        identity: &DatasetIdentity,
        snapshot: SnapshotId,
        manifest: &ManifestRecord,
        max_samples: usize,
    ) -> Result<u32, AuthorityError> {
        self.get_dataset(identity).await?;
        let snapshot_value = self
            .store
            .get(&key::snapshot(identity, snapshot)?)
            .await?
            .ok_or(AuthorityError::SnapshotNotFound)?;
        let snapshot_record: SnapshotRecord = decode(&snapshot_value.bytes)?;
        if !matches!(
            snapshot_record.publication.state,
            PublicationState::Prepared | PublicationState::Published
        ) || manifest.snapshot != snapshot
        {
            return Err(DatasetError::InvalidManifest.into());
        }
        let partitions = manifest.partition(max_samples)?;
        if partitions.is_empty() {
            return Err(DatasetError::InvalidManifest.into());
        }
        if let Some(parent) = snapshot_record.publication.parent {
            if let Some(parent_schema) = self.manifest_schema(identity, parent).await? {
                for required in parent_schema.fields.iter().filter(|field| field.required) {
                    if !manifest
                        .schema
                        .fields
                        .iter()
                        .any(|field| field.name == required.name)
                    {
                        return Err(DatasetError::InvalidManifest.into());
                    }
                }
            }
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
        if snapshot_record.publication.state == PublicationState::Prepared {
            let published = SnapshotRecord {
                publication: SnapshotPublication {
                    snapshot,
                    parent: snapshot_record.publication.parent,
                    operation: snapshot_record.publication.operation,
                    manifest: encode(manifest)?,
                    state: PublicationState::Published,
                },
            };
            let published_bytes = encode(&published)?;
            match self
                .store
                .compare_exchange(
                    &key::snapshot(identity, snapshot)?,
                    Some(&snapshot_value.bytes),
                    &published_bytes,
                )
                .await?
            {
                CasOutcome::Applied(_) => {}
                CasOutcome::Conflict(_) => return Err(AuthorityError::HeadConflict),
            }
            self.advance_head(identity, None, snapshot_record.publication.parent, snapshot)
                .await?;
        } else if self.current_latest(identity).await? != Some(snapshot) {
            self.advance_head(identity, None, snapshot_record.publication.parent, snapshot)
                .await?;
        }
        Ok(decode::<ManifestBinding>(&binding)?.partitions)
    }

    /// Reads one persisted manifest partition by its snapshot-bound key.
    ///
    /// # Errors
    /// Returns a missing partition or malformed/version-incompatible bytes.
    pub async fn get_manifest_partition(
        &self,
        identity: &DatasetIdentity,
        snapshot: SnapshotId,
        partition: u32,
    ) -> Result<ManifestPartition, AuthorityError> {
        let value = self
            .store
            .get(&key::manifest_partition(identity, snapshot, partition)?)
            .await?
            .ok_or(AuthorityError::NotFound)?;
        decode(&value.bytes)
    }

    /// Loads the lightweight manifest index for bounded planning and shuffle.
    ///
    /// # Errors
    /// Returns missing or malformed manifest partitions and storage failures.
    pub async fn get_manifest(
        &self,
        identity: &DatasetIdentity,
        snapshot: SnapshotId,
    ) -> Result<ManifestRecord, AuthorityError> {
        let binding = self
            .store
            .get(&key::manifest_binding(identity, snapshot)?)
            .await?
            .ok_or(AuthorityError::NotFound)?;
        let binding: crate::record::ManifestBinding = decode(&binding.bytes)?;
        if binding.snapshot != snapshot || binding.partitions == 0 {
            return Err(AuthorityError::Corrupt);
        }
        let snapshot_record: SnapshotRecord = decode(
            &self
                .store
                .get(&key::snapshot(identity, snapshot)?)
                .await?
                .ok_or(AuthorityError::SnapshotNotFound)?
                .bytes,
        )?;
        let mut samples = Vec::new();
        let mut schema = None;
        for partition in 0..binding.partitions {
            let part = self.get_manifest_partition(identity, snapshot, partition).await?;
            if part.snapshot != snapshot || part.partition != partition {
                return Err(AuthorityError::Corrupt);
            }
            schema.get_or_insert_with(|| part.schema.clone());
            samples.extend(part.samples);
        }
        let manifest = ManifestRecord {
            version: crate::manifest::MANIFEST_VERSION,
            snapshot,
            schema: schema.ok_or(AuthorityError::Corrupt)?,
            samples,
        };
        manifest.validate()?;
        let mut published_manifest = decode_manifest(&snapshot_record.publication.manifest)?;
        published_manifest.snapshot = snapshot;
        if published_manifest != manifest {
            return Err(AuthorityError::Corrupt);
        }
        Ok(manifest)
    }

    /// Plans a scan directly from persisted manifest partitions. Only the
    /// selected IDs and their small locator metadata are retained, bounded by
    /// `limits`; payloads are never opened during planning.
    ///
    /// # Errors
    /// Returns validation, partition, or read-limit failures.
    pub async fn plan_batches(
        &self,
        identity: &DatasetIdentity,
        plan: &crate::ReadPlan,
        limits: crate::ReadLimits,
    ) -> Result<Vec<Vec<Vec<u8>>>, AuthorityError> {
        plan.validate()?;
        limits.validate()?;
        let binding = self
            .store
            .get(&key::manifest_binding(identity, plan.snapshot)?)
            .await?
            .ok_or(AuthorityError::NotFound)?;
        let binding: ManifestBinding = decode(&binding.bytes)?;
        if binding.snapshot != plan.snapshot || binding.partitions == 0 {
            return Err(AuthorityError::Corrupt);
        }
        let mut ids = Vec::new();
        let mut metadata_bytes = 0usize;
        for index in 0..binding.partitions {
            let partition = self
                .get_manifest_partition(identity, plan.snapshot, index)
                .await?;
            for sample in partition.samples {
                if !plan.selection.matches(&sample) {
                    continue;
                }
                metadata_bytes = metadata_bytes.saturating_add(sample.sample_id.len());
                metadata_bytes = metadata_bytes.saturating_add(
                    sample
                        .fields
                        .iter()
                        .map(|field| field.name.len() + locator_size(&field.value))
                        .sum::<usize>(),
                );
                if metadata_bytes > limits.max_metadata_bytes || ids.len() >= limits.max_samples {
                    return Err(DatasetError::ReadLimitExceeded.into());
                }
                ids.push(sample.sample_id);
            }
        }
        ids.sort();
        let batches: Vec<_> = ids.chunks(plan.batch_size).map(<[Vec<u8>]>::to_vec).collect();
        if batches.len() > limits.max_batches {
            return Err(DatasetError::ReadLimitExceeded.into());
        }
        Ok(batches)
    }

    /// Deletes one unreachable snapshot's Dataset metadata idempotently.
    /// Payload chunk deletion remains separate because locators may be external.
    ///
    /// # Errors
    /// Returns storage failures or malformed manifest bindings.
    pub async fn reclaim_snapshot_metadata(
        &self,
        identity: &DatasetIdentity,
        snapshot: SnapshotId,
    ) -> Result<(), AuthorityError> {
        if self.has_active_snapshot_read(identity, snapshot).await? {
            return Err(AuthorityError::SnapshotProtected);
        }
        let head = self.store.get(&key::head(identity)?).await?;
        if let Some(head) = head {
            let head: HeadRecord = decode(&head.bytes)?;
            if head.latest == Some(snapshot) || head.stable == Some(snapshot) {
                return Err(AuthorityError::SnapshotProtected);
            }
        }
        if self
            .store
            .get(&key::retention(identity, snapshot)?)
            .await?
            .is_some()
        {
            return Err(AuthorityError::SnapshotProtected);
        }
        if self.snapshot_reachable_from_head(identity, snapshot).await? {
            return Err(AuthorityError::SnapshotProtected);
        }
        if let Some(binding) = self
            .store
            .get(&key::manifest_binding(identity, snapshot)?)
            .await?
        {
            let binding: ManifestBinding = decode(&binding.bytes)?;
            for partition in 0..binding.partitions {
                self.store
                    .delete(&key::manifest_partition(identity, snapshot, partition)?)
                    .await?;
            }
            self.store
                .delete(&key::manifest_binding(identity, snapshot)?)
                .await?;
        }
        self.store.delete(&key::snapshot(identity, snapshot)?).await?;
        let progress = crate::ReclaimProgress {
            snapshot,
            state: crate::ReclaimState::Completed,
            chunks_reclaimed: 0,
        };
        let progress_key = key::reclaim_progress(identity, snapshot)?;
        let bytes = encode(&progress)?;
        match self.store.compare_exchange(&progress_key, None, &bytes).await? {
            CasOutcome::Applied(_) | CasOutcome::Conflict(Some(_)) => {}
            CasOutcome::Conflict(None) => return Err(AuthorityError::Corrupt),
        }
        Ok(())
    }

    /// Returns durable reclaim progress, defaulting to pending for an unseen snapshot.
    ///
    /// # Errors
    /// Returns storage or malformed progress data.
    pub async fn reclaim_progress(
        &self,
        identity: &DatasetIdentity,
        snapshot: SnapshotId,
    ) -> Result<crate::ReclaimProgress, AuthorityError> {
        let Some(value) = self
            .store
            .get(&key::reclaim_progress(identity, snapshot)?)
            .await?
        else {
            return Ok(crate::ReclaimProgress {
                snapshot,
                state: crate::ReclaimState::Pending,
                chunks_reclaimed: 0,
            });
        };
        decode(&value.bytes)
    }

    async fn snapshot_reachable_from_head(
        &self,
        identity: &DatasetIdentity,
        target: SnapshotId,
    ) -> Result<bool, AuthorityError> {
        let Some(head) = self.store.get(&key::head(identity)?).await? else {
            return Ok(false);
        };
        let head: HeadRecord = decode(&head.bytes)?;
        let mut current = head.latest;
        let mut seen = HashSet::new();
        while let Some(snapshot) = current {
            if snapshot == target {
                return Ok(true);
            }
            if !seen.insert(snapshot) {
                return Err(AuthorityError::AncestryCycle);
            }
            let Some(value) = self.store.get(&key::snapshot(identity, snapshot)?).await? else {
                break;
            };
            current = decode::<SnapshotRecord>(&value.bytes)?.publication.parent;
        }
        Ok(false)
    }

    async fn has_active_snapshot_read(
        &self,
        identity: &DatasetIdentity,
        snapshot: SnapshotId,
    ) -> Result<bool, AuthorityError> {
        let Some(value) = self.store.get(&key::read_lease(identity, snapshot)?).await? else {
            return Ok(false);
        };
        let record: crate::ActiveReadLease = decode(&value.bytes)?;
        Ok(record.active != 0)
    }

    /// Resolves one field using copy-on-write snapshot ancestry. The first
    /// field record encountered wins; a tombstone intentionally hides all
    /// parent values.
    ///
    /// # Errors
    /// Returns a missing or malformed snapshot, binding, or partition.
    pub async fn resolve_field(
        &self,
        identity: &DatasetIdentity,
        snapshot: SnapshotId,
        sample_id: &[u8],
        field_name: &str,
    ) -> Result<Option<crate::FieldLocator>, AuthorityError> {
        for snapshot_record in self.snapshot_chain(identity, snapshot).await? {
            let snapshot = snapshot_record.publication.snapshot;
            let Some(binding_value) = self
                .store
                .get(&key::manifest_binding(identity, snapshot)?)
                .await?
            else {
                continue;
            };
            let binding: ManifestBinding = decode(&binding_value.bytes)?;
            for partition in 0..binding.partitions {
                let value = self
                    .store
                    .get(&key::manifest_partition(identity, snapshot, partition)?)
                    .await?
                    .ok_or(AuthorityError::Corrupt)?;
                let partition: crate::ManifestPartition = decode(&value.bytes)?;
                if let Some(sample) = partition
                    .samples
                    .iter()
                    .find(|sample| sample.sample_id == sample_id)
                {
                    if let Some(field) = sample.fields.iter().find(|field| field.name == field_name) {
                        return Ok(match &field.value {
                            crate::FieldLocator::Tombstone => None,
                            value => Some(value.clone()),
                        });
                    }
                }
            }
        }
        Ok(None)
    }

    /// Resolves a projection against one immutable snapshot identity.
    ///
    /// # Errors
    /// Propagates snapshot, binding, and partition storage errors.
    pub async fn resolve_projection(
        &self,
        identity: &DatasetIdentity,
        snapshot: SnapshotId,
        sample_id: &[u8],
        fields: &[&str],
    ) -> Result<BTreeMap<String, crate::FieldLocator>, AuthorityError> {
        self.ensure_member(identity, snapshot, sample_id).await?;
        let mut projection = BTreeMap::new();
        for field in fields {
            if let Some(value) = self.resolve_field(identity, snapshot, sample_id, field).await? {
                projection.insert((*field).to_owned(), value);
            }
        }
        Ok(projection)
    }

    async fn ensure_member(
        &self,
        identity: &DatasetIdentity,
        snapshot: SnapshotId,
        sample_id: &[u8],
    ) -> Result<(), AuthorityError> {
        let Some(binding) = self
            .store
            .get(&key::manifest_binding(identity, snapshot)?)
            .await?
        else {
            return Err(AuthorityError::Corrupt);
        };
        let binding: ManifestBinding = decode(&binding.bytes)?;
        for partition in 0..binding.partitions {
            let value = self
                .store
                .get(&key::manifest_partition(identity, snapshot, partition)?)
                .await?
                .ok_or(AuthorityError::Corrupt)?;
            let partition: ManifestPartition = decode(&value.bytes)?;
            if partition
                .samples
                .iter()
                .any(|sample| sample.sample_id == sample_id)
            {
                return Ok(());
            }
        }
        Err(DatasetError::SampleNotFound.into())
    }

    /// Reads one bounded batch through the same projection path used by native
    /// and HTTP callers.
    ///
    /// # Errors
    /// Rejects batches larger than the planner bound and propagates field-read
    /// failures.
    pub async fn read_batch(
        &self,
        identity: &DatasetIdentity,
        snapshot: SnapshotId,
        sample_ids: &[Vec<u8>],
        fields: &[&str],
    ) -> Result<Vec<SampleView>, AuthorityError> {
        if sample_ids.len() > crate::MAX_BATCH_SIZE {
            return Err(DatasetError::InvalidManifest.into());
        }
        let mut batch = Vec::with_capacity(sample_ids.len());
        for sample_id in sample_ids {
            batch.push(SampleView {
                sample_id: sample_id.clone(),
                fields: self
                    .resolve_projection(identity, snapshot, sample_id, fields)
                    .await?,
                values: BTreeMap::new(),
            });
        }
        self.materialize_batch(identity, snapshot, &mut batch).await?;
        Ok(batch)
    }

    async fn materialize_batch(
        &self,
        _identity: &DatasetIdentity,
        _snapshot: SnapshotId,
        batch: &mut [SampleView],
    ) -> Result<(), AuthorityError> {
        let cancel = ReadCancellation::new();
        for sample in batch {
            for (name, locator) in &sample.fields {
                let payload = match locator {
                    crate::FieldLocator::Inline { value, .. } => value.clone(),
                    crate::FieldLocator::Chunk { location, .. } => self
                        .chunk_reader
                        .read(location, None, &cancel)
                        .await
                        .map_err(|error| map_chunk_error(&error))?,
                    crate::FieldLocator::Tombstone => continue,
                };
                locator.verify_payload(&payload)?;
                sample.values.insert(name.clone(), payload);
            }
        }
        Ok(())
    }

    /// Reads projected values with a caller-owned cancellation handle.
    /// # Errors
    /// Returns membership, payload integrity, storage, or cancellation errors.
    pub async fn read_batch_cancelled(
        &self,
        identity: &DatasetIdentity,
        snapshot: SnapshotId,
        sample_ids: &[Vec<u8>],
        fields: &[&str],
        cancel: &ReadCancellation,
    ) -> Result<Vec<SampleView>, AuthorityError> {
        if cancel.is_cancelled() {
            return Err(DatasetError::ReadCancelled.into());
        }
        let mut batch = Vec::with_capacity(sample_ids.len());
        for sample_id in sample_ids {
            batch.push(SampleView {
                sample_id: sample_id.clone(),
                fields: self
                    .resolve_projection(identity, snapshot, sample_id, fields)
                    .await?,
                values: BTreeMap::new(),
            });
        }
        for sample in &mut batch {
            for (name, locator) in &sample.fields {
                if cancel.is_cancelled() {
                    return Err(DatasetError::ReadCancelled.into());
                }
                let payload = match locator {
                    crate::FieldLocator::Inline { value, .. } => value.clone(),
                    crate::FieldLocator::Chunk { location, .. } => self
                        .chunk_reader
                        .read(location, None, cancel)
                        .await
                        .map_err(|error| map_chunk_error(&error))?,
                    crate::FieldLocator::Tombstone => continue,
                };
                locator.verify_payload(&payload)?;
                sample.values.insert(name.clone(), payload);
            }
        }
        Ok(batch)
    }

    /// Executes the same logical batch read for either public transport
    /// surface. Transport handlers map into this method so ordering,
    /// projection, and locator integrity cannot diverge.
    ///
    /// # Errors
    /// Propagates the underlying snapshot and field resolution failures.
    pub async fn read_batch_surface(
        &self,
        surface: crate::ReadSurface,
        identity: &DatasetIdentity,
        snapshot: SnapshotId,
        sample_ids: &[Vec<u8>],
        fields: &[&str],
    ) -> Result<Vec<SampleView>, AuthorityError> {
        let _ = surface;
        self.read_batch(identity, snapshot, sample_ids, fields).await
    }

    /// Loads a durable cursor bound to its logical plan identity.
    ///
    /// # Errors
    /// Returns malformed cursor bytes or storage failures.
    pub async fn load_cursor(
        &self,
        identity: &DatasetIdentity,
        plan: &crate::ReadPlan,
    ) -> Result<Option<ReadCursor>, AuthorityError> {
        let Some(value) = self.store.get(&key::cursor(identity, plan.identity())?).await? else {
            return Ok(None);
        };
        let cursor: ReadCursor = decode(&value.bytes)?;
        cursor.validate_for(plan)?;
        Ok(Some(cursor))
    }

    /// Persists a confirmed cursor using CAS so an older worker cannot move a
    /// durable replay boundary backwards after a restart.
    ///
    /// # Errors
    /// Rejects a cursor bound to another plan or an unexpected stored value.
    pub async fn persist_cursor(
        &self,
        identity: &DatasetIdentity,
        plan: &crate::ReadPlan,
        cursor: &ReadCursor,
        expected: Option<&ReadCursor>,
    ) -> Result<(), AuthorityError> {
        cursor.validate_for(plan)?;
        let key = key::cursor(identity, plan.identity())?;
        let expected_bytes = expected.map(encode).transpose()?;
        let bytes = encode(cursor)?;
        match self
            .store
            .compare_exchange(&key, expected_bytes.as_deref(), &bytes)
            .await?
        {
            CasOutcome::Applied(_) => Ok(()),
            CasOutcome::Conflict(_) => Err(AuthorityError::CursorConflict),
        }
    }

    async fn manifest_schema(
        &self,
        identity: &DatasetIdentity,
        snapshot: SnapshotId,
    ) -> Result<Option<crate::SchemaRecord>, AuthorityError> {
        let Some(binding_value) = self
            .store
            .get(&key::manifest_binding(identity, snapshot)?)
            .await?
        else {
            return Ok(None);
        };
        let binding: ManifestBinding = decode(&binding_value.bytes)?;
        if binding.partitions == 0 {
            return Err(AuthorityError::Corrupt);
        }
        let value = self
            .store
            .get(&key::manifest_partition(identity, snapshot, 0)?)
            .await?
            .ok_or(AuthorityError::Corrupt)?;
        let partition: crate::ManifestPartition = decode(&value.bytes)?;
        Ok(Some(partition.schema))
    }
}

fn decode_manifest(bytes: &[u8]) -> Result<ManifestRecord, AuthorityError> {
    bincode::deserialize(bytes)
        .or_else(|_| decode(bytes))
        .map_err(|_| AuthorityError::Invalid(DatasetError::InvalidManifest))
}

fn locator_size(locator: &crate::FieldLocator) -> usize {
    match locator {
        crate::FieldLocator::Inline { value, .. } => value.len(),
        crate::FieldLocator::Chunk { location, .. } => location.len(),
        crate::FieldLocator::Tombstone => 0,
    }
}

fn encode<T: serde::Serialize>(value: &T) -> Result<Vec<u8>, AuthorityError> {
    bincode::serialize(&VersionedRecord {
        version: RECORD_VERSION,
        payload: value,
    })
    .map_err(|_| AuthorityError::Corrupt)
}

fn decode<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> Result<T, AuthorityError> {
    let record: VersionedRecord<T> = bincode::deserialize(bytes).map_err(|_| AuthorityError::Corrupt)?;
    if record.version != RECORD_VERSION {
        return Err(AuthorityError::Corrupt);
    }
    Ok(record.payload)
}
