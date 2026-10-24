mod access;
pub use access::{
    MetadataCleanupProgress, SnapshotLifecycle, SnapshotReclaimCursor, SnapshotReclaimError,
    SnapshotReclaimPhase, SnapshotReclaimStatus, SnapshotReclaimSweep,
};
mod discovery;
mod head;
mod projection;

use head::{decode_head, encode_head};
pub use head::{DatasetHead, SnapshotHeadUpdate};
mod publication;

use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;

use crate::chunk_store::{unavailable_chunk_reader, ChunkReadError, ChunkReader, ReadCancellation};
use crate::key;
use crate::manifest::{ManifestPartition, ManifestRecord};
use crate::record::{DatasetRecord, HeadRecord, ManifestBinding, SnapshotRecord};
use crate::store::{CasOutcome, DatasetStore, StoreError};
use crate::{DatasetError, DatasetId, DatasetIdentity, PublicationState, SnapshotId, SnapshotPublication};

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
    #[error("Snapshot publication condition no longer matches the head")]
    SnapshotHeadConflict { observed: Box<DatasetHead> },
    #[error("stored dataset record is malformed")]
    Corrupt,
    #[error("snapshot was not found")]
    SnapshotNotFound,
    #[error("snapshot ancestry contains a cycle")]
    AncestryCycle,
    #[error("read cursor lost its compare-and-exchange race")]
    CursorConflict,
    #[error("snapshot is protected by a head, retention marker, or parent chain")]
    SnapshotProtected,
    #[error("snapshot deletion grace period ends at {reclaim_after}")]
    ReclaimPending { reclaim_after: u64 },
}

pub struct DatasetAuthority {
    pub(crate) store: Arc<dyn DatasetStore>,
    chunk_reader: Arc<dyn ChunkReader>,
    #[cfg(feature = "test-util")]
    reclaim_clock: Option<Arc<std::sync::atomic::AtomicU64>>,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct SampleView {
    pub sample_id: Vec<u8>,
    pub fields: BTreeMap<String, crate::FieldLocator>,
    pub values: BTreeMap<String, Vec<u8>>,
}

impl DatasetAuthority {
    #[must_use]
    pub fn new(store: Arc<dyn DatasetStore>) -> Self {
        Self {
            store,
            chunk_reader: unavailable_chunk_reader(),
            #[cfg(feature = "test-util")]
            reclaim_clock: None,
        }
    }

    #[must_use]
    pub fn with_chunk_reader(store: Arc<dyn DatasetStore>, chunk_reader: Arc<dyn ChunkReader>) -> Self {
        Self {
            store,
            chunk_reader,
            #[cfg(feature = "test-util")]
            reclaim_clock: None,
        }
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
        let mut dataset = self.read_dataset_record(identity).await?;
        if let Some(head) = self.store.get(&key::head(identity)?).await? {
            let head = decode_head(&head.bytes)?;
            dataset.latest = head.latest;
            dataset.stable = head.stable;
        }
        Ok(dataset)
    }

    async fn read_dataset_record(&self, identity: &DatasetIdentity) -> Result<DatasetRecord, AuthorityError> {
        let value = self
            .store
            .get(&key::dataset(identity)?)
            .await?
            .ok_or(AuthorityError::NotFound)?;
        decode(&value.bytes)
    }

    /// # Errors
    /// Returns a missing or malformed head.
    pub async fn latest(&self, identity: &DatasetIdentity) -> Result<Option<SnapshotId>, AuthorityError> {
        let Some(value) = self.store.get(&key::head(identity)?).await? else {
            self.get_dataset(identity).await?;
            return Ok(None);
        };
        Ok(decode_head(&value.bytes)?.latest)
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
        self.get_snapshot(identity, snapshot).await?;
        let marker = encode(&snapshot)?;
        match self
            .store
            .compare_exchange(&key::retention(identity, snapshot)?, None, &marker)
            .await?
        {
            CasOutcome::Applied(_) | CasOutcome::Conflict(Some(_)) => {
                self.ensure_snapshot_live(identity, snapshot).await
            }
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
            .map(|value| decode_head(&value.bytes))
            .transpose()?
            .unwrap_or(HeadRecord {
                latest: None,
                stable: None,
            });
        if head.stable == Some(snapshot) {
            return Ok(());
        }
        self.ensure_snapshot_live(identity, snapshot).await?;
        if let Some(current) = &current {
            self.preserve_legacy_head(identity, current).await?;
        }
        head.stable = Some(snapshot);
        let bytes = encode_head(head)?;
        let expected = current.as_ref().map(|value| value.bytes.as_slice());
        match self.store.compare_exchange(&key, expected, &bytes).await? {
            CasOutcome::Applied(_) => Ok(()),
            CasOutcome::Conflict(value) => {
                let observed = head::reference(value.as_ref())?;
                if observed.stable == Some(snapshot) {
                    return Ok(());
                }
                Err(AuthorityError::SnapshotHeadConflict {
                    observed: Box::new(observed),
                })
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
            let record = self.get_snapshot(identity, id).await?;
            current = record.publication.parent;
            chain.push(record);
        }
        Ok(chain)
    }

    /// Opens one complete Snapshot without changing the current head.
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
        if record.publication.snapshot != snapshot {
            return Err(AuthorityError::Corrupt);
        }
        if record.publication.state != PublicationState::Published {
            return Err(AuthorityError::SnapshotNotFound);
        }
        self.require_committed_snapshot(identity, snapshot).await?;
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
        self.persist_inspection_index(identity, snapshot_record.publication.parent, manifest)
            .await?;
        self.bind_completed_manifest(identity, &snapshot_record, manifest)
            .await?;
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
            self.complete_snapshot_candidate(identity, snapshot).await?;
            self.advance_head(identity, None, snapshot_record.publication.parent, snapshot)
                .await?;
        } else if self.current_latest(identity).await? != Some(snapshot) {
            self.complete_snapshot_candidate(identity, snapshot).await?;
            self.advance_head(identity, None, snapshot_record.publication.parent, snapshot)
                .await?;
        } else {
            self.complete_snapshot_candidate(identity, snapshot).await?;
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

    /// Loads stored Manifest partitions for internal planning and cleanup.
    /// Public read handlers use `read_manifest` to enforce resource admission.
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

    /// Reads the complete Manifest of an available Snapshot without changing any state.
    /// # Errors
    /// Rejects incomplete or deleted resources, corrupt content and storage failures.
    pub async fn read_manifest(
        &self,
        identity: &DatasetIdentity,
        snapshot: SnapshotId,
    ) -> Result<ManifestRecord, AuthorityError> {
        self.require_committed_snapshot(identity, snapshot).await?;
        self.get_manifest(identity, snapshot).await
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
        Ok(crate::planner::into_batches(
            self.plan_ids(identity, plan, limits).await?,
            plan.batch_size,
        ))
    }

    pub(crate) async fn plan_ids(
        &self,
        identity: &DatasetIdentity,
        plan: &crate::ReadPlan,
        limits: crate::ReadLimits,
    ) -> Result<Vec<Vec<u8>>, AuthorityError> {
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
        if ids.len().div_ceil(plan.batch_size) > limits.max_batches {
            return Err(DatasetError::ReadLimitExceeded.into());
        }
        Ok(ids)
    }

    /// Deletes one unreachable snapshot's Dataset metadata idempotently.
    /// Payload chunk deletion remains separate because locators may be external.
    ///
    /// # Errors
    /// Returns storage failures or malformed manifest bindings.
    #[cfg(feature = "test-util")]
    pub async fn reclaim_snapshot_metadata_for_tests(
        &self,
        identity: &DatasetIdentity,
        snapshot: SnapshotId,
    ) -> Result<(), AuthorityError> {
        let cancel = ReadCancellation::new();
        let mut progress = MetadataCleanupProgress::new(snapshot);
        let work = async {
            for _ in 0..64 {
                if self
                    .reclaim_snapshot_metadata_step(identity, snapshot, &mut progress, &cancel)
                    .await?
                    .complete
                {
                    return Ok(());
                }
            }
            Err(AuthorityError::Invalid(DatasetError::ReadLimitExceeded))
        };
        tokio::time::timeout(std::time::Duration::from_secs(30), work)
            .await
            .map_err(|_| AuthorityError::Invalid(DatasetError::ReadLimitExceeded))?
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
        let head = decode_head(&head.bytes)?;
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
        if sample_ids.is_empty() || fields.is_empty() {
            self.get_snapshot(identity, snapshot).await?;
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
        if sample_ids.is_empty() || fields.is_empty() {
            self.get_snapshot(identity, snapshot).await?;
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
