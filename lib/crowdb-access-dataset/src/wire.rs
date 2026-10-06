use serde::{Deserialize, Serialize};

use crate::{
    DatasetError, ManifestRecord, OperationId, ReadCursor, ReadPlan, SampleView, ShuffleSpec, SnapshotId,
    SnapshotRecord, MAX_BATCH_SIZE,
};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct DatasetPublishRequest {
    pub parent: Option<SnapshotId>,
    pub manifest: Vec<u8>,
    pub operation: Option<OperationId>,
}

impl DatasetPublishRequest {
    /// # Errors
    /// Rejects empty or oversized manifest bytes.
    pub fn validate(&self) -> Result<(), DatasetError> {
        if self.manifest.is_empty() || self.manifest.len() > 16 * 1024 * 1024 {
            return Err(DatasetError::InvalidManifest);
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct DatasetPublishResponse {
    pub snapshot: SnapshotId,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct DatasetSnapshotRequest {
    pub snapshot: SnapshotId,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct DatasetSnapshotResponse {
    pub snapshot: SnapshotRecord,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct DatasetSnapshotsResponse {
    pub snapshots: Vec<SnapshotRecord>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct DatasetManifestResponse {
    pub manifest: ManifestRecord,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct DatasetRetentionResponse {
    pub snapshot: SnapshotId,
    pub retained: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct DatasetReclaimResponse {
    pub snapshot: SnapshotId,
    pub reclaimed: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct DatasetReadRequest {
    pub snapshot: SnapshotId,
    pub sample_ids: Vec<Vec<u8>>,
    pub fields: Vec<String>,
}

impl DatasetReadRequest {
    /// # Errors
    /// Rejects empty or duplicate IDs/fields and batches above the core bound.
    pub fn validate(&self) -> Result<(), DatasetError> {
        if self.sample_ids.is_empty() || self.sample_ids.len() > MAX_BATCH_SIZE {
            return Err(DatasetError::InvalidManifest);
        }
        let mut ids = std::collections::BTreeSet::new();
        if self.sample_ids.iter().any(|id| id.is_empty() || !ids.insert(id)) {
            return Err(DatasetError::InvalidManifest);
        }
        let mut fields = std::collections::BTreeSet::new();
        if self
            .fields
            .iter()
            .any(|field| field.is_empty() || !fields.insert(field))
        {
            return Err(DatasetError::InvalidManifest);
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct DatasetReadResponse {
    pub snapshot: SnapshotId,
    pub samples: Vec<SampleView>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct DatasetScanRequest {
    pub plan: ReadPlan,
    pub shuffle: Option<ShuffleSpec>,
    pub cursor: Option<ReadCursor>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct DatasetScanResponse {
    pub snapshot: SnapshotId,
    pub samples: Vec<SampleView>,
    pub cursor: ReadCursor,
    pub end: bool,
}

impl DatasetReadResponse {
    #[must_use]
    pub fn from_batch(snapshot: SnapshotId, samples: Vec<SampleView>) -> Self {
        Self { snapshot, samples }
    }
}
