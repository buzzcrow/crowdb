use serde::{Deserialize, Serialize};

use crate::{DatasetError, DatasetId, DatasetIdentity, SnapshotPublication};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct DatasetRecord {
    pub id: DatasetId,
    pub identity: DatasetIdentity,
    pub latest: Option<crate::SnapshotId>,
    pub stable: Option<crate::SnapshotId>,
}

impl DatasetRecord {
    /// # Errors
    /// Rejects records whose identity or stable binding is invalid.
    pub fn validate(&self) -> Result<(), DatasetError> {
        if self.identity.key()?.is_empty() {
            return Err(DatasetError::InvalidName);
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct HeadRecord {
    pub latest: Option<crate::SnapshotId>,
    pub stable: Option<crate::SnapshotId>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SnapshotRecord {
    pub publication: SnapshotPublication,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ManifestBinding {
    pub snapshot: crate::SnapshotId,
    pub partitions: u32,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ActiveReadLease {
    pub snapshot: crate::SnapshotId,
    pub active: u64,
    pub deadline: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum ReclaimState {
    Pending,
    Completed,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ReclaimProgress {
    pub snapshot: crate::SnapshotId,
    pub state: ReclaimState,
    pub chunks_reclaimed: u64,
}

impl SnapshotRecord {
    /// # Errors
    /// Propagates publication validation.
    pub fn validate(&self) -> Result<(), DatasetError> {
        self.publication.validate()
    }
}
