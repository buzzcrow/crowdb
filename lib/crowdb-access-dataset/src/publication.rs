use serde::{Deserialize, Serialize};

use crate::{DatasetError, OperationId, SnapshotId};

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum PublicationState {
    Prepared,
    Published,
    Aborted,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SnapshotPublication {
    pub snapshot: SnapshotId,
    pub parent: Option<SnapshotId>,
    pub operation: OperationId,
    pub manifest: Vec<u8>,
    pub state: PublicationState,
}

impl SnapshotPublication {
    /// # Errors
    /// Rejects self-parenting snapshots and empty manifest references.
    pub fn validate(&self) -> Result<(), DatasetError> {
        if self.parent == Some(self.snapshot) {
            return Err(DatasetError::SelfParent);
        }
        if self.manifest.is_empty() {
            return Err(DatasetError::EmptyManifest);
        }
        if self.state == PublicationState::Published && self.operation.as_bytes() == &[0; 16] {
            return Err(DatasetError::InvalidPublicationState);
        }
        Ok(())
    }

    /// # Errors
    /// Only a prepared publication may be atomically advanced to published.
    pub fn publish(&mut self) -> Result<(), DatasetError> {
        self.validate()?;
        if self.state != PublicationState::Prepared {
            return Err(DatasetError::InvalidPublicationState);
        }
        self.state = PublicationState::Published;
        Ok(())
    }
}
