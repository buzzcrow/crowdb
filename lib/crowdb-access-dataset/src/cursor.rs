use serde::{Deserialize, Serialize};

use crate::{DatasetError, ReadPlan, SnapshotId};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ReadCursor {
    pub snapshot: SnapshotId,
    pub plan_identity: [u8; 16],
    pub group: u32,
    pub offset: u32,
    pub confirmed: bool,
}

impl ReadCursor {
    #[must_use]
    pub fn start(plan: &ReadPlan) -> Self {
        Self {
            snapshot: plan.snapshot,
            plan_identity: plan.identity(),
            group: 0,
            offset: 0,
            confirmed: true,
        }
    }

    /// # Errors
    /// Rejects a cursor created for another snapshot or logical read plan.
    pub fn validate_for(&self, plan: &ReadPlan) -> Result<(), DatasetError> {
        if self.snapshot != plan.snapshot || self.plan_identity != plan.identity() {
            return Err(DatasetError::CursorMismatch);
        }
        Ok(())
    }

    /// Returns the next cursor only after the caller confirms the delivered
    /// batch. Unconfirmed cursors remain at the previous replay boundary.
    ///
    /// # Errors
    /// Rejects a cursor that does not match the plan.
    pub fn confirm_batch(&self, plan: &ReadPlan, group: u32, offset: u32) -> Result<Self, DatasetError> {
        self.validate_for(plan)?;
        Ok(Self {
            snapshot: self.snapshot,
            plan_identity: self.plan_identity,
            group,
            offset,
            confirmed: true,
        })
    }
}
