use crate::error::ValidationError;
use crate::key::OperationId;

use super::{CatalogContext, ClearTransition};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RootState {
    Ready,
    Initializing,
    Fencing,
    Maintenance(ClearTransition),
    Published(ClearTransition),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ActiveCatalogRecord {
    pub context: CatalogContext,
    pub operation: OperationId,
    pub state: RootState,
}

impl ActiveCatalogRecord {
    /// # Errors
    /// Rejects contexts inconsistent with the selected publication phase.
    pub fn validate(self) -> Result<(), ValidationError> {
        self.context.validate()?;
        let (transition, expected) = match self.state {
            RootState::Initializing if self.context.activation_epoch != 1 => {
                return Err(ValidationError::Record);
            }
            RootState::Ready | RootState::Initializing | RootState::Fencing => return Ok(()),
            RootState::Maintenance(transition) => (transition, transition.previous),
            RootState::Published(transition) => (transition, transition.replacement),
        };
        transition.validate()?;
        if self.context != expected || self.operation != transition.operation {
            return Err(ValidationError::IdentityMismatch);
        }
        Ok(())
    }
}
