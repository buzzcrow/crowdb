use crate::{catalog::CatalogError, error::ValidationError, operation::PayloadReference};

use super::{GcRepository, GcTask};

mod pages;
mod set;
mod traversal;

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct GcProofState {
    pub root: Option<PayloadReference>,
    pub pending: Option<PayloadReference>,
    pub complete: bool,
}

impl GcProofState {
    pub(super) fn validate(&self, task: &GcTask) -> Result<(), ValidationError> {
        for reference in [&self.root, &self.pending].into_iter().flatten() {
            reference.validate()?;
            if reference.catalog != task.context.catalog
                || reference.operation != task.identity
                || reference.length > crate::operation::PAYLOAD_PAGE_BYTES
            {
                return Err(ValidationError::Record);
            }
        }
        if (self.complete
            && (self.root.is_none() || self.pending.is_some() || task.queue_read != task.queue_write))
            || (self.root.is_none() != (task.marked == 0))
        {
            return Err(ValidationError::Record);
        }
        Ok(())
    }
}

impl GcRepository {
    pub(super) async fn verify_proof_task(&self, task: &GcTask) -> Result<(), CatalogError> {
        task.validate()?;
        if self.task(task.context.catalog, task.identity).await?.as_ref() != Some(task) {
            return Err(CatalogError::Conflict);
        }
        Ok(())
    }
}
