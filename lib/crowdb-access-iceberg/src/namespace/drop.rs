use std::sync::Arc;

use crate::catalog::{CatalogContext, CatalogError};
use crate::error::ValidationError;
use crate::key::OperationId;
use crate::operation::RequestIdentity;

use super::{
    NamespaceAction, NamespaceCreator, NamespaceIdentifier, NamespaceJournal, NamespaceOutcome,
    NamespacePhase, NamespaceStore,
};

mod resource;

pub use resource::NamespaceDropOutcome;

#[derive(Clone, Debug)]
pub struct NamespaceDropRequest {
    pub context: CatalogContext,
    pub identity: RequestIdentity,
    pub principal: String,
    pub identifier: NamespaceIdentifier,
}

pub struct NamespaceDropper {
    pub(super) creator: NamespaceCreator,
}

impl NamespaceDropper {
    #[must_use]
    pub fn new<Store: NamespaceStore + 'static>(store: Arc<Store>) -> Self {
        Self {
            creator: NamespaceCreator::new(store),
        }
    }

    /// # Errors
    /// Rejects invalid requests and uncertain storage. Each call selects the current name.
    pub async fn drop_namespace(
        &self,
        request: &NamespaceDropRequest,
    ) -> Result<Option<NamespaceDropOutcome>, CatalogError> {
        self.drop_resource(request).await
    }

    /// # Errors
    /// Fails closed on uncertain writes, corrupt probes and exhausted recovery work.
    pub async fn resume(
        &self,
        context: CatalogContext,
        identity: OperationId,
    ) -> Result<NamespaceOutcome, CatalogError> {
        self.resume_with_budget(context, identity, &mut 16).await
    }

    pub(super) async fn resume_with_budget(
        &self,
        context: CatalogContext,
        identity: OperationId,
        budget: &mut usize,
    ) -> Result<NamespaceOutcome, CatalogError> {
        let journal = NamespaceJournal::new(self.creator.repository.store.clone());
        while *budget > 0 {
            *budget -= 1;
            let operation = journal
                .load(context, identity)
                .await?
                .ok_or(ValidationError::Record)?;
            if operation.action != NamespaceAction::Drop {
                return Err(ValidationError::Record.into());
            }
            match operation.phase {
                NamespacePhase::Prepared => self.prepare_fence(&operation, budget).await?,
                NamespacePhase::Fencing => self.apply_fence(&operation).await?,
                NamespacePhase::ProbingNamespaces | NamespacePhase::ProbingTables => {
                    self.probe(&operation, budget).await?;
                }
                NamespacePhase::Restoring | NamespacePhase::Tombstoning => {
                    self.apply_finish(&operation).await?;
                }
                NamespacePhase::Complete => {
                    self.cleanup(&operation).await?;
                    self.creator.repository.check_context(context).await?;
                    return operation.outcome.ok_or_else(|| ValidationError::Record.into());
                }
                _ => return Err(ValidationError::Record.into()),
            }
        }
        Err(CatalogError::Busy)
    }
}
