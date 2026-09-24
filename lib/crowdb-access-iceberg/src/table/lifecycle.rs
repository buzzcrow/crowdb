use crate::{
    catalog::{CatalogContext, CatalogError, CatalogStore},
    commit::TableCommitOutcome,
    key::OperationId,
    namespace::{NamespaceIdentifier, NamespaceRepository, NamespaceStore},
    operation::{PayloadStore, RequestIdentity},
};
use std::sync::Arc;

mod admission;
mod completion;
mod journal;
mod operation;
mod preparation;
mod reservation;

pub use operation::{TableLifecycleOperation, TableLifecyclePhase, TablePurgeTask};
use TableLifecyclePhase as Phase;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TableLifecycleAction {
    Drop {
        purge_requested: bool,
    },
    Rename {
        namespace: NamespaceIdentifier,
        name: String,
    },
}

#[derive(Clone, Debug)]
pub struct TableLifecycleRequest {
    pub context: CatalogContext,
    pub identity: RequestIdentity,
    pub principal: String,
    pub namespace: NamespaceIdentifier,
    pub name: String,
    pub action: TableLifecycleAction,
}

pub struct TableLifecycles {
    store: Arc<dyn CatalogStore>,
    names: Arc<dyn NamespaceStore>,
    namespaces: NamespaceRepository,
    payloads: PayloadStore,
}

impl TableLifecycles {
    #[must_use]
    pub fn new<Store: NamespaceStore + 'static>(store: Arc<Store>) -> Self {
        Self::from_parts(store.clone(), store)
    }

    pub(crate) fn from_parts(store: Arc<dyn CatalogStore>, names: Arc<dyn NamespaceStore>) -> Self {
        Self {
            namespaces: NamespaceRepository::from_parts(store.clone(), names.clone()),
            payloads: PayloadStore::new(store.clone()),
            store,
            names,
        }
    }

    /// # Errors
    /// Rejects changed request identities, retired catalogs and uncertain storage outcomes.
    pub async fn execute(&self, request: &TableLifecycleRequest) -> Result<TableCommitOutcome, CatalogError> {
        let input = preparation::input(request)?;
        if let Some(operation) = self.load(request.context, request.identity.operation).await? {
            self.match_request(request, &input, &operation).await?;
        } else if let Some(outcome) = self.prepare(request, &input).await? {
            return Ok(outcome);
        }
        self.resume(request.context, request.identity.operation).await
    }

    /// # Errors
    /// Retains durable intent on unknown outcomes or exhausted bounded helping budgets.
    pub async fn resume(
        &self,
        context: CatalogContext,
        identity: OperationId,
    ) -> Result<TableCommitOutcome, CatalogError> {
        self.resume_with_budget(context, identity, &mut 24).await
    }

    pub(crate) async fn resume_with_budget(
        &self,
        context: CatalogContext,
        identity: OperationId,
        budget: &mut usize,
    ) -> Result<TableCommitOutcome, CatalogError> {
        while *budget > 0 {
            *budget -= 1;
            let operation = self
                .load(context, identity)
                .await?
                .ok_or(crate::error::ValidationError::Record)?;
            match operation.phase {
                Phase::Prepared if operation.is_rename() => self.reserve(&operation, budget).await?,
                Phase::Prepared => {
                    self.advance(&operation, &operation.next(Phase::Publishing)?)
                        .await?;
                }
                Phase::Reserved => self.prepare_admission(&operation, budget).await?,
                Phase::Admitting => self.admit(&operation).await?,
                Phase::Publishing => self.publish(&operation).await?,
                Phase::Published => self.complete(&operation).await?,
                Phase::Aborting => {
                    self.cleanup(&operation).await?;
                    self.advance(&operation, &operation.next(Phase::Aborted)?).await?;
                }
                Phase::Complete | Phase::Aborted => {
                    self.cleanup(&operation).await?;
                    crate::catalog::check_context(self.store.as_ref(), context).await?;
                    return operation
                        .outcome
                        .ok_or_else(|| crate::error::ValidationError::Record.into());
                }
            }
        }
        Err(CatalogError::Busy)
    }
}
