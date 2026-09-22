use std::sync::Arc;

use crate::catalog::{CatalogContext, CatalogError};
use crate::error::ValidationError;
use crate::key::OperationId;
use crate::operation::{PayloadStore, RequestIdentity};

use super::{
    NamespaceAction, NamespaceCreator, NamespaceIdentifier, NamespaceJournal, NamespaceOperation,
    NamespaceOutcome, NamespacePhase, NamespaceStore,
};

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
    /// Rejects invalid requests, changed retry identity and unavailable storage.
    /// Returns no outcome when no namespace exists before operation admission.
    pub async fn drop_namespace(
        &self,
        request: &NamespaceDropRequest,
    ) -> Result<Option<NamespaceOutcome>, CatalogError> {
        if request.principal.is_empty() || request.principal.len() > 256 || request.principal.contains('\0') {
            return Err(ValidationError::Text.into());
        }
        let journal = NamespaceJournal::new(self.creator.repository.store.clone());
        if let Some(existing) = journal.load(request.context, request.identity.operation).await? {
            if existing.action != NamespaceAction::Drop
                || existing.identity != request.identity
                || existing.principal != request.principal
                || existing.identifier != request.identifier
            {
                return Err(CatalogError::Conflict);
            }
            return self
                .resume(request.context, request.identity.operation)
                .await
                .map(Some);
        }
        let Some(authority) = self
            .creator
            .repository
            .load(request.context, &request.identifier)
            .await?
        else {
            return Ok(None);
        };
        authority
            .admission_fence
            .checked_add(2)
            .ok_or(ValidationError::GenerationExhausted)?;
        authority
            .mutation_revision
            .checked_add(3)
            .ok_or(ValidationError::GenerationExhausted)?;
        let input = PayloadStore::new(self.creator.repository.store.clone())
            .put(request.context.catalog, request.identity.operation, b"{}")
            .await?;
        let operation = NamespaceOperation {
            context: request.context,
            identity: request.identity,
            principal: request.principal.clone(),
            action: NamespaceAction::Drop,
            identifier: request.identifier.clone(),
            namespace: authority.namespace,
            parent: authority.parent,
            phase: NamespacePhase::Prepared,
            revision: 1,
            input,
            mutation: None,
            scan_after: Vec::new(),
            scan_generation: 0,
            outcome: None,
        };
        journal.begin(operation).await?;
        self.resume(request.context, request.identity.operation)
            .await
            .map(Some)
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
