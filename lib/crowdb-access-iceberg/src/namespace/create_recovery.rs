use crate::catalog::{CatalogContext, CatalogError};
use crate::error::ValidationError;
use crate::key::OperationId;
use crate::operation::PayloadStore;

use super::update_recovery::next_phase;
use super::{
    NamespaceAction, NamespaceCreator, NamespaceJournal, NamespaceOperation, NamespaceOutcome, NamespacePhase,
};

impl NamespaceCreator {
    pub(crate) async fn help_table_parent(
        store: std::sync::Arc<dyn crate::catalog::CatalogStore>,
        names: std::sync::Arc<dyn super::NamespaceStore>,
        context: CatalogContext,
        holder: crate::key::NamespaceId,
        identity: OperationId,
        budget: &mut usize,
    ) -> Result<(), CatalogError> {
        Self {
            repository: super::NamespaceRepository::from_parts(store, names.clone()),
            names,
        }
        .help_marker(context, holder, identity, budget)
        .await
    }
    /// # Errors
    /// Rejects foreign operations and retired contexts; unfinished bounded work returns busy.
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
        let journal = NamespaceJournal::new(self.repository.store.clone());
        while *budget > 0 {
            *budget -= 1;
            let operation = journal
                .load(context, identity)
                .await?
                .ok_or(ValidationError::Record)?;
            if operation.action != NamespaceAction::Create {
                return Err(ValidationError::Record.into());
            }
            match operation.phase {
                NamespacePhase::Prepared => self.reserve_name(&operation, budget).await?,
                NamespacePhase::Reserved => self.prepare_admission(&operation, budget).await?,
                NamespacePhase::Admitting => self.finish_admission(&operation).await?,
                NamespacePhase::Admitted => self.prepare_publication(&operation).await?,
                NamespacePhase::Publishing => self.publish(&operation).await?,
                NamespacePhase::Published => self.complete(&operation).await?,
                NamespacePhase::Aborting => {
                    self.release_admission(&operation).await?;
                    self.remove_reservation(&operation).await?;
                    journal
                        .advance(&operation, &next_phase(&operation, NamespacePhase::Aborted)?)
                        .await?;
                }
                NamespacePhase::Complete => {
                    self.release_created_authority(&operation).await?;
                    self.repository.check_context(context).await?;
                    return operation.outcome.ok_or_else(|| ValidationError::Record.into());
                }
                NamespacePhase::Aborted => {
                    self.release_admission(&operation).await?;
                    self.remove_reservation(&operation).await?;
                    self.repository.check_context(context).await?;
                    return operation.outcome.ok_or_else(|| ValidationError::Record.into());
                }
                _ => return Err(ValidationError::Record.into()),
            }
        }
        Err(CatalogError::Busy)
    }

    pub(super) async fn abort(
        &self,
        operation: &NamespaceOperation,
        status: u16,
    ) -> Result<(), CatalogError> {
        let (kind, message) = match status {
            400 => (
                "BadRequestException",
                "Parent namespace is not available for child admission",
            ),
            409 => ("AlreadyExistsException", "Namespace already exists"),
            _ => return Err(ValidationError::Record.into()),
        };
        let bytes = serde_json::to_vec(&serde_json::json!({
            "error": { "code": status, "type": kind, "message": message }
        }))
        .map_err(|_| ValidationError::Record)?;
        let body = PayloadStore::new(self.repository.store.clone())
            .put(operation.context.catalog, operation.identity.operation, &bytes)
            .await?;
        let mut next = next_phase(operation, NamespacePhase::Aborting)?;
        next.outcome = Some(NamespaceOutcome { status, body });
        NamespaceJournal::new(self.repository.store.clone())
            .advance(operation, &next)
            .await?;
        Ok(())
    }

    pub(super) async fn help_marker(
        &self,
        context: CatalogContext,
        holder: crate::key::NamespaceId,
        identity: OperationId,
        budget: &mut usize,
    ) -> Result<(), CatalogError> {
        let Some(operation) = NamespaceJournal::new(self.repository.store.clone())
            .load(context, identity)
            .await?
        else {
            return Box::pin(crate::commit::TableCreator::help_admission(
                self.repository.store.clone(),
                self.names.clone(),
                context,
                holder,
                identity,
                budget,
            ))
            .await;
        };
        if operation.namespace != holder
            && !(operation.action == NamespaceAction::Create && operation.parent == Some(holder))
        {
            return Err(ValidationError::IdentityMismatch.into());
        }
        match operation.action {
            NamespaceAction::Update => {
                if !matches!(
                    operation.phase,
                    NamespacePhase::Publishing | NamespacePhase::Published | NamespacePhase::Complete
                ) {
                    return Err(ValidationError::Record.into());
                }
                self.repository
                    .resume_property_with_budget(context, identity, budget)
                    .await?;
            }
            NamespaceAction::Create => {
                if !matches!(
                    operation.phase,
                    NamespacePhase::Admitting
                        | NamespacePhase::Admitted
                        | NamespacePhase::Publishing
                        | NamespacePhase::Published
                        | NamespacePhase::Complete
                        | NamespacePhase::Aborting
                        | NamespacePhase::Aborted
                ) {
                    return Err(ValidationError::Record.into());
                }
                Box::pin(self.resume_with_budget(context, identity, budget)).await?;
            }
            NamespaceAction::Drop => {
                let dropper = super::NamespaceDropper {
                    creator: self.clone(),
                };
                Box::pin(dropper.resume_with_budget(context, identity, budget)).await?;
            }
        }
        Ok(())
    }
}
