use super::{
    Arc, CatalogContext, CatalogError, CatalogStore, Error, NamespaceStore, OperationId, Phase, TableCreator,
};
use crate::{
    error::ValidationError,
    key::NamespaceId,
    table::{TableMapping, TableMappingState},
};

impl TableCreator {
    pub(crate) async fn help_admission(
        store: Arc<dyn CatalogStore>,
        names: Arc<dyn NamespaceStore>,
        context: CatalogContext,
        holder: NamespaceId,
        identity: OperationId,
        budget: &mut usize,
    ) -> Result<(), CatalogError> {
        let creator = Self {
            store,
            names,
            blocks: None,
            staged_limits: None,
            response_reserve: 0,
        };
        let operation = creator.journal().load(context, identity).await?;
        let Some(operation) = operation else {
            return Box::pin(
                crate::table::TableLifecycles::from_parts(creator.store, creator.names)
                    .help_admission(context, holder, identity, budget),
            )
            .await;
        };
        if operation.candidate.namespace != holder
            || !matches!(
                operation.phase,
                Phase::Admitting
                    | Phase::Admitted
                    | Phase::Publishing
                    | Phase::Published
                    | Phase::Complete
                    | Phase::Aborting
                    | Phase::Aborted
            )
        {
            return Err(ValidationError::IdentityMismatch.into());
        }
        creator
            .resume_with_budget(context, identity, budget)
            .await
            .map_err(catalog_error)?;
        Ok(())
    }

    pub(crate) async fn help_reservation(
        store: Arc<dyn CatalogStore>,
        names: Arc<dyn NamespaceStore>,
        context: CatalogContext,
        mapping: &TableMapping,
        budget: &mut usize,
    ) -> Result<(), CatalogError> {
        let creator = Self {
            store,
            names,
            blocks: None,
            staged_limits: None,
            response_reserve: 0,
        };
        let operation = creator.journal().load(context, mapping.operation).await?;
        let Some(operation) = operation else {
            return Box::pin(
                crate::table::TableLifecycles::from_parts(creator.store, creator.names)
                    .help_reservation(context, mapping, budget),
            )
            .await;
        };
        if operation.mapping(TableMappingState::Reserved) != *mapping {
            return Err(ValidationError::IdentityMismatch.into());
        }
        creator
            .resume_with_budget(context, mapping.operation, budget)
            .await
            .map_err(catalog_error)?;
        Ok(())
    }
}

fn catalog_error(error: Error) -> CatalogError {
    match error {
        Error::Catalog(error) => error,
        Error::Validation(error) => error.into(),
        _ => ValidationError::Record.into(),
    }
}
