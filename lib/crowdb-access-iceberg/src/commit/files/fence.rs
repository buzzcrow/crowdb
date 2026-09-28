use std::sync::Arc;

use crate::{
    catalog::{CatalogContext, CatalogError, CatalogStore},
    commit::{PriorManifestSource, TableCreateJournal, TableCreateOperation},
    record::StorageRecord,
    table::{head_key, name_key, TableMappingState, TableRepository},
};

pub(super) enum CandidateFence {
    Generation(Arc<PriorManifestSource>),
    Creation(Box<TableCreateOperation>),
}

impl CandidateFence {
    pub(super) fn prior(&self) -> Option<&Arc<PriorManifestSource>> {
        match self {
            Self::Generation(prior) => Some(prior),
            Self::Creation(_) => None,
        }
    }

    pub(super) async fn check(
        &self,
        store: Arc<dyn CatalogStore>,
        context: CatalogContext,
    ) -> Result<(), CatalogError> {
        match self {
            Self::Generation(prior) => {
                TableRepository::new(store)
                    .ensure_current(context, prior.selected())
                    .await
            }
            Self::Creation(operation) => {
                let journal = TableCreateJournal::new(store.clone());
                if journal
                    .load(context, operation.identity.operation)
                    .await?
                    .as_ref()
                    != Some(operation)
                {
                    return Err(CatalogError::Conflict);
                }
                let mapping = operation.mapping(TableMappingState::Reserved);
                let key = name_key(mapping.catalog, mapping.namespace, &mapping.name)?;
                let value = store.get(&key.encode()?).await?.ok_or(CatalogError::Conflict)?;
                if StorageRecord::decode(&key, &value.bytes)? != StorageRecord::TableMapping(mapping) {
                    return Err(CatalogError::Conflict);
                }
                let head = head_key(context.catalog, operation.candidate.table).encode()?;
                if store.get(&head).await?.is_some()
                    || journal
                        .load(context, operation.identity.operation)
                        .await?
                        .as_ref()
                        != Some(operation)
                {
                    return Err(CatalogError::Conflict);
                }
                Ok(())
            }
        }
    }
}
