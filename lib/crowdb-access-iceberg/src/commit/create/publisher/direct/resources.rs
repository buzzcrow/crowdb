use super::{Error, NamespaceAuthority, NamespaceLifecycle, TableCreator, TableHead};
use crate::{
    catalog::{CasOutcome, CatalogContext, CatalogError},
    error::ValidationError,
    key::OperationId,
    namespace::authority_key,
    operation::mutation_identity,
    record::StorageRecord,
    table::{name_key, storage_head_key, TableMapping, TableMappingState, TableRenames},
};

impl TableCreator {
    pub(super) async fn publish_direct_creation(
        &self,
        context: CatalogContext,
        parent: &NamespaceAuthority,
        head: TableHead,
    ) -> Result<(), Error> {
        let mapping = TableMapping {
            catalog: head.catalog,
            storage_namespace: head.storage_location().storage_namespace,
            namespace: head.namespace,
            name: head.name.clone(),
            table: head.table,
            name_epoch: head.name_epoch,
            operation: OperationId::random(),
            state: TableMappingState::Reserved,
        };
        self.reserve_direct(context, &mapping).await?;
        let key = authority_key(context.catalog, parent.namespace);
        let ready = if let Some(value) = self.store.get(&key.encode()?).await.map_err(CatalogError::from)? {
            let StorageRecord::NamespaceAuthority(current) = StorageRecord::decode(&key, &value.bytes)?
            else {
                return Err(ValidationError::Record.into());
            };
            current.lifecycle == NamespaceLifecycle::Ready
                && current.namespace == parent.namespace
                && current.identifier == parent.identifier
                && current.name_epoch == parent.name_epoch
        } else {
            false
        };
        if !ready {
            TableRenames::from_parts(self.store.clone(), self.names.clone())
                .help_reserved(context, &mapping)
                .await?;
            return Err(Error::NamespaceMissing);
        }
        let key = storage_head_key(head.storage_location()).encode()?;
        let bytes = StorageRecord::TableHead(Box::new(head)).encode()?;
        match self
            .store
            .compare_exchange(&key, None, &bytes, mutation_identity(&key, None, &bytes))
            .await
            .map_err(CatalogError::from)?
        {
            CasOutcome::Applied(_) => {}
            CasOutcome::Conflict(Some(current)) if current.bytes == bytes => {}
            CasOutcome::Conflict(_) => return Err(CatalogError::Conflict.into()),
        }
        self.publish_direct_name(&mapping).await
    }

    async fn reserve_direct(&self, context: CatalogContext, mapping: &TableMapping) -> Result<(), Error> {
        let key = name_key(mapping.catalog, mapping.namespace, &mapping.name)?;
        let encoded = key.encode()?;
        let bytes = StorageRecord::TableMapping(mapping.clone()).encode()?;
        let current = match self
            .names
            .compare_exchange(&encoded, None, &bytes, mutation_identity(&encoded, None, &bytes))
            .await
            .map_err(CatalogError::from)?
        {
            CasOutcome::Applied(_) => return Ok(()),
            CasOutcome::Conflict(Some(current)) if current.bytes == bytes => return Ok(()),
            CasOutcome::Conflict(Some(current)) => current,
            CasOutcome::Conflict(None) => return Err(CatalogError::Busy.into()),
        };
        let StorageRecord::TableMapping(existing) = StorageRecord::decode(&key, &current.bytes)? else {
            return Err(ValidationError::Record.into());
        };
        if existing.state == TableMappingState::Reserved {
            TableRenames::from_parts(self.store.clone(), self.names.clone())
                .help_reserved(context, &existing)
                .await?;
            return Err(CatalogError::Busy.into());
        }
        let head = storage_head_key(existing.storage_location());
        if let Some(value) = self
            .store
            .get(&head.encode()?)
            .await
            .map_err(CatalogError::from)?
        {
            let record = StorageRecord::decode(&head, &value.bytes)?;
            if matches!(&record, StorageRecord::TableHead(head) if existing.resolves(head)) {
                return Err(Error::TableExists);
            }
            if !matches!(
                record,
                StorageRecord::TableHead(_) | StorageRecord::RetiredTable(_)
            ) {
                return Err(ValidationError::Record.into());
            }
        }
        let deleted = self
            .names
            .delete_mapping(
                &encoded,
                &current.bytes,
                mutation_identity(&encoded, Some(&current.bytes), &[]),
            )
            .await
            .map_err(CatalogError::from)?;
        if !matches!(deleted, CasOutcome::Applied(_) | CasOutcome::Conflict(None)) {
            return Err(CatalogError::Busy.into());
        }
        // The exact stale name is now absent; keep this request's candidate.
        match self
            .names
            .compare_exchange(&encoded, None, &bytes, mutation_identity(&encoded, None, &bytes))
            .await
            .map_err(CatalogError::from)?
        {
            CasOutcome::Applied(_) => Ok(()),
            CasOutcome::Conflict(Some(current)) if current.bytes == bytes => Ok(()),
            CasOutcome::Conflict(_) => Err(CatalogError::Busy.into()),
        }
    }

    async fn publish_direct_name(&self, mapping: &TableMapping) -> Result<(), Error> {
        let key = name_key(mapping.catalog, mapping.namespace, &mapping.name)?.encode()?;
        let before = StorageRecord::TableMapping(mapping.clone()).encode()?;
        let mut published = mapping.clone();
        published.state = TableMappingState::Published;
        let after = StorageRecord::TableMapping(published).encode()?;
        match self
            .names
            .compare_exchange(
                &key,
                Some(&before),
                &after,
                mutation_identity(&key, Some(&before), &after),
            )
            .await
            .map_err(CatalogError::from)?
        {
            CasOutcome::Applied(_) => Ok(()),
            CasOutcome::Conflict(Some(current)) if current.bytes == after => Ok(()),
            CasOutcome::Conflict(_) => Err(CatalogError::Conflict.into()),
        }
    }
}
