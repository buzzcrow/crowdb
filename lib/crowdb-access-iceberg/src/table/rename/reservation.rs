use super::{
    mutation_identity, name_key, storage_head_key, CasOutcome, CatalogContext, CatalogError, NamespaceRename,
    StorageRecord, TableMapping, TableMappingState, TableRenames, ValidationError,
};
use crate::table::{retire_absent_table, TableSlot};

impl TableRenames {
    pub(super) async fn reserve(
        &self,
        context: CatalogContext,
        rename: &NamespaceRename,
    ) -> Result<(), CatalogError> {
        let mapping = rename.destination(TableMappingState::Reserved);
        let key = name_key(mapping.catalog, mapping.namespace, &mapping.name)?;
        let encoded = key.encode()?;
        let bytes = StorageRecord::TableMapping(mapping).encode()?;
        for _ in 0..8 {
            match self
                .names
                .compare_exchange(&encoded, None, &bytes, mutation_identity(&encoded, None, &bytes))
                .await?
            {
                CasOutcome::Applied(_) => return Ok(()),
                CasOutcome::Conflict(Some(value)) if value.bytes == bytes => return Ok(()),
                CasOutcome::Conflict(Some(value)) => {
                    let StorageRecord::TableMapping(existing) = StorageRecord::decode(&key, &value.bytes)?
                    else {
                        return Err(ValidationError::Record.into());
                    };
                    if existing.state == TableMappingState::Reserved {
                        Box::pin(crate::commit::TableCreator::help_reservation(
                            self.store.clone(),
                            self.names.clone(),
                            context,
                            &existing,
                            &mut 16,
                        ))
                        .await?;
                        return Err(CatalogError::Busy);
                    }
                    let head_key = storage_head_key(existing.storage_location());
                    if let Some(head) = self.store.get(&head_key.encode()?).await? {
                        let record = StorageRecord::decode(&head_key, &head.bytes)?;
                        if matches!(&record, StorageRecord::TableHead(head) if existing.resolves(head)) {
                            return Err(CatalogError::Conflict);
                        }
                        if !matches!(
                            record,
                            StorageRecord::TableHead(_) | StorageRecord::RetiredTable(_)
                        ) {
                            return Err(ValidationError::Record.into());
                        }
                    }
                    self.remove(&existing).await?;
                }
                CasOutcome::Conflict(None) => return Err(CatalogError::Busy),
            }
        }
        Err(CatalogError::Busy)
    }

    pub(super) async fn remove(&self, mapping: &TableMapping) -> Result<(), CatalogError> {
        let key = name_key(mapping.catalog, mapping.namespace, &mapping.name)?.encode()?;
        let bytes = StorageRecord::TableMapping(mapping.clone()).encode()?;
        self.names
            .delete_mapping(&key, &bytes, mutation_identity(&key, Some(&bytes), &[]))
            .await?;
        Ok(())
    }

    pub(crate) async fn help_reserved(
        &self,
        context: CatalogContext,
        mapping: &TableMapping,
    ) -> Result<(), CatalogError> {
        let key = crate::namespace::authority_key(context.catalog, mapping.namespace);
        if let Some(parent) = self.store.get(&key.encode()?).await? {
            match StorageRecord::decode(&key, &parent.bytes)? {
                StorageRecord::NamespaceAuthority(parent) => {
                    if let Some(rename) = parent
                        .rename
                        .as_ref()
                        .filter(|rename| rename.destination(TableMappingState::Reserved) == *mapping)
                    {
                        return self.recover(context, rename).await;
                    }
                }
                StorageRecord::RetiredNamespace { .. } => {}
                _ => return Err(ValidationError::Record.into()),
            }
        }
        // Occupancy can be cleared only after the old head CAS is fenced or settled.
        if let TableSlot::Existing(head) =
            retire_absent_table(self.store.as_ref(), mapping.storage_location()).await?
        {
            let mut published = mapping.clone();
            published.state = TableMappingState::Published;
            if published.resolves(&head) {
                return self.publish_name(mapping).await;
            }
        }
        self.remove(mapping).await
    }

    pub(super) async fn publish_name(&self, reserved: &TableMapping) -> Result<(), CatalogError> {
        let key = name_key(reserved.catalog, reserved.namespace, &reserved.name)?.encode()?;
        let before = StorageRecord::TableMapping(reserved.clone()).encode()?;
        let mut published = reserved.clone();
        published.state = TableMappingState::Published;
        let bytes = StorageRecord::TableMapping(published).encode()?;
        let result = self
            .names
            .compare_exchange(
                &key,
                Some(&before),
                &bytes,
                mutation_identity(&key, Some(&before), &bytes),
            )
            .await?;
        match result {
            CasOutcome::Applied(_) => Ok(()),
            CasOutcome::Conflict(Some(observed)) if observed.bytes == bytes => Ok(()),
            CasOutcome::Conflict(_) => Err(CatalogError::Busy),
        }
    }
}
