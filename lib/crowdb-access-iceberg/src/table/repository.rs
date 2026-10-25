use std::sync::Arc;

use super::{TableHead, TableMappingState};
use crate::{
    catalog::{check_context, CatalogContext, CatalogError, CatalogStore, StoredValue},
    error::ValidationError,
    file::{ContentFormat, FileKind, FileRecord, FileRepository},
    key::NamespaceId,
    record::StorageRecord,
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SelectedTable {
    pub head: TableHead,
    pub metadata: FileRecord,
}

#[derive(Clone)]
pub struct TableRepository {
    store: Arc<dyn CatalogStore>,
    files: FileRepository,
}

impl TableRepository {
    #[must_use]
    pub fn new(store: Arc<dyn CatalogStore>) -> Self {
        Self {
            files: FileRepository::new(store.clone()),
            store,
        }
    }

    /// Resolves a name through a head and immutable metadata authority from that head.
    /// Does not validate metadata JSON or implement a table creation publisher.
    /// # Errors
    /// Corruption, missing selected files and retired catalog contexts are errors, not absence.
    pub async fn select(
        &self,
        context: CatalogContext,
        namespace: NamespaceId,
        name: &str,
    ) -> Result<Option<SelectedTable>, CatalogError> {
        check_context(self.store.as_ref(), context).await?;
        let selected = self.resolve(context, namespace, name).await?;
        check_context(self.store.as_ref(), context).await?;
        Ok(selected)
    }

    /// Rechecks the complete selected head, including name and lifecycle fences.
    /// This is a read-side freshness check, not a substitute for publication CAS.
    /// # Errors
    /// Rejects a retired context, missing authority, corruption or any head change.
    pub async fn ensure_current(
        &self,
        context: CatalogContext,
        selected: &SelectedTable,
    ) -> Result<(), CatalogError> {
        check_context(self.store.as_ref(), context).await?;
        self.ensure_current_in_context(context, selected).await?;
        check_context(self.store.as_ref(), context).await
    }

    pub(super) async fn ensure_current_in_context(
        &self,
        context: CatalogContext,
        selected: &SelectedTable,
    ) -> Result<(), CatalogError> {
        if selected.head.catalog != context.catalog {
            return Err(ValidationError::IdentityMismatch.into());
        }
        let key = super::storage_head_key(selected.head.storage_location());
        let value = self
            .store
            .get(&key.encode()?)
            .await?
            .ok_or(CatalogError::Conflict)?;
        let head = match StorageRecord::decode(&key, &value.bytes)? {
            StorageRecord::TableHead(head) => head,
            StorageRecord::RetiredTable(_) => return Err(CatalogError::Conflict),
            _ => return Err(ValidationError::Record.into()),
        };
        if *head != selected.head {
            return Err(CatalogError::Conflict);
        }
        Ok(())
    }

    pub(super) async fn resolve(
        &self,
        context: CatalogContext,
        namespace: NamespaceId,
        name: &str,
    ) -> Result<Option<SelectedTable>, CatalogError> {
        Ok(self
            .resolve_with_mapping(context, namespace, name)
            .await?
            .map(|(selected, _)| selected))
    }

    // Composite requests guard their context and retain the observed name bytes for exact mutations.
    pub(super) async fn resolve_with_mapping(
        &self,
        context: CatalogContext,
        namespace: NamespaceId,
        name: &str,
    ) -> Result<Option<(SelectedTable, StoredValue)>, CatalogError> {
        let key = super::name_key(context.catalog, namespace, name)?;
        let Some(source) = self.store.get(&key.encode()?).await? else {
            return Ok(None);
        };
        let StorageRecord::TableMapping(mapping) = StorageRecord::decode(&key, &source.bytes)? else {
            return Err(ValidationError::Record.into());
        };
        if mapping.state != TableMappingState::Published {
            return Ok(None);
        }
        let key = super::storage_head_key(mapping.storage_location());
        let Some(value) = self.store.get(&key.encode()?).await? else {
            return Ok(None);
        };
        let head = match StorageRecord::decode(&key, &value.bytes)? {
            StorageRecord::TableHead(head) => head,
            StorageRecord::RetiredTable(_) => return Ok(None),
            _ => return Err(ValidationError::Record.into()),
        };
        if head.lifecycle == super::TableLifecycle::Reclaiming {
            return Err(CatalogError::Busy);
        }
        if !mapping.resolves(&head) {
            return Ok(None);
        }
        let metadata = self
            .files
            .load_in_context(context, &head.metadata_location)
            .await?
            .ok_or(ValidationError::Record)?;
        if metadata.file != head.metadata_file
            || metadata.digest != head.metadata_digest
            || metadata.kind != FileKind::Metadata
            || metadata.format != ContentFormat::Json
        {
            return Err(ValidationError::IdentityMismatch.into());
        }
        Ok(Some((
            SelectedTable {
                head: *head,
                metadata,
            },
            source,
        )))
    }
}
