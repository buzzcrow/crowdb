use crowdb_chunk_kv_client::MultiScanContinuation;

use crate::catalog::{CatalogContext, CatalogError, StoreError};
use crate::error::ValidationError;
use crate::key::IcebergKey;
use crate::operation::mutation_identity;
use crate::record::StorageRecord;

use super::{
    authority_key, NamespaceAction, NamespaceJournal, NamespaceMapping, NamespaceMappingState,
    NamespaceRecovery, NamespaceRecoveryPage, NamespaceRecoveryScan,
};

impl NamespaceRecovery {
    /// # Errors
    /// Rejects invalid scan responses, retired catalogs and corrupt mapping envelopes.
    pub async fn repair_page(
        &self,
        context: CatalogContext,
        continuation: Option<MultiScanContinuation>,
    ) -> Result<NamespaceRecoveryPage, CatalogError> {
        self.creator.repository.check_context(context).await?;
        let scan = NamespaceRecoveryScan {
            catalog: context.catalog,
            continuation,
        };
        let request = scan.mappings_request()?;
        let page = self.store.scan_namespace_mappings(scan.clone()).await?;
        if let Some(failure) = page.terminal_failure {
            return Err(StoreError::Rejected(failure).into());
        }
        if page.items.len() > request.max_items {
            return Err(ValidationError::RecordTooLarge.into());
        }
        let start = request.start.as_ref().ok_or(ValidationError::Key)?;
        let end = request.end.as_ref().ok_or(ValidationError::Key)?;
        let mut last = request
            .continuation
            .as_ref()
            .map_or(start, |cursor| &cursor.last_key)
            .clone();
        let mut mappings = Vec::with_capacity(page.items.len());
        for item in page.items {
            if item.key <= last || item.key >= *end {
                return Err(ValidationError::Key.into());
            }
            last.clone_from(&item.key);
            let key = IcebergKey::decode(&item.key)?;
            let StorageRecord::NamespaceMapping(mapping) = StorageRecord::decode(&key, &item.value)? else {
                return Err(ValidationError::Record.into());
            };
            mappings.push((item, mapping));
        }
        if let Some(cursor) = &page.continuation {
            NamespaceRecoveryScan {
                continuation: Some(cursor.clone()),
                ..scan
            }
            .mappings_request()?;
            if cursor.last_key < last || mappings.is_empty() {
                return Err(ValidationError::Key.into());
            }
        }
        let mut report = NamespaceRecoveryPage {
            continuation: page.continuation,
            completed: 0,
            deferred: 0,
            failures: Vec::new(),
        };
        for (item, mapping) in mappings {
            match self
                .repair_mapping(context, &mapping, &item.key, &item.value)
                .await
            {
                Ok(()) => report.completed += 1,
                Err(CatalogError::Busy) => report.deferred += 1,
                Err(error) => report.failures.push((mapping.operation, error)),
            }
        }
        self.creator.repository.check_context(context).await?;
        Ok(report)
    }

    async fn repair_mapping(
        &self,
        context: CatalogContext,
        mapping: &NamespaceMapping,
        key: &[u8],
        bytes: &[u8],
    ) -> Result<(), CatalogError> {
        if mapping.state == NamespaceMappingState::Reserved {
            let owner = NamespaceJournal::new(self.creator.repository.store.clone())
                .load(context, mapping.operation)
                .await?
                .ok_or(ValidationError::Record)?;
            if owner.action != NamespaceAction::Create
                || owner.namespace != mapping.namespace
                || owner.parent != mapping.parent
                || owner.identifier.name() != mapping.name
            {
                return Err(ValidationError::IdentityMismatch.into());
            }
            self.creator
                .resume_with_budget(context, mapping.operation, &mut 16)
                .await?;
            return Ok(());
        }
        let authority_key = authority_key(context.catalog, mapping.namespace);
        if let Some(value) = self.store.get(&authority_key.encode()?).await? {
            let StorageRecord::NamespaceAuthority(authority) =
                StorageRecord::decode(&authority_key, &value.bytes)?
            else {
                return Err(ValidationError::Record.into());
            };
            if mapping.resolves(&authority) {
                return Ok(());
            }
        }
        self.creator.repository.check_context(context).await?;
        self.store
            .delete_mapping(key, bytes, mutation_identity(key, Some(bytes), &[]))
            .await?;
        Ok(())
    }
}
