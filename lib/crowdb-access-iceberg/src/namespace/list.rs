use std::sync::Arc;

use crowdb_chunk_kv_client::MultiScanContinuation;
use crowdb_protocol::chunk_kv::ScanDirection;

use crate::catalog::{CatalogContext, CatalogError, StoreError};
use crate::error::ValidationError;
use crate::key::{CatalogScope, IcebergKey, NamespaceId};
use crate::record::StorageRecord;

use super::list_token::ListTokens;
use super::{
    authority_key, child_range, ChildScan, NamespaceIdentifier, NamespaceMappingState, NamespaceRepository,
    NamespaceStore,
};

pub struct NamespaceLister {
    repository: NamespaceRepository,
    tokens: ListTokens,
}

#[derive(Debug)]
pub struct NamespaceListPage {
    pub namespaces: Vec<NamespaceIdentifier>,
    pub next_page_token: Option<String>,
    pub scanned: usize,
}

impl NamespaceLister {
    /// # Errors
    /// Rejects an invalid signing key.
    pub fn new<Store: NamespaceStore + 'static>(
        store: Arc<Store>,
        secret: &[u8; 32],
    ) -> Result<Self, ValidationError> {
        Ok(Self {
            repository: NamespaceRepository::new(store),
            tokens: ListTokens::new(secret)?,
        })
    }

    /// # Errors
    /// Rejects invalid tokens, bounds, corrupt storage and retired catalog contexts.
    /// Returns no page when the requested parent does not exist.
    pub async fn page(
        &self,
        context: CatalogContext,
        parent: Option<&NamespaceIdentifier>,
        limit: usize,
        token: &str,
    ) -> Result<Option<NamespaceListPage>, CatalogError> {
        if limit == 0 || limit > 100 {
            return Err(ValidationError::Text.into());
        }
        self.repository.check_context(context).await?;
        let Some(parent_id) = self.parent(context, parent).await? else {
            return Ok(None);
        };
        let binding = ListTokens::binding(context, parent_id, parent, limit)?;
        let range = child_range(context.catalog, parent_id, CatalogScope::NamespaceName)?;
        let continuation = if token.is_empty() {
            None
        } else {
            let (catalog_generation, last_key) = self.tokens.decode(token, &binding)?;
            Some(MultiScanContinuation {
                direction: ScanDirection::Forward,
                original_start: Some(range.start.clone()),
                original_end: Some(range.end.clone()),
                last_key,
                catalog_generation,
            })
        };
        let scan = ChildScan {
            catalog: context.catalog,
            parent: parent_id,
            scope: CatalogScope::NamespaceName,
            limit,
            continuation,
        };
        scan.request()?;
        let page = self.repository.names.scan_children(scan.clone()).await?;
        if let Some(failure) = page.terminal_failure {
            return Err(StoreError::Rejected(failure).into());
        }
        if page.items.len() > limit {
            return Err(ValidationError::RecordTooLarge.into());
        }
        let mut last = scan
            .continuation
            .as_ref()
            .map_or(&range.start, |cursor| &cursor.last_key)
            .clone();
        let scanned = page.items.len();
        let mut namespaces = Vec::new();
        for item in page.items {
            if item.key <= last || !range.contains(&item.key) {
                return Err(ValidationError::Key.into());
            }
            last.clone_from(&item.key);
            if let Some(identifier) = self.visible_child(&item.key, &item.value, parent).await? {
                namespaces.push(identifier);
            }
        }
        let next_page_token = if let Some(cursor) = page.continuation {
            ChildScan {
                continuation: Some(cursor.clone()),
                ..scan
            }
            .request()?;
            if cursor.last_key < last || scanned == 0 {
                return Err(ValidationError::Key.into());
            }
            Some(
                self.tokens
                    .encode(&binding, cursor.catalog_generation, &cursor.last_key),
            )
        } else {
            None
        };
        if self.parent(context, parent).await? != Some(parent_id) {
            return Err(CatalogError::Conflict);
        }
        self.repository.check_context(context).await?;
        Ok(Some(NamespaceListPage {
            namespaces,
            next_page_token,
            scanned,
        }))
    }

    async fn parent(
        &self,
        context: CatalogContext,
        identifier: Option<&NamespaceIdentifier>,
    ) -> Result<Option<Option<NamespaceId>>, CatalogError> {
        match identifier {
            None => Ok(Some(None)),
            Some(identifier) => Ok(self
                .repository
                .load(context, identifier)
                .await?
                .map(|authority| Some(authority.namespace))),
        }
    }

    async fn visible_child(
        &self,
        encoded: &[u8],
        bytes: &[u8],
        parent: Option<&NamespaceIdentifier>,
    ) -> Result<Option<NamespaceIdentifier>, CatalogError> {
        let key = IcebergKey::decode(encoded)?;
        let StorageRecord::NamespaceMapping(mapping) = StorageRecord::decode(&key, bytes)? else {
            return Err(ValidationError::Record.into());
        };
        if mapping.state != NamespaceMappingState::Published {
            return Ok(None);
        }
        let key = authority_key(mapping.catalog, mapping.namespace);
        let Some(value) = self.repository.store.get(&key.encode()?).await? else {
            return Ok(None);
        };
        let StorageRecord::NamespaceAuthority(authority) = StorageRecord::decode(&key, &value.bytes)? else {
            return Err(ValidationError::Record.into());
        };
        Ok(
            (mapping.resolves(&authority) && authority.identifier.parent().as_ref() == parent)
                .then_some(authority.identifier.clone()),
        )
    }
}
