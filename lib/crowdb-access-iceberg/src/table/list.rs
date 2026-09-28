use std::sync::Arc;

use crowdb_chunk_kv_client::MultiScanContinuation;
use crowdb_protocol::chunk_kv::ScanDirection;

use crate::{
    catalog::{CatalogContext, CatalogError, CatalogStore, StoreError},
    error::ValidationError,
    key::{CatalogScope, IcebergKey},
    namespace::{
        child_range, ChildScan, NamespaceAuthority, NamespaceIdentifier, NamespaceRepository, NamespaceStore,
    },
    record::StorageRecord,
};

mod token;

pub struct TableLister {
    namespaces: NamespaceRepository,
    store: Arc<dyn CatalogStore>,
    names: Arc<dyn NamespaceStore>,
    tokens: token::Tokens,
}

#[derive(Clone, Copy, Debug)]
pub struct TableListLimits {
    pub page_size: usize,
    pub scanned: usize,
    pub names_bytes: usize,
}

#[derive(Debug)]
pub struct TableListPage {
    pub names: Vec<String>,
    pub next_page_token: Option<String>,
    pub scanned: usize,
}

impl TableLister {
    /// # Errors
    /// Rejects an invalid token signing key.
    pub fn new<Store: NamespaceStore + 'static>(
        store: Arc<Store>,
        secret: &[u8; 32],
    ) -> Result<Self, ValidationError> {
        Ok(Self {
            namespaces: NamespaceRepository::new(store.clone()),
            store: store.clone(),
            names: store,
            tokens: token::Tokens::new(secret)?,
        })
    }

    /// An absent token requests a complete bounded result; an empty token starts pagination.
    /// No partial response escapes when scanning or retained-name budgets are exhausted.
    /// # Errors
    /// Rejects foreign tokens, corrupt storage, exhausted budgets and namespace replacement.
    pub async fn list(
        &self,
        context: CatalogContext,
        namespace: &NamespaceIdentifier,
        limits: TableListLimits,
        token: Option<&str>,
    ) -> Result<Option<TableListPage>, CatalogError> {
        if limits.page_size == 0
            || limits.page_size > 100
            || limits.scanned == 0
            || limits.scanned > 100_000
            || limits.names_bytes == 0
            || limits.names_bytes > 16 * 1024 * 1024
        {
            return Err(ValidationError::RecordTooLarge.into());
        }
        let Some(parent) = self.namespaces.load(context, namespace).await? else {
            return Ok(None);
        };
        let mut result = TableListPage {
            names: Vec::new(),
            next_page_token: None,
            scanned: 0,
        };
        let mut cursor = token.unwrap_or("").to_owned();
        let mut bytes = 0_usize;
        loop {
            let page = self
                .page(
                    context,
                    &parent,
                    limits.page_size,
                    limits.scanned - result.scanned,
                    &cursor,
                )
                .await?;
            result.scanned += page.scanned;
            if result.scanned > limits.scanned {
                return Err(ValidationError::RecordTooLarge.into());
            }
            for name in page.names {
                bytes = bytes
                    .checked_add(name.len())
                    .ok_or(ValidationError::RecordTooLarge)?;
                if bytes > limits.names_bytes {
                    return Err(ValidationError::RecordTooLarge.into());
                }
                result.names.push(name);
            }
            result.next_page_token = page.next_page_token;
            if token.is_some() || result.next_page_token.is_none() {
                break;
            }
            if result.scanned == limits.scanned {
                return Err(ValidationError::RecordTooLarge.into());
            }
            cursor.clone_from(result.next_page_token.as_ref().ok_or(ValidationError::Key)?);
        }
        let current = self.namespaces.load(context, namespace).await?;
        if !current.is_some_and(|current| {
            current.namespace == parent.namespace && current.name_epoch == parent.name_epoch
        }) {
            return Err(CatalogError::Conflict);
        }
        Ok(Some(result))
    }

    async fn page(
        &self,
        context: CatalogContext,
        parent: &NamespaceAuthority,
        limit: usize,
        remaining: usize,
        token: &str,
    ) -> Result<TableListPage, CatalogError> {
        let binding = token::binding(context, parent, limit)?;
        let range = child_range(context.catalog, Some(parent.namespace), CatalogScope::TableName)?;
        let continuation = if token.is_empty() {
            None
        } else {
            let (generation, last_key) = self.tokens.decode(token, &binding)?;
            Some(MultiScanContinuation {
                direction: ScanDirection::Forward,
                original_start: Some(range.start.clone()),
                original_end: Some(range.end.clone()),
                last_key,
                catalog_generation: generation,
            })
        };
        let scan = ChildScan {
            catalog: context.catalog,
            parent: Some(parent.namespace),
            scope: CatalogScope::TableName,
            limit: limit.min(remaining),
            continuation,
        };
        scan.request()?;
        let page = self.names.scan_children(scan.clone()).await?;
        if let Some(failure) = page.terminal_failure {
            return Err(StoreError::Rejected(failure).into());
        }
        if page.items.len() > scan.limit {
            return Err(ValidationError::RecordTooLarge.into());
        }
        let scanned = page.items.len();
        let mut last = scan
            .continuation
            .as_ref()
            .map_or(&range.start, |cursor| &cursor.last_key)
            .clone();
        let mut names = Vec::new();
        for item in page.items {
            if item.key <= last || !range.contains(&item.key) {
                return Err(ValidationError::Key.into());
            }
            last = item.key;
            let key = IcebergKey::decode(&last)?;
            let StorageRecord::TableMapping(mapping) = StorageRecord::decode(&key, &item.value)? else {
                return Err(ValidationError::Record.into());
            };
            if mapping.state != super::TableMappingState::Published {
                continue;
            }
            let key = super::head_key(context.catalog, mapping.table);
            let Some(stored) = self.store.get(&key.encode()?).await? else {
                continue;
            };
            let StorageRecord::TableHead(head) = StorageRecord::decode(&key, &stored.bytes)? else {
                return Err(ValidationError::Record.into());
            };
            if mapping.resolves(&head) {
                names.push(mapping.name);
            }
        }
        let next_page_token = if let Some(cursor) = page.continuation {
            ChildScan {
                continuation: Some(cursor.clone()),
                ..scan
            }
            .request()?;
            if scanned == 0 || cursor.last_key < last {
                return Err(ValidationError::Key.into());
            }
            Some(
                self.tokens
                    .encode(&binding, cursor.catalog_generation, &cursor.last_key),
            )
        } else {
            None
        };
        Ok(TableListPage {
            names,
            next_page_token,
            scanned,
        })
    }
}
