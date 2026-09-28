use async_trait::async_trait;
use crowdb_chunk_kv_client::{MultiScanContinuation, MultiScanPage, MultiScanRequest};
use crowdb_protocol::chunk_kv::{ClientRequestId, ScanDirection};

use crate::catalog::{CasOutcome, CatalogStore, RoutedCatalogStore, StoreError};
use crate::error::ValidationError;
use crate::key::{CatalogId, CatalogScope, NamespaceId};
use crate::record::MAX_RECORD_BYTES;

use super::child_range;

#[derive(Clone, Debug)]
pub struct ChildScan {
    pub catalog: CatalogId,
    pub parent: Option<NamespaceId>,
    pub scope: CatalogScope,
    pub limit: usize,
    pub continuation: Option<MultiScanContinuation>,
}

impl ChildScan {
    /// # Errors
    /// Rejects non-child ranges, excessive pages and foreign continuations.
    pub fn request(&self) -> Result<MultiScanRequest, ValidationError> {
        let range = child_range(self.catalog, self.parent, self.scope)?;
        if self.limit == 0 || self.limit > 256 {
            return Err(ValidationError::RecordTooLarge);
        }
        if let Some(cursor) = &self.continuation {
            if cursor.direction != ScanDirection::Forward
                || cursor.original_start.as_ref() != Some(&range.start)
                || cursor.original_end.as_ref() != Some(&range.end)
                || cursor.catalog_generation == 0
                || !range.contains(&cursor.last_key)
            {
                return Err(ValidationError::Key);
            }
        }
        Ok(MultiScanRequest {
            start: Some(range.start),
            end: Some(range.end),
            direction: ScanDirection::Forward,
            max_items: self.limit,
            max_bytes: MAX_RECORD_BYTES * self.limit,
            continuation: self.continuation.clone(),
        })
    }
}

#[async_trait]
pub trait NamespaceStore: CatalogStore {
    async fn scan_children(&self, request: ChildScan) -> Result<MultiScanPage, StoreError>;

    async fn delete_mapping(
        &self,
        key: &[u8],
        expected: &[u8],
        identity: ClientRequestId,
    ) -> Result<CasOutcome, StoreError>;
}

#[async_trait]
impl NamespaceStore for RoutedCatalogStore {
    async fn scan_children(&self, request: ChildScan) -> Result<MultiScanPage, StoreError> {
        self.scan(request.request()?).await
    }

    async fn delete_mapping(
        &self,
        key: &[u8],
        expected: &[u8],
        identity: ClientRequestId,
    ) -> Result<CasOutcome, StoreError> {
        self.delete_mapping_if(key, expected, identity).await
    }
}
