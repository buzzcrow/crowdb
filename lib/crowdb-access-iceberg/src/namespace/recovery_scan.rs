use async_trait::async_trait;
use crowdb_chunk_kv_client::{MultiScanContinuation, MultiScanPage, MultiScanRequest};
use crowdb_protocol::chunk_kv::ScanDirection;

use crate::catalog::{RoutedCatalogStore, StoreError};
use crate::error::ValidationError;
use crate::key::{CatalogId, CatalogScope, IcebergKey};
use crate::record::MAX_RECORD_BYTES;

use super::NamespaceStore;

#[derive(Clone, Debug)]
pub struct NamespaceRecoveryScan {
    pub catalog: CatalogId,
    pub continuation: Option<MultiScanContinuation>,
}

impl NamespaceRecoveryScan {
    /// # Errors
    /// Rejects foreign, backward or malformed continuations.
    pub fn request(&self) -> Result<MultiScanRequest, ValidationError> {
        let mut start = IcebergKey::catalog_range(self.catalog).start;
        let mut end = start.clone();
        start.push(CatalogScope::NamespaceOperation as u8);
        end.push(CatalogScope::NamespaceOperation as u8 + 1);
        if let Some(cursor) = &self.continuation {
            if cursor.original_start.as_ref() != Some(&start)
                || cursor.original_end.as_ref() != Some(&end)
                || cursor.direction != ScanDirection::Forward
                || cursor.catalog_generation == 0
                || cursor.last_key < start
                || cursor.last_key >= end
            {
                return Err(ValidationError::Key);
            }
        }
        Ok(MultiScanRequest {
            start: Some(start),
            end: Some(end),
            direction: ScanDirection::Forward,
            max_items: 4,
            max_bytes: 4 * MAX_RECORD_BYTES,
            continuation: self.continuation.clone(),
        })
    }
}

#[async_trait]
pub trait NamespaceRecoveryStore: NamespaceStore {
    async fn scan_namespace_operations(
        &self,
        scan: NamespaceRecoveryScan,
    ) -> Result<MultiScanPage, StoreError>;
}

#[async_trait]
impl NamespaceRecoveryStore for RoutedCatalogStore {
    async fn scan_namespace_operations(
        &self,
        scan: NamespaceRecoveryScan,
    ) -> Result<MultiScanPage, StoreError> {
        self.scan(scan.request()?).await
    }
}
