use async_trait::async_trait;
use crowdb_chunk_kv_client::{MultiScanPage, MultiScanRequest};
use crowdb_protocol::chunk_kv::ScanDirection;

use crate::catalog::{CatalogStore, RoutedCatalogStore, StoreError};
use crate::error::ValidationError;
use crate::key::{CatalogId, CatalogScope, IcebergKey, OperationId};
use crate::record::MAX_RECORD_BYTES;

#[derive(Clone, Debug)]
pub struct MultipartPartScan {
    pub catalog: CatalogId,
    pub upload: OperationId,
    pub after: u16,
    pub limit: u16,
}

impl MultipartPartScan {
    /// # Errors
    /// Rejects invalid part markers or unbounded storage pages.
    pub fn request(&self) -> Result<MultiScanRequest, ValidationError> {
        if self.after > 10_000 || self.limit == 0 || self.limit > 256 {
            return Err(ValidationError::Key);
        }
        let mut start = IcebergKey::catalog_range(self.catalog).start;
        start.push(CatalogScope::MultipartPart as u8);
        start.extend_from_slice(self.upload.as_bytes());
        let mut end = start.clone();
        start.extend_from_slice(&(self.after + 1).to_be_bytes());
        end.push(u8::MAX);
        Ok(MultiScanRequest {
            start: Some(start),
            end: Some(end),
            direction: ScanDirection::Forward,
            max_items: usize::from(self.limit),
            max_bytes: usize::from(self.limit) * MAX_RECORD_BYTES,
            continuation: None,
        })
    }
}

#[async_trait]
pub trait MultipartPartStore: CatalogStore {
    async fn scan_multipart_parts(&self, scan: MultipartPartScan) -> Result<MultiScanPage, StoreError>;
}

#[async_trait]
impl MultipartPartStore for RoutedCatalogStore {
    async fn scan_multipart_parts(&self, scan: MultipartPartScan) -> Result<MultiScanPage, StoreError> {
        self.scan(scan.request()?).await
    }
}
