use async_trait::async_trait;
use crowdb_chunk_kv_client::{MultiScanPage, MultiScanRequest};
use crowdb_protocol::chunk_kv::{ClientRequestId, ScanDirection};

use crate::{
    catalog::{CasOutcome, CatalogStore, RoutedCatalogStore, StoreError},
    error::ValidationError,
    key::{CatalogId, CatalogScope, IcebergKey},
};

#[derive(Clone, Debug)]
pub struct GcScan {
    pub catalog: CatalogId,
    pub scope: Option<CatalogScope>,
    pub prefix: Vec<u8>,
    pub after: Vec<u8>,
    pub items: usize,
    pub bytes: usize,
}

impl GcScan {
    /// # Errors
    /// Rejects unbounded scans and cursors outside the captured catalog/scope.
    pub fn request(&self) -> Result<MultiScanRequest, ValidationError> {
        if self.items == 0 || self.items > 256 || self.bytes == 0 || self.bytes > 16 * 1024 * 1024 {
            return Err(ValidationError::RecordTooLarge);
        }
        if self.prefix.len() > 40 || (!self.prefix.is_empty() && self.scope.is_none()) {
            return Err(ValidationError::Key);
        }
        let mut range = IcebergKey::catalog_range(self.catalog);
        if let Some(scope) = self.scope {
            range.end = range.start.clone();
            range.start.push(scope as u8);
            range.end.push(scope as u8 + 1);
        }
        if !self.prefix.is_empty() {
            range.start.extend_from_slice(&self.prefix);
            range.end = range.start.clone();
            while range.end.last() == Some(&255) {
                range.end.pop();
            }
            let last = range.end.last_mut().ok_or(ValidationError::Key)?;
            *last += 1;
        }
        if !self.after.is_empty() {
            if !range.contains(&self.after) {
                return Err(ValidationError::Key);
            }
            IcebergKey::decode(&self.after)?;
            range.start.clone_from(&self.after);
            if range.start.len() < crate::key::MAX_KEY_BYTES {
                range.start.push(0);
            } else {
                while range.start.last() == Some(&255) {
                    range.start.pop();
                }
                let last = range.start.last_mut().ok_or(ValidationError::Key)?;
                *last += 1;
            }
        }
        Ok(MultiScanRequest {
            start: Some(range.start),
            end: Some(range.end),
            direction: ScanDirection::Forward,
            max_items: self.items,
            max_bytes: self.bytes,
            continuation: None,
        })
    }

    /// # Errors
    /// Rejects unordered, foreign, oversized or failed scan pages.
    pub fn validate_page(&self, page: &MultiScanPage) -> Result<(), StoreError> {
        let request = self.request()?;
        let start = request.start.ok_or(ValidationError::Key)?;
        let end = request.end.ok_or(ValidationError::Key)?;
        if page.terminal_failure.is_some()
            || page.items.len() > self.items
            || page
                .items
                .iter()
                .map(|item| item.key.len() + item.value.len())
                .sum::<usize>()
                > self.bytes
            || page.items.windows(2).any(|items| items[0].key >= items[1].key)
            || page
                .items
                .iter()
                .any(|item| item.key < start || item.key >= end || item.revision == 0)
            || (page.items.is_empty() && page.continuation.is_some())
        {
            return Err(StoreError::Response);
        }
        Ok(())
    }
}

#[async_trait]
pub trait GcStore: CatalogStore {
    async fn scan_gc(&self, request: GcScan) -> Result<MultiScanPage, StoreError>;
    async fn delete_gc_record(
        &self,
        key: &[u8],
        expected: &[u8],
        identity: ClientRequestId,
    ) -> Result<CasOutcome, StoreError>;
}

#[async_trait]
impl GcStore for RoutedCatalogStore {
    async fn scan_gc(&self, request: GcScan) -> Result<MultiScanPage, StoreError> {
        let page = self.scan(request.request()?).await?;
        request.validate_page(&page)?;
        Ok(page)
    }

    async fn delete_gc_record(
        &self,
        key: &[u8],
        expected: &[u8],
        identity: ClientRequestId,
    ) -> Result<CasOutcome, StoreError> {
        self.delete_gc_record_if(key, expected, identity).await
    }
}
