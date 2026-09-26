use std::sync::Arc;

use async_trait::async_trait;
use crowdb_access_iceberg::{
    catalog::{CasOutcome, StoreError},
    gc::{GcScan, GcStore, GcSystemScan},
};
use crowdb_chunk_kv_client::MultiScanPage;
use crowdb_protocol::chunk_kv::{ClientRequestId, RpcValue};

use crate::common::TestStore;

#[async_trait]
impl GcStore for TestStore {
    async fn scan_gc(&self, scan: GcScan) -> Result<MultiScanPage, StoreError> {
        let request = scan.request()?;
        let values = self.values.load();
        let mut items = Vec::new();
        let mut bytes = 0;
        for (key, value) in values.iter() {
            if request.start.as_ref().is_some_and(|start| key < start)
                || request.end.as_ref().is_some_and(|end| key >= end)
            {
                continue;
            }
            let size = key.len() + value.bytes.len();
            if items.is_empty() && size > request.max_bytes {
                return Err(StoreError::Response);
            }
            if items.len() == request.max_items || bytes + size > request.max_bytes {
                break;
            }
            items.push(RpcValue {
                key: key.clone(),
                value: value.bytes.clone(),
                revision: value.revision,
            });
            bytes += size;
        }
        let page = MultiScanPage {
            items,
            continuation: None,
            terminal_failure: None,
        };
        scan.validate_page(&page)?;
        Ok(page)
    }

    async fn scan_gc_system(&self, scan: GcSystemScan) -> Result<MultiScanPage, StoreError> {
        let request = scan.request()?;
        let values = self.values.load();
        let mut items = Vec::new();
        let mut bytes = 0;
        for (key, value) in values.iter() {
            if request.start.as_ref().is_some_and(|start| key < start)
                || request.end.as_ref().is_some_and(|end| key >= end)
            {
                continue;
            }
            let size = key.len() + value.bytes.len();
            if items.is_empty() && size > request.max_bytes {
                return Err(StoreError::Response);
            }
            if items.len() == request.max_items || bytes + size > request.max_bytes {
                break;
            }
            items.push(RpcValue {
                key: key.clone(),
                value: value.bytes.clone(),
                revision: value.revision,
            });
            bytes += size;
        }
        let page = MultiScanPage {
            items,
            continuation: None,
            terminal_failure: None,
        };
        scan.validate_page(&page)?;
        Ok(page)
    }

    async fn delete_gc_record(
        &self,
        key: &[u8],
        expected: &[u8],
        identity: ClientRequestId,
    ) -> Result<CasOutcome, StoreError> {
        identity.validate().unwrap();
        loop {
            let current = self.values.load_full();
            let previous = current.get(key);
            if previous.map(|value| value.bytes.as_slice()) != Some(expected) {
                return Ok(CasOutcome::Conflict(previous.cloned()));
            }
            let revision = previous.unwrap().revision + 1;
            let mut next = (*current).clone();
            next.remove(key);
            let observed = self.values.compare_and_swap(&current, Arc::new(next));
            if Arc::ptr_eq(&current, &observed) {
                if self
                    .gc_delete_reply_loss
                    .swap(false, std::sync::atomic::Ordering::Relaxed)
                {
                    return Err(StoreError::Response);
                }
                return Ok(CasOutcome::Applied(revision));
            }
        }
    }
}
