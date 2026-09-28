use async_trait::async_trait;
use crowdb_access_iceberg::catalog::StoreError;
use crowdb_access_iceberg::namespace::{NamespaceRecoveryScan, NamespaceRecoveryStore};
use crowdb_chunk_kv_client::{MultiScanContinuation, MultiScanPage};
use crowdb_protocol::chunk_kv::RpcValue;

use crate::common::TestStore;

#[async_trait]
impl NamespaceRecoveryStore for TestStore {
    async fn scan_namespace_mappings(
        &self,
        scan: NamespaceRecoveryScan,
    ) -> Result<MultiScanPage, StoreError> {
        Ok(self.namespace_scan_page(scan.mappings_request()?))
    }
    async fn scan_namespace_operations(
        &self,
        scan: NamespaceRecoveryScan,
    ) -> Result<MultiScanPage, StoreError> {
        Ok(self.namespace_scan_page(scan.request()?))
    }
}

impl TestStore {
    fn namespace_scan_page(&self, request: crowdb_chunk_kv_client::MultiScanRequest) -> MultiScanPage {
        let snapshot = self.values.load_full();
        let mut candidates = snapshot.iter().filter(|(key, _)| {
            *key >= request.start.as_ref().unwrap()
                && *key < request.end.as_ref().unwrap()
                && request
                    .continuation
                    .as_ref()
                    .map_or(true, |cursor| *key > &cursor.last_key)
        });
        let items: Vec<_> = candidates
            .by_ref()
            .take(request.max_items)
            .map(|(key, value)| RpcValue {
                key: key.clone(),
                value: value.bytes.clone(),
                revision: value.revision,
            })
            .collect();
        let continuation = candidates.next().map(|_| MultiScanContinuation {
            direction: request.direction,
            original_start: request.start,
            original_end: request.end,
            last_key: items.last().unwrap().key.clone(),
            catalog_generation: 1,
        });
        MultiScanPage {
            items,
            continuation,
            terminal_failure: None,
        }
    }
}
