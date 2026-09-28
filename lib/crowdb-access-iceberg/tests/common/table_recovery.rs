use crate::common::TestStore;
use async_trait::async_trait;
use crowdb_access_iceberg::{
    catalog::StoreError,
    commit::{TableRecoveryScan, TableRecoveryStore},
};
use crowdb_chunk_kv_client::{MultiScanContinuation, MultiScanPage};
use crowdb_protocol::chunk_kv::RpcValue;

#[async_trait]
impl TableRecoveryStore for TestStore {
    async fn scan_table_operations(&self, scan: TableRecoveryScan) -> Result<MultiScanPage, StoreError> {
        let request = scan.request()?;
        let values = self.values.load_full();
        let mut candidates = values.iter().filter(|(key, _)| {
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
        Ok(MultiScanPage {
            items,
            continuation,
            terminal_failure: None,
        })
    }
}
