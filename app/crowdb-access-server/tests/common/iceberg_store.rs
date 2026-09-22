use arc_swap::ArcSwap;
use async_trait::async_trait;
use crowdb_access_iceberg::catalog::{CasOutcome, CatalogStore, StoreError, StoredValue};
use crowdb_protocol::chunk_kv::ClientRequestId;
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

#[derive(Default)]
pub struct TestStore {
    values: ArcSwap<BTreeMap<Vec<u8>, StoredValue>>,
    pub read_delay_ms: AtomicU64,
}

#[async_trait]
impl CatalogStore for TestStore {
    async fn get(&self, key: &[u8]) -> Result<Option<StoredValue>, StoreError> {
        let value = self.values.load().get(key).cloned();
        let delay = self.read_delay_ms.load(Ordering::SeqCst);
        if delay != 0 {
            tokio::time::sleep(std::time::Duration::from_millis(delay)).await;
        }
        Ok(value)
    }
    async fn compare_exchange(
        &self,
        key: &[u8],
        expected: Option<&[u8]>,
        value: &[u8],
        identity: ClientRequestId,
    ) -> Result<CasOutcome, StoreError> {
        identity.validate().unwrap();
        loop {
            let current = self.values.load_full();
            let previous = current.get(key);
            if previous.map(|value| value.bytes.as_slice()) != expected {
                return Ok(CasOutcome::Conflict(previous.cloned()));
            }
            let revision = previous.map_or(1, |value| value.revision + 1);
            let mut next = (*current).clone();
            next.insert(
                key.to_vec(),
                StoredValue {
                    bytes: value.to_vec(),
                    revision,
                },
            );
            if Arc::ptr_eq(&current, &self.values.compare_and_swap(&current, Arc::new(next))) {
                return Ok(CasOutcome::Applied(revision));
            }
        }
    }
}
