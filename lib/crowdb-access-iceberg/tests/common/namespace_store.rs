use std::sync::{atomic::Ordering, Arc};

use async_trait::async_trait;
use crowdb_access_iceberg::catalog::{CasOutcome, StoreError};
use crowdb_access_iceberg::namespace::{ChildScan, NamespaceStore};
use crowdb_chunk_kv_client::{MultiScanContinuation, MultiScanPage};
use crowdb_protocol::chunk_kv::{ClientRequestId, RpcValue};

use crate::common::TestStore;

#[async_trait]
impl NamespaceStore for TestStore {
    async fn scan_children(&self, scan: ChildScan) -> Result<MultiScanPage, StoreError> {
        let request = scan.request()?;
        let snapshot = self.values.load_full();
        let mut candidates = snapshot.iter().filter(|(key, _)| {
            request.start.as_ref().map_or(true, |start| *key >= start)
                && request.end.as_ref().map_or(true, |end| *key < end)
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
        let continuation = if candidates.next().is_some() {
            Some(MultiScanContinuation {
                direction: request.direction,
                original_start: request.start,
                original_end: request.end,
                last_key: items.last().unwrap().key.clone(),
                catalog_generation: 1,
            })
        } else {
            None
        };
        Ok(MultiScanPage {
            items,
            continuation,
            terminal_failure: None,
        })
    }

    async fn delete_mapping(
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
                let writes = self.writes.fetch_add(1, Ordering::SeqCst) + 1;
                if self.fail_after.load(Ordering::SeqCst) == writes {
                    return Err(StoreError::Response);
                }
                return Ok(CasOutcome::Applied(revision));
            }
        }
    }
}
