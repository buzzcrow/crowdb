use arc_swap::ArcSwap;
use async_trait::async_trait;
use crowdb_access_iceberg::catalog::{CasOutcome, CatalogStore, StoreError, StoredValue};
use crowdb_protocol::chunk_kv::ClientRequestId;
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicU8, Ordering};
use std::sync::Arc;

#[derive(Default)]
pub struct TestStore {
    pub values: ArcSwap<BTreeMap<Vec<u8>, StoredValue>>,
    pub read_delay_ms: AtomicU64,
    pub scan_delay_ms: AtomicU64,
    pub scans: AtomicU64,
    pub lose_reply_kind: AtomicU8,
    pub pause_file_read: AtomicBool,
    pub file_read_entered: tokio::sync::Notify,
    pub file_read_release: tokio::sync::Notify,
}

#[async_trait]
impl crowdb_access_iceberg::namespace::NamespaceStore for TestStore {
    async fn scan_children(
        &self,
        scan: crowdb_access_iceberg::namespace::ChildScan,
    ) -> Result<crowdb_chunk_kv_client::MultiScanPage, StoreError> {
        let request = scan.request()?;
        self.scans.fetch_add(1, Ordering::SeqCst);
        tokio::time::sleep(std::time::Duration::from_millis(
            self.scan_delay_ms.load(Ordering::SeqCst),
        ))
        .await;
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
            .map(|(key, value)| crowdb_protocol::chunk_kv::RpcValue {
                key: key.clone(),
                value: value.bytes.clone(),
                revision: value.revision,
            })
            .collect();
        let continuation = candidates
            .next()
            .map(|_| crowdb_chunk_kv_client::MultiScanContinuation {
                direction: request.direction,
                original_start: request.start,
                original_end: request.end,
                last_key: items.last().unwrap().key.clone(),
                catalog_generation: 1,
            });
        Ok(crowdb_chunk_kv_client::MultiScanPage {
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
            if Arc::ptr_eq(&current, &self.values.compare_and_swap(&current, Arc::new(next))) {
                return Ok(CasOutcome::Applied(revision));
            }
        }
    }
}

#[async_trait]
impl CatalogStore for TestStore {
    async fn get(&self, key: &[u8]) -> Result<Option<StoredValue>, StoreError> {
        let value = self.values.load().get(key).cloned();
        if matches!(
            crowdb_access_iceberg::key::IcebergKey::decode(key),
            Ok(crowdb_access_iceberg::key::IcebergKey::Catalog {
                scope: crowdb_access_iceberg::key::CatalogScope::File,
                ..
            })
        ) && self.pause_file_read.swap(false, Ordering::SeqCst)
        {
            self.file_read_entered.notify_one();
            self.file_read_release.notified().await;
        }
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
                let mode = self.lose_reply_kind.load(Ordering::SeqCst);
                let lose = mode == 1
                    || match crowdb_access_iceberg::key::IcebergKey::decode(key) {
                        Ok(crowdb_access_iceberg::key::IcebergKey::Catalog { scope, .. }) => {
                            (mode == 2
                                && scope == crowdb_access_iceberg::key::CatalogScope::NamespaceAuthority)
                                || (mode == 3 && scope == crowdb_access_iceberg::key::CatalogScope::Operation)
                                || (mode == 5 && scope == crowdb_access_iceberg::key::CatalogScope::TableHead)
                                || (mode == 4
                                    && scope
                                        == crowdb_access_iceberg::key::CatalogScope::TableCommitOperation
                                    && matches!(crowdb_access_iceberg::key::IcebergKey::decode(key).and_then(|key|
                                        crowdb_access_iceberg::record::StorageRecord::decode(&key, value)),
                                        Ok(crowdb_access_iceberg::record::StorageRecord::TableCommitOperation(operation))
                                            if operation.phase == crowdb_access_iceberg::commit::TableCommitPhase::Rejected))
                        }
                        _ => false,
                    };
                if lose && self.lose_reply_kind.swap(0, Ordering::SeqCst) != 0 {
                    return Err(StoreError::Response);
                }
                return Ok(CasOutcome::Applied(revision));
            }
        }
    }
}
