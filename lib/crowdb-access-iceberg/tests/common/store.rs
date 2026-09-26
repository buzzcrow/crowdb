use std::collections::BTreeMap;
use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc,
};

use arc_swap::ArcSwap;
use async_trait::async_trait;
use crowdb_access_iceberg::catalog::{CasOutcome, CatalogStore, StoreError, StoredValue};
use crowdb_protocol::chunk_kv::ClientRequestId;

#[derive(Default)]
pub struct TestStore {
    pub values: ArcSwap<BTreeMap<Vec<u8>, StoredValue>>,
    pub fail_after: AtomicUsize,
    pub file_record_reply_loss: AtomicBool,
    pub gc_workspace_denied: AtomicBool,
    #[allow(dead_code)]
    pub gc_delete_reply_loss: AtomicBool,
    pub writes: AtomicUsize,
    pub fencing_delay_ms: AtomicUsize,
    pub fencing_barrier: Option<Arc<tokio::sync::Barrier>>,
    pub fencing_visits: AtomicUsize,
    pub namespace_update_barrier: Option<Arc<tokio::sync::Barrier>>,
    pub namespace_update_visits: AtomicUsize,
    pub namespace_reservation_barrier: Option<Arc<tokio::sync::Barrier>>,
    pub namespace_reservation_visits: AtomicUsize,
    pub file_mapping_barrier: Option<Arc<tokio::sync::Barrier>>,
    pub file_mapping_visits: AtomicUsize,
    pub file_mapping_pause: AtomicBool,
    pub file_mapping_entered: tokio::sync::Notify,
    pub file_mapping_release: tokio::sync::Notify,
    pub table_reservation_barrier: Option<Arc<tokio::sync::Barrier>>,
    pub table_reservation_visits: AtomicUsize,
    pub stage_transition_barrier: Option<Arc<tokio::sync::Barrier>>,
    pub stage_transition_visits: AtomicUsize,
    pub table_head_pause_before: AtomicBool,
    pub table_head_pause_after: AtomicBool,
    pub table_head_entered: tokio::sync::Notify,
    pub table_head_release: tokio::sync::Notify,
}

impl TestStore {
    fn deny_gc_workspace(&self, key: &[u8]) -> bool {
        self.gc_workspace_denied.load(Ordering::SeqCst)
            && matches!(
                crowdb_access_iceberg::key::IcebergKey::decode(key),
                Ok(crowdb_access_iceberg::key::IcebergKey::Catalog {
                    scope: crowdb_access_iceberg::key::CatalogScope::OperationPayload
                        | crowdb_access_iceberg::key::CatalogScope::GcClaim
                        | crowdb_access_iceberg::key::CatalogScope::GcCandidate,
                    ..
                })
            )
    }

    async fn pause_file_mapping(&self) {
        if self.file_mapping_pause.swap(false, Ordering::SeqCst) {
            self.file_mapping_entered.notify_one();
            self.file_mapping_release.notified().await;
        }
    }

    fn lose_file_reply(&self, key: &[u8]) -> bool {
        matches!(
            crowdb_access_iceberg::key::IcebergKey::decode(key),
            Ok(crowdb_access_iceberg::key::IcebergKey::Catalog {
                scope: crowdb_access_iceberg::key::CatalogScope::File,
                ..
            })
        ) && self.file_record_reply_loss.swap(false, Ordering::SeqCst)
    }

    async fn pause_table_head(&self, key: &[u8], expected: Option<&[u8]>, value: &[u8], after: bool) {
        if expected.is_none() {
            return;
        }
        let Ok(crowdb_access_iceberg::record::StorageRecord::TableHead(head)) =
            crowdb_access_iceberg::key::IcebergKey::decode(key)
                .and_then(|key| crowdb_access_iceberg::record::StorageRecord::decode(&key, value))
        else {
            return;
        };
        let enabled = if after {
            &self.table_head_pause_after
        } else {
            &self.table_head_pause_before
        };
        if head.pending_operation.is_some() && enabled.swap(false, Ordering::SeqCst) {
            self.table_head_entered.notify_one();
            self.table_head_release.notified().await;
        }
    }

    async fn pause_stage_transition(&self, key: &[u8], expected: Option<&[u8]>) {
        if let (Some(barrier), Some(expected)) = (&self.stage_transition_barrier, expected) {
            if let Ok(crowdb_access_iceberg::record::StorageRecord::TableCreateOperation(operation)) =
                crowdb_access_iceberg::key::IcebergKey::decode(key)
                    .and_then(|key| crowdb_access_iceberg::record::StorageRecord::decode(&key, expected))
            {
                if operation.phase == crowdb_access_iceberg::commit::TableCreatePhase::Staged
                    && self.stage_transition_visits.fetch_add(1, Ordering::SeqCst) < 2
                {
                    barrier.wait().await;
                }
            }
        }
    }
}

#[async_trait]
impl CatalogStore for TestStore {
    async fn get(&self, key: &[u8]) -> Result<Option<StoredValue>, StoreError> {
        Ok(self.values.load().get(key).cloned())
    }

    async fn compare_exchange(
        &self,
        key: &[u8],
        expected: Option<&[u8]>,
        value: &[u8],
        identity: ClientRequestId,
    ) -> Result<CasOutcome, StoreError> {
        identity.validate().unwrap();
        if self.deny_gc_workspace(key) {
            return Err(StoreError::Budget);
        }
        self.pause_table_head(key, expected, value, false).await;
        self.pause_stage_transition(key, expected).await;
        if matches!(
            crowdb_access_iceberg::key::IcebergKey::decode(key),
            Ok(crowdb_access_iceberg::key::IcebergKey::Catalog {
                scope: crowdb_access_iceberg::key::CatalogScope::FileLocation,
                ..
            })
        ) {
            self.pause_file_mapping().await;
            if let Some(barrier) = &self.file_mapping_barrier {
                if self.file_mapping_visits.fetch_add(1, Ordering::SeqCst) < 2 {
                    barrier.wait().await;
                }
            }
        }
        if expected.is_none() {
            if let Ok(crowdb_access_iceberg::record::StorageRecord::TableMapping(mapping)) =
                crowdb_access_iceberg::key::IcebergKey::decode(key)
                    .and_then(|key| crowdb_access_iceberg::record::StorageRecord::decode(&key, value))
            {
                if mapping.state == crowdb_access_iceberg::table::TableMappingState::Reserved {
                    if let Some(barrier) = &self.table_reservation_barrier {
                        if self.table_reservation_visits.fetch_add(1, Ordering::SeqCst) < 2 {
                            barrier.wait().await;
                        }
                    }
                }
            }
            if let Ok(crowdb_access_iceberg::record::StorageRecord::NamespaceMapping(mapping)) =
                crowdb_access_iceberg::key::IcebergKey::decode(key)
                    .and_then(|key| crowdb_access_iceberg::record::StorageRecord::decode(&key, value))
            {
                if mapping.state == crowdb_access_iceberg::namespace::NamespaceMappingState::Reserved {
                    if let Some(barrier) = &self.namespace_reservation_barrier {
                        if self.namespace_reservation_visits.fetch_add(1, Ordering::SeqCst) < 2 {
                            barrier.wait().await;
                        }
                    }
                }
            }
        }
        if let Ok(crowdb_access_iceberg::record::StorageRecord::NamespaceAuthority(authority)) =
            crowdb_access_iceberg::key::IcebergKey::decode(key)
                .and_then(|key| crowdb_access_iceberg::record::StorageRecord::decode(&key, value))
        {
            if authority.pending_operation.is_some() && expected.is_some() {
                if let Some(barrier) = &self.namespace_update_barrier {
                    if self.namespace_update_visits.fetch_add(1, Ordering::SeqCst) < 2 {
                        barrier.wait().await;
                    }
                }
            }
        }
        if let Ok(crowdb_access_iceberg::record::StorageRecord::Active(root)) =
            crowdb_access_iceberg::key::IcebergKey::decode(key)
                .and_then(|key| crowdb_access_iceberg::record::StorageRecord::decode(&key, value))
        {
            if root.state == crowdb_access_iceberg::catalog::RootState::Fencing {
                if let Some(barrier) = &self.fencing_barrier {
                    if self.fencing_visits.fetch_add(1, Ordering::SeqCst) < 2 {
                        barrier.wait().await;
                    }
                }
                let delay = self.fencing_delay_ms.swap(0, Ordering::SeqCst);
                tokio::time::sleep(std::time::Duration::from_millis(delay.try_into().unwrap())).await;
            }
        }
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
            let observed = self.values.compare_and_swap(&current, Arc::new(next));
            if Arc::ptr_eq(&current, &observed) {
                let writes = self.writes.fetch_add(1, Ordering::SeqCst) + 1;
                if self.fail_after.load(Ordering::SeqCst) == writes {
                    return Err(StoreError::Response);
                }
                if self.lose_file_reply(key) {
                    return Err(StoreError::Response);
                }
                self.pause_table_head(key, expected, value, true).await;
                return Ok(CasOutcome::Applied(revision));
            }
        }
    }
}
