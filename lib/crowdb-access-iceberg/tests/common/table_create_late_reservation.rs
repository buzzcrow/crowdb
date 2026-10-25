// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc,
};

use arc_swap::ArcSwapOption;
use async_trait::async_trait;
use crowdb_access_iceberg::{
    catalog::{CasOutcome, CatalogStore, StoreError, StoredValue},
    file::FileKind,
    key::{IcebergKey, NamespaceId, StorageKey},
    namespace::{ChildScan, NamespaceStore},
    operation::mutation_identity,
    record::StorageRecord,
    table::{storage_head_key, TableHead, TableLifecycle, TableMapping, TableMappingState},
};
use crowdb_chunk_kv_client::MultiScanPage;
use crowdb_protocol::chunk_kv::ClientRequestId;
use tokio::sync::Notify;

use crate::common::TestStore;

pub struct TestLateReservationStore {
    pub inner: Arc<TestStore>,
    parent: NamespaceId,
    armed: AtomicBool,
    pub entered: Notify,
    pub release: Notify,
    pub mapping: ArcSwapOption<TableMapping>,
    pub hits: AtomicUsize,
    pub reservations: AtomicUsize,
    pub retired_reads: AtomicUsize,
    pub head_fences: AtomicUsize,
    pub head_attempts: AtomicUsize,
    pub exact_removals: AtomicUsize,
}

impl TestLateReservationStore {
    pub fn new(inner: Arc<TestStore>, parent: NamespaceId) -> Self {
        Self {
            inner,
            parent,
            armed: AtomicBool::new(true),
            entered: Notify::new(),
            release: Notify::new(),
            mapping: ArcSwapOption::empty(),
            hits: AtomicUsize::new(0),
            reservations: AtomicUsize::new(0),
            retired_reads: AtomicUsize::new(0),
            head_fences: AtomicUsize::new(0),
            head_attempts: AtomicUsize::new(0),
            exact_removals: AtomicUsize::new(0),
        }
    }

    pub async fn assert_delayed_head_is_fenced(&self, mapping: &TableMapping) {
        let snapshot = self.inner.values.load_full();
        let file = snapshot
            .iter()
            .find_map(|(key, value)| {
                let record = StorageKey::decode(key)
                    .and_then(|key| StorageRecord::decode(&key, &value.bytes))
                    .ok()?;
                match record {
                    StorageRecord::File(file)
                        if file.kind == FileKind::Metadata
                            && file.location.table() == mapping.storage_location() =>
                    {
                        Some(file)
                    }
                    _ => None,
                }
            })
            .unwrap();
        let bytes = file
            .content
            .inline_bytes(file.length, &file.digest)
            .unwrap()
            .unwrap();
        let metadata: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        let head = TableHead {
            catalog: mapping.catalog,
            table: mapping.table,
            namespace: mapping.namespace,
            name: mapping.name.clone(),
            name_epoch: mapping.name_epoch,
            lifecycle: TableLifecycle::Ready,
            generation: 1,
            metadata_file: file.file,
            metadata_location: file.location.clone(),
            metadata_digest: file.digest,
            commit_binding: None,
            format_version: u8::try_from(metadata["format-version"].as_u64().unwrap()).unwrap(),
            table_uuid: Some(uuid::Uuid::parse_str(metadata["table-uuid"].as_str().unwrap()).unwrap()),
            operation_fence: 1,
            pending_operation: None,
        };
        let key = storage_head_key(mapping.storage_location());
        let bytes = StorageRecord::TableHead(Box::new(head)).encode().unwrap();
        assert!(matches!(
            StorageRecord::decode(&key, &bytes).unwrap(),
            StorageRecord::TableHead(_)
        ));
        let encoded = key.encode().unwrap();
        let result = self
            .inner
            .compare_exchange(&encoded, None, &bytes, mutation_identity(&encoded, None, &bytes))
            .await
            .unwrap();
        let CasOutcome::Conflict(Some(current)) = result else {
            panic!("retired head slot reopened: {result:?}");
        };
        assert_eq!(
            StorageRecord::decode(&key, &current.bytes).unwrap(),
            StorageRecord::RetiredTable(mapping.storage_location())
        );
    }
}

#[async_trait]
impl CatalogStore for TestLateReservationStore {
    async fn get(&self, key: &[u8]) -> Result<Option<StoredValue>, StoreError> {
        let value = self.inner.get(key).await?;
        if value.as_ref().is_some_and(|value| {
            matches!(
                IcebergKey::decode(key).and_then(|key| StorageRecord::decode(&key, &value.bytes)),
                Ok(StorageRecord::RetiredNamespace { namespace, .. }) if namespace == self.parent
            )
        }) {
            self.retired_reads.fetch_add(1, Ordering::SeqCst);
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
        let record = StorageKey::decode(key).and_then(|key| StorageRecord::decode(&key, value))?;
        let mut reservation = false;
        if let StorageRecord::TableMapping(mapping) = &record {
            if expected.is_none()
                && mapping.state == TableMappingState::Reserved
                && self.armed.swap(false, Ordering::SeqCst)
            {
                self.mapping.store(Some(Arc::new(mapping.clone())));
                self.hits.fetch_add(1, Ordering::SeqCst);
                self.entered.notify_one();
                self.release.notified().await;
                reservation = true;
            }
        }
        if matches!(record, StorageRecord::TableHead(_)) {
            self.head_attempts.fetch_add(1, Ordering::SeqCst);
        }
        let fence = matches!(record, StorageRecord::RetiredTable(_));
        let result = self
            .inner
            .compare_exchange(key, expected, value, identity)
            .await?;
        if matches!(result, CasOutcome::Applied(_)) {
            if reservation {
                self.reservations.fetch_add(1, Ordering::SeqCst);
            }
            if fence {
                self.head_fences.fetch_add(1, Ordering::SeqCst);
            }
        }
        Ok(result)
    }
}

#[async_trait]
impl NamespaceStore for TestLateReservationStore {
    async fn scan_children(&self, scan: ChildScan) -> Result<MultiScanPage, StoreError> {
        self.inner.scan_children(scan).await
    }
    async fn delete_mapping(
        &self,
        key: &[u8],
        expected: &[u8],
        identity: ClientRequestId,
    ) -> Result<CasOutcome, StoreError> {
        let result = self.inner.delete_mapping(key, expected, identity).await?;
        if matches!(result, CasOutcome::Applied(_))
            && self.mapping.load_full().is_some_and(|mapping| {
                matches!(IcebergKey::decode(key).and_then(|key| StorageRecord::decode(&key, expected)),
                Ok(StorageRecord::TableMapping(current)) if current == *mapping)
            })
        {
            self.exact_removals.fetch_add(1, Ordering::SeqCst);
        }
        Ok(result)
    }
}

pub async fn recreate_parent(
    test: &crate::creation::TestCreation,
) -> crowdb_access_iceberg::namespace::NamespaceAuthority {
    use crowdb_access_iceberg::{
        key::OperationId,
        namespace::{NamespaceCreateRequest, NamespaceCreator, NamespaceProperties, NamespaceRepository},
        operation::RequestIdentity,
    };
    NamespaceCreator::new(test.fixture.store.clone())
        .create(&NamespaceCreateRequest {
            context: test.fixture.context,
            identity: RequestIdentity {
                operation: OperationId::random(),
                issued_ms: 100,
            },
            principal: "writer".into(),
            identifier: test.parent.identifier.clone(),
            properties: NamespaceProperties::default(),
        })
        .await
        .unwrap();
    NamespaceRepository::new(test.fixture.store.clone())
        .load(test.fixture.context, &test.parent.identifier)
        .await
        .unwrap()
        .unwrap()
}
