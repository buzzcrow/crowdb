// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Historical response bytes remain decodable; new API calls use no ledger.

use crowdb_access_iceberg::catalog::{
    CasOutcome, CatalogContext, CatalogRepository, CatalogStore, RoutedCatalogStore,
};
use crowdb_access_iceberg::key::{IcebergKey, OperationId};
use crowdb_access_iceberg::operation::{mutation_identity, RequestIdentity, RetryRecord};
use crowdb_access_iceberg::record::StorageRecord;
use crowdb_chunk_kv_client::MultiScanRequest;
use crowdb_protocol::chunk_kv::ScanDirection;

use super::{now_ms, TestNativeStorage};

pub(super) async fn catalog_keys(
    store: &RoutedCatalogStore,
    context: CatalogContext,
) -> std::collections::BTreeSet<Vec<u8>> {
    let range = IcebergKey::catalog_range(context.catalog);
    let mut request = MultiScanRequest {
        start: Some(range.start.clone()),
        end: Some(range.end.clone()),
        direction: ScanDirection::Forward,
        max_items: 20,
        max_bytes: 1024 * 1024,
        continuation: None,
    };
    let mut keys = std::collections::BTreeSet::new();
    loop {
        let page = store.scan(request.clone()).await.unwrap();
        for item in page.items {
            assert!(range.contains(&item.key));
            assert!(keys.insert(item.key));
        }
        assert!(
            keys.len() <= 10_000,
            "fixture catalog exceeded its inventory bound"
        );
        request.continuation = page.continuation;
        if request.continuation.is_none() {
            return keys;
        }
    }
}

pub async fn verify_legacy_records(stack: &TestNativeStorage, repository: &CatalogRepository) {
    let store = stack.store().await;
    let context = repository.status().await.unwrap().0.context;
    for _ in 0..3 {
        let record = RetryRecord {
            identity: RequestIdentity {
                operation: OperationId::random(),
                issued_ms: now_ms(),
            },
            principal: "reader".into(),
            route: "legacy fixture mutation".into(),
            digest: [1; 32],
            context,
            retained_until_ms: now_ms() + 300_000,
            status: 409,
            body: b"conflict".to_vec(),
        };
        let key = record.result_key();
        let bytes = StorageRecord::Retry(Box::new(record.clone())).encode().unwrap();
        let encoded = key.encode().unwrap();
        assert!(matches!(
            store
                .compare_exchange(&encoded, None, &bytes, mutation_identity(&encoded, None, &bytes))
                .await
                .unwrap(),
            CasOutcome::Applied(_)
        ));
        let value = stack.store().await.get(&encoded).await.unwrap().unwrap();
        assert_eq!(
            StorageRecord::decode(&key, &value.bytes).unwrap(),
            StorageRecord::Retry(Box::new(record))
        );
    }
    let range = IcebergKey::catalog_range(context.catalog);
    let mut scan = MultiScanRequest {
        start: Some(range.start.clone()),
        end: Some(range.end.clone()),
        direction: ScanDirection::Forward,
        max_items: 1,
        max_bytes: 64 * 1024,
        continuation: None,
    };
    let mut count = 0;
    loop {
        let page = store.scan(scan.clone()).await.unwrap();
        assert!(page.items.len() <= 1);
        for item in &page.items {
            assert!(range.contains(&item.key));
        }
        count += page.items.len();
        scan.continuation = page.continuation;
        if scan.continuation.is_none() {
            break;
        }
        assert!(count <= 4);
    }
    assert_eq!(count, 4);
    scan.start = None;
    assert!(store.scan(scan).await.is_err());
}
