// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::sync::Arc;

use crowdb_access_iceberg::{
    catalog::{RoutedCatalogStore, StoreError},
    gc::{BudgetedGcStore, GcIoBudget, GcLocalScan, GcScan, GcStore, GcSystemScan},
    key::{CatalogId, CatalogScope, IcebergKey, OperationId, SystemScope},
};
use crowdb_chunk_kv_client::{MultiScanContinuation, MultiScanPage};
use crowdb_protocol::chunk_kv::{RpcValue, ScanDirection};

#[path = "common/gc_local_scan.rs"]
mod fixture;

fn scan(catalog: CatalogId) -> GcScan {
    GcScan {
        native: false,
        catalog,
        scope: Some(CatalogScope::GcTask),
        prefix: Vec::new(),
        after: Vec::new(),
        items: 2,
        bytes: 128 * 1024,
    }
}

fn raw_key(catalog: CatalogId, length: usize) -> Vec<u8> {
    let mut key = IcebergKey::catalog_range(catalog).start;
    key.push(CatalogScope::GcTask as u8);
    key.resize(length, 1);
    key
}

fn page(keys: &[Vec<u8>]) -> MultiScanPage {
    MultiScanPage {
        items: keys
            .iter()
            .map(|key| RpcValue {
                key: key.clone(),
                value: vec![7],
                revision: 1,
            })
            .collect(),
        continuation: None,
        terminal_failure: None,
    }
}

#[test]
fn malformed_raw_keys_use_exclusive_native_continuation_without_weakening_persistent_cursors() {
    let catalog = CatalogId::random();
    for length in [4_097, 64 * 1024] {
        let raw = raw_key(catalog, length);
        assert!(IcebergKey::decode(&raw).is_err());
        let mut durable = scan(catalog);
        durable.after = raw.clone();
        assert!(durable.request().is_err());
        let local = GcLocalScan::catalog(&scan(catalog), raw.clone()).unwrap();
        let request = local.request(7).unwrap();
        assert_eq!(request.start, scan(catalog).request().unwrap().start);
        let token = request.continuation.unwrap();
        assert_eq!(token.last_key, raw);
        assert_eq!(token.catalog_generation, 7);
        assert_eq!(token.direction, ScanDirection::Forward);
    }
    let mut raw =
        crowdb_access_iceberg::operation::ledger_key(SystemScope::ManagementOperation, OperationId::random())
            .unwrap()
            .encode()
            .unwrap();
    raw.resize(4_097, 1);
    let system = || GcSystemScan {
        after: Vec::new(),
        items: 1,
        bytes: 128 * 1024,
    };
    assert!(GcSystemScan {
        after: raw.clone(),
        ..system()
    }
    .request()
    .is_err());
    assert_eq!(
        GcLocalScan::system(&system(), raw.clone())
            .unwrap()
            .request(7)
            .unwrap()
            .continuation
            .unwrap()
            .last_key,
        raw
    );
}

#[test]
fn raw_positions_keep_catalog_scope_prefix_and_byte_bounds() {
    let catalog = CatalogId::random();
    let raw = raw_key(catalog, 4_097);
    let local = GcLocalScan::catalog(&scan(catalog), raw.clone()).unwrap();
    assert!(local.request(0).is_err());
    assert!(GcLocalScan::catalog(&scan(CatalogId::random()), raw.clone()).is_err());
    let mut scoped = scan(catalog);
    scoped.scope = Some(CatalogScope::GcCandidate);
    assert!(GcLocalScan::catalog(&scoped, raw.clone()).is_err());
    let mut prefixed = scan(catalog);
    prefixed.prefix = vec![2];
    assert!(GcLocalScan::catalog(&prefixed, raw.clone()).is_err());
    let mut too_small = scan(catalog);
    too_small.bytes = 4_096;
    let resumed = GcLocalScan::catalog(&too_small, raw.clone()).unwrap();
    assert_eq!(resumed.request(7).unwrap().continuation.unwrap().last_key, raw);
    let mut unbounded = scan(catalog);
    unbounded.items = 0;
    assert!(GcLocalScan::catalog(&unbounded, Vec::new()).is_err());
    let mut persisted = scan(catalog);
    persisted.after = IcebergKey::Catalog {
        catalog,
        scope: CatalogScope::GcTask,
        suffix: vec![2; 16],
    }
    .encode()
    .unwrap();
    assert!(GcLocalScan::catalog(&persisted, Vec::new()).is_err());
}

#[test]
fn raw_page_validation_rejects_reversed_repeated_foreign_or_unbounded_progress() {
    let catalog = CatalogId::random();
    let first = raw_key(catalog, 4_097);
    let mut child = first.clone();
    child.push(0);
    let local = GcLocalScan::catalog(&scan(catalog), first.clone()).unwrap();
    assert!(local.validate_page(&page(&[child.clone()])).is_ok());
    let cases = [
        page(&[first]),
        page(&[child.clone(), child.clone()]),
        page(&[child.clone(), raw_key(catalog, 4_096)]),
        page(&[raw_key(CatalogId::random(), 4_097)]),
    ];
    for invalid in cases {
        assert!(local.validate_page(&invalid).is_err());
    }
    let mut invalid = page(&[child.clone()]);
    invalid.items[0].revision = 0;
    assert!(local.validate_page(&invalid).is_err());
    invalid.items[0].revision = 1;
    invalid.items[0].value.resize(128 * 1024, 0);
    assert!(local.validate_page(&invalid).is_err());
    let mut invalid = page(&[child.clone()]);
    let request = local.request(7).unwrap();
    invalid.continuation = Some(MultiScanContinuation {
        direction: ScanDirection::Reverse,
        original_start: request.start,
        original_end: request.end,
        last_key: child,
        catalog_generation: 7,
    });
    assert!(local.validate_page(&invalid).is_err());
}

#[tokio::test]
async fn routed_local_scan_uses_a_real_generation_and_does_not_skip_raw_key_children() {
    let catalog = CatalogId::random();
    let raw = raw_key(catalog, 4_097);
    let mut child = raw.clone();
    child.push(0);
    let healthy = IcebergKey::Catalog {
        catalog,
        scope: CatalogScope::GcTask,
        suffix: vec![2; 16],
    }
    .encode()
    .unwrap();
    let transport = Arc::new(fixture::TestTransport::new(vec![
        raw.clone(),
        child.clone(),
        healthy.clone(),
    ]));
    let store = RoutedCatalogStore::new(fixture::client(transport.clone()));
    let first = store
        .scan_gc_local(GcLocalScan::catalog(&scan(catalog), Vec::new()).unwrap())
        .await
        .unwrap();
    assert_eq!(first.items[0].key, raw);
    let next = store
        .scan_gc_local(GcLocalScan::catalog(&scan(catalog), raw.clone()).unwrap())
        .await
        .unwrap();
    assert_eq!(
        next.items.iter().map(|item| item.key.clone()).collect::<Vec<_>>(),
        [child, healthy]
    );
    let requests = transport.requests.load();
    assert_eq!(requests.len(), 2);
    let token = requests[1].continuation.as_ref().unwrap();
    assert_eq!(token.last_key, raw);
    assert_eq!(token.map_revision, 7);
    assert!(store.scan(scan(catalog).request().unwrap()).await.is_err());
}

#[tokio::test]
async fn local_scan_reserves_the_existing_io_budget_before_dispatch() {
    let catalog = CatalogId::random();
    let transport = Arc::new(fixture::TestTransport::new(vec![raw_key(catalog, 4_097)]));
    let inner = Arc::new(RoutedCatalogStore::new(fixture::client(transport.clone())));
    let budget = Arc::new(GcIoBudget::new(128 * 1024, 1, 1024, 1));
    let store = BudgetedGcStore::new(inner, budget.clone());
    let local = GcLocalScan::catalog(&scan(catalog), Vec::new()).unwrap();
    assert_eq!(store.scan_gc_local(local.clone()).await.unwrap().items.len(), 1);
    assert!(matches!(
        store.scan_gc_local(local.clone()).await,
        Err(StoreError::Budget)
    ));
    assert_eq!(transport.requests.load().len(), 1);
    budget.reset();
    assert_eq!(store.scan_gc_local(local).await.unwrap().items.len(), 1);
    assert_eq!(transport.requests.load().len(), 2);
}
