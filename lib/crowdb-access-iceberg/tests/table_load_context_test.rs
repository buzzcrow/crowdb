// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#[path = "common/file_blocks.rs"]
mod blocks;
#[path = "common/store.rs"]
mod common;
#[path = "common/table_read.rs"]
mod fixture;
#[path = "common/table_metadata.rs"]
#[allow(dead_code)]
mod metadata;
#[path = "common/namespace_store.rs"]
mod namespace_store;
#[path = "common/namespace.rs"]
mod namespaces;
#[path = "common/table_context.rs"]
mod observed;

use crowdb_access_iceberg::{
    catalog::{Capabilities, CatalogAuthority, CatalogError, RootState},
    key::{CatalogScope, IcebergKey},
    namespace::NamespaceIdentifier,
    record::StorageRecord,
    table::{SnapshotLoadingMode, TableLoad, TableLoadError},
};
use fixture::TestTable;
use observed::{TestContextStore, TestReset};
use std::sync::atomic::Ordering;

fn retired<Value: std::fmt::Debug>(error: &Result<Value, TableLoadError>, reset: TestReset) {
    assert!(
        match reset {
            TestReset::Fencing => matches!(error, Err(TableLoadError::Catalog(CatalogError::Busy))),
            TestReset::Replacement | TestReset::ReusedCatalog => {
                matches!(error, Err(TableLoadError::Catalog(CatalogError::Conflict)))
            }
        },
        "{reset:?}: {error:?}"
    );
}

#[tokio::test]
async fn composite_load_and_head_use_two_root_checks_and_never_write() {
    let table = TestTable::new().await;
    for mode in [SnapshotLoadingMode::All, SnapshotLoadingMode::Refs] {
        let store = TestContextStore::new(&table, 0, TestReset::Replacement);
        let writes = table.fixture.store.writes.load(Ordering::Relaxed);
        let TableLoad::Loaded { metadata, .. } = store
            .loader(&table)
            .load(
                table.fixture.context,
                &table.parent.identifier,
                &table.head.name,
                mode,
                None,
            )
            .await
            .unwrap()
        else {
            panic!("expected metadata");
        };
        if mode == SnapshotLoadingMode::All {
            assert_eq!(metadata, table.bytes);
        }
        assert_eq!(store.roots.load(Ordering::Relaxed), 2);
        assert_eq!(store.reads.load(Ordering::Relaxed), 10);
        assert_eq!(table.fixture.store.writes.load(Ordering::Relaxed), writes);
    }
    let store = TestContextStore::new(&table, 0, TestReset::Replacement);
    assert_eq!(
        store
            .loader(&table)
            .head(table.fixture.context, &table.parent.identifier, &table.head.name)
            .await
            .unwrap(),
        Some(table.head.clone())
    );
    assert_eq!(store.roots.load(Ordering::Relaxed), 2);
    assert_eq!(store.reads.load(Ordering::Relaxed), 10);
}

#[tokio::test]
async fn every_metadata_read_window_is_covered_by_the_final_context_check() {
    for reset in [
        TestReset::Fencing,
        TestReset::Replacement,
        TestReset::ReusedCatalog,
    ] {
        for after in 2..=9 {
            for mode in [SnapshotLoadingMode::All, SnapshotLoadingMode::Refs] {
                let table = TestTable::new().await;
                let store = TestContextStore::new(&table, after, reset);
                let result = store
                    .loader(&table)
                    .load(
                        table.fixture.context,
                        &table.parent.identifier,
                        &table.head.name,
                        mode,
                        Some("*"),
                    )
                    .await;
                retired(&result, reset);
                assert_eq!(store.roots.load(Ordering::Relaxed), 2, "after GET {after}");
            }
        }
    }
}

#[tokio::test]
async fn every_head_read_window_is_covered_by_the_final_context_check() {
    for reset in [
        TestReset::Fencing,
        TestReset::Replacement,
        TestReset::ReusedCatalog,
    ] {
        for after in 2..=9 {
            let table = TestTable::new().await;
            let store = TestContextStore::new(&table, after, reset);
            let result = store
                .loader(&table)
                .head(table.fixture.context, &table.parent.identifier, &table.head.name)
                .await;
            retired(&result, reset);
            assert_eq!(store.roots.load(Ordering::Relaxed), 2, "after GET {after}");
        }
    }
}

#[tokio::test]
async fn reset_during_payload_read_cannot_escape_the_final_context_fence() {
    let table = TestTable::new().await;
    let store = TestContextStore::new(&table, 0, TestReset::Replacement);
    table.blocks.pause_reads.store(true, Ordering::Relaxed);
    let loader = store.loader(&table);
    let read = loader.load(
        table.fixture.context,
        &table.parent.identifier,
        &table.head.name,
        SnapshotLoadingMode::All,
        Some("*"),
    );
    tokio::pin!(read);
    tokio::select! {
        result = &mut read => panic!("unexpected completion {result:?}"),
        () = table.blocks.read_entered.notified() => {}
    }
    store.replace_root().await;
    table.blocks.pause_reads.store(false, Ordering::Relaxed);
    table.blocks.read_release.notify_one();
    retired(&read.await, TestReset::Replacement);
    assert_eq!(store.roots.load(Ordering::Relaxed), 2);
}

#[tokio::test]
async fn missing_namespace_and_table_paths_still_finish_with_a_context_check() {
    for missing_namespace in [true, false] {
        let table = TestTable::new().await;
        let store = TestContextStore::new(
            &table,
            if missing_namespace { 2 } else { 4 },
            TestReset::Replacement,
        );
        let namespace = if missing_namespace {
            NamespaceIdentifier::new(vec!["missing".into()]).unwrap()
        } else {
            table.parent.identifier.clone()
        };
        let result = store
            .loader(&table)
            .load(
                table.fixture.context,
                &namespace,
                "missing",
                SnapshotLoadingMode::All,
                None,
            )
            .await;
        retired(&result, TestReset::Replacement);
        assert_eq!(store.roots.load(Ordering::Relaxed), 2);
        assert_eq!(
            store.reads.load(Ordering::Relaxed),
            if missing_namespace { 3 } else { 7 }
        );
    }
}

#[tokio::test]
async fn a_storage_failure_returns_without_a_confirmation_or_final_context_read() {
    let table = TestTable::new().await;
    let store = TestContextStore::new(&table, 0, TestReset::Replacement);
    store.fail_at.store(4, Ordering::Relaxed);
    let result = store
        .loader(&table)
        .load(
            table.fixture.context,
            &table.parent.identifier,
            &table.head.name,
            SnapshotLoadingMode::All,
            None,
        )
        .await;
    assert!(matches!(
        result,
        Err(TableLoadError::Catalog(CatalogError::Store(
            crowdb_access_iceberg::catalog::StoreError::Response
        )))
    ));
    assert_eq!(store.reads.load(Ordering::Relaxed), 4);
    assert_eq!(store.roots.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn unavailable_contexts_fail_before_any_namespace_or_table_read() {
    for reset in [
        TestReset::Fencing,
        TestReset::Replacement,
        TestReset::ReusedCatalog,
    ] {
        let table = TestTable::new().await;
        let store = TestContextStore::new(&table, 0, reset);
        store.replace_root().await;
        let reads = table.fixture.store.reads.load(Ordering::Relaxed);
        let result = table
            .loader()
            .load(
                table.fixture.context,
                &table.parent.identifier,
                &table.head.name,
                SnapshotLoadingMode::All,
                None,
            )
            .await;
        retired(&result, reset);
        assert_eq!(table.fixture.store.reads.load(Ordering::Relaxed) - reads, 1);
    }
}

#[tokio::test]
async fn monotonic_capability_expansion_during_a_read_keeps_the_original_permission() {
    let table = TestTable::new().await;
    let profile = Capabilities::from_bits(0x0300).unwrap();
    let authority = CatalogAuthority::new(table.fixture.context.catalog, "test".into())
        .unwrap()
        .activated(profile)
        .unwrap();
    let key = IcebergKey::Catalog {
        catalog: authority.catalog,
        scope: CatalogScope::Authority,
        suffix: vec![],
    };
    table
        .fixture
        .put(key.clone(), StorageRecord::Authority(authority.clone()))
        .await;
    let store = TestContextStore::new(&table, 0, TestReset::Replacement);
    table.blocks.pause_reads.store(true, Ordering::Relaxed);
    let loader = store.loader(&table);
    let read = loader.load_with_capabilities(
        table.fixture.context,
        &table.parent.identifier,
        &table.head.name,
        SnapshotLoadingMode::All,
        profile,
    );
    tokio::pin!(read);
    tokio::select! {
        result = &mut read => panic!("unexpected completion {result:?}"),
        () = table.blocks.read_entered.notified() => {}
    }
    table
        .fixture
        .root(table.fixture.context, RootState::Fencing)
        .await;
    let expanded = authority
        .activated(Capabilities::from_bits(0x0333).unwrap())
        .unwrap();
    assert!(expanded.activated(profile).is_err());
    table.fixture.put(key, StorageRecord::Authority(expanded)).await;
    table.fixture.root(table.fixture.context, RootState::Ready).await;
    table.blocks.pause_reads.store(false, Ordering::Relaxed);
    table.blocks.read_release.notify_one();
    let TableLoad::Loaded { metadata, .. } = read.await.unwrap() else {
        panic!("expected metadata");
    };
    assert_eq!(metadata, table.bytes);
    assert_eq!(store.roots.load(Ordering::Relaxed), 2);
    assert_eq!(store.reads.load(Ordering::Relaxed), 10);
}
