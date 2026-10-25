// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[path = "common/file_blocks.rs"]
mod blocks;
#[path = "common/store.rs"]
mod common;
#[path = "common/table_creation.rs"]
#[allow(dead_code)]
mod creation;
#[path = "common/namespace.rs"]
#[allow(dead_code)]
mod fixture;
#[path = "common/table_drop_interleave.rs"]
mod interleave;
#[path = "table_drop_read_cost/metadata.rs"]
mod metadata;
#[path = "common/namespace_store.rs"]
mod namespace_store;
#[path = "common/table_drop_reads.rs"]
mod reads;

use crowdb_access_iceberg::{
    catalog::{CatalogAuthority, CatalogError, StoreError},
    key::{CatalogScope, IcebergKey, OperationId},
    operation::RequestIdentity,
    record::StorageRecord,
    table::{
        storage_head_key, TableLifecycle, TableLifecycleAction, TableLifecycleRequest, TableLifecycles,
        TableRepository,
    },
};
use interleave::{TestDropInterleave, TestInterleave};
use std::sync::{atomic::Ordering, Arc};

async fn setup(depth: usize, native: bool) -> creation::TestCreation {
    let mut test = creation::TestCreation::new().await;
    if depth == 3 {
        let first = test.fixture.authority(None, &["root"]);
        test.fixture.publish(&first).await;
        let second = test.fixture.authority(Some(first.namespace), &["root", "branch"]);
        test.fixture.publish(&second).await;
        test.parent = test
            .fixture
            .authority(Some(second.namespace), &["root", "branch", "leaf"]);
        test.fixture.publish(&test.parent).await;
        test.request.namespace = test.parent.identifier.clone();
    }
    if native {
        test.fixture
            .put(
                IcebergKey::Catalog {
                    catalog: test.fixture.context.catalog,
                    scope: CatalogScope::Authority,
                    suffix: Vec::new(),
                },
                StorageRecord::Authority(
                    CatalogAuthority::new_native(test.fixture.context.catalog, "native".into()).unwrap(),
                ),
            )
            .await;
    }
    test.creator().create(&test.request).await.unwrap();
    test
}

#[tokio::test]
async fn public_table_selection_keeps_its_independent_context_guards_and_metadata_validation() {
    let test = setup(1, false).await;
    let store = Arc::new(reads::TestDropReads::new(test.fixture.store.clone()));
    TableRepository::new(store.clone())
        .select(test.fixture.context, test.parent.namespace, "events")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(store.counts(), [2, 0, 0, 1, 1, 1]);
}

#[tokio::test]
async fn rename_winning_during_metadata_selection_prevents_the_drop_without_a_name_reload() {
    for native in [false, true] {
        let test = setup(1, native).await;
        let store = TestDropInterleave::new(&test, TestInterleave::Rename).await;
        assert_eq!(
            TableLifecycles::new(store.clone())
                .execute(&request(&test, false))
                .await
                .unwrap()
                .status,
            409
        );
        assert_eq!(store.reads.counts(), [2, 1, 1, 1, 1, 1]);
        assert_eq!(store.reads.deletes.load(Ordering::SeqCst), 0);
        let renamed = TableRepository::new(test.fixture.store.clone())
            .select(test.fixture.context, test.parent.namespace, "moved")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(renamed.head.table, store.head.table);
        assert_eq!(renamed.head.lifecycle, TableLifecycle::Ready);
    }
}

#[tokio::test]
async fn catalog_clear_or_replacement_is_rejected_by_the_guard_before_head_publication() {
    for replacement in [false, true] {
        let test = setup(3, false).await;
        let store = TestDropInterleave::new(&test, TestInterleave::Clear(replacement)).await;
        let result = TableLifecycles::new(store.clone())
            .execute(&request(&test, true))
            .await;
        assert!(
            matches!(result, Err(CatalogError::Busy | CatalogError::Conflict)),
            "{result:?}"
        );
        assert_eq!(store.reads.counts(), [2, 3, 3, 1, 1, 1]);
        assert_eq!(store.reads.deletes.load(Ordering::SeqCst), 0);
        let key = storage_head_key(store.head.storage_location());
        assert_eq!(
            StorageRecord::decode(
                &key,
                &test.fixture.store.values.load()[&key.encode().unwrap()].bytes
            )
            .unwrap(),
            StorageRecord::TableHead(Box::new(store.head.clone()))
        );
    }
}

#[tokio::test]
async fn retirement_winning_after_the_last_guard_is_preserved_by_the_exact_head_cas() {
    for native in [false, true] {
        let test = setup(1, native).await;
        let store = TestDropInterleave::new(&test, TestInterleave::Retire).await;
        assert_eq!(
            TableLifecycles::new(store.clone())
                .execute(&request(&test, false))
                .await
                .unwrap()
                .status,
            409
        );
        assert_eq!(store.reads.counts(), [2, 1, 1, 1, 1, 1]);
        assert_eq!(store.reads.deletes.load(Ordering::SeqCst), 0);
        let key = storage_head_key(store.head.storage_location());
        assert_eq!(
            StorageRecord::decode(
                &key,
                &test.fixture.store.values.load()[&key.encode().unwrap()].bytes
            )
            .unwrap(),
            StorageRecord::RetiredTable(store.head.storage_location())
        );
    }
}

#[tokio::test]
async fn unknown_head_results_return_without_further_reads_or_name_mutation() {
    for after in [false, true] {
        let test = setup(1, true).await;
        let store = TestDropInterleave::new(&test, TestInterleave::Unknown(after)).await;
        assert!(matches!(
            TableLifecycles::new(store.clone())
                .execute(&request(&test, false))
                .await,
            Err(CatalogError::Store(StoreError::Response))
        ));
        assert_eq!(store.reads.counts(), [2, 1, 1, 1, 1, 1]);
        assert_eq!(store.reads_after_unknown.load(Ordering::SeqCst), 0);
        assert_eq!(store.writes_after_unknown.load(Ordering::SeqCst), 0);
        assert_eq!(store.reads.deletes.load(Ordering::SeqCst), 0);
    }
}

#[tokio::test]
async fn exact_original_mapping_bytes_cannot_remove_a_recreated_table_after_head_deletion() {
    for native in [false, true] {
        let test = setup(1, native).await;
        let store = TestDropInterleave::new(&test, TestInterleave::Recreate).await;
        assert_eq!(
            TableLifecycles::new(store.clone())
                .execute(&request(&test, false))
                .await
                .unwrap()
                .status,
            204
        );
        assert_eq!(store.reads.counts(), [3, 1, 1, 1, 1, 1]);
        let selected = TableRepository::new(test.fixture.store.clone())
            .select(test.fixture.context, test.parent.namespace, "events")
            .await
            .unwrap()
            .unwrap();
        assert_ne!(selected.head.table, store.head.table);
        assert_eq!(selected.head.lifecycle, TableLifecycle::Ready);
    }
}

#[tokio::test]
async fn a_successful_head_mutation_still_checks_the_final_catalog_context() {
    let test = setup(1, true).await;
    let store = TestDropInterleave::new(&test, TestInterleave::ClearAfterHead).await;
    assert!(matches!(
        TableLifecycles::new(store.clone())
            .execute(&request(&test, false))
            .await,
        Err(CatalogError::Conflict)
    ));
    assert_eq!(store.reads.counts(), [3, 1, 1, 1, 1, 1]);
    let key = storage_head_key(store.head.storage_location());
    let StorageRecord::TableHead(head) = StorageRecord::decode(
        &key,
        &test.fixture.store.values.load()[&key.encode().unwrap()].bytes,
    )
    .unwrap() else {
        panic!()
    };
    assert_eq!(head.lifecycle, TableLifecycle::Tombstone);
}

#[tokio::test]
async fn missing_namespace_or_table_still_finishes_with_a_context_guard() {
    for missing_namespace in [false, true] {
        let test = setup(3, false).await;
        let mut request = request(&test, true);
        if missing_namespace {
            request.namespace = crowdb_access_iceberg::namespace::NamespaceIdentifier::new(vec![
                "root".into(),
                "branch".into(),
                "missing".into(),
            ])
            .unwrap();
        } else {
            request.name = "missing".into();
        }
        let store = Arc::new(reads::TestDropReads::new(test.fixture.store.clone()));
        let writes = test.fixture.store.writes.load(Ordering::SeqCst);
        assert_eq!(
            TableLifecycles::new(store.clone())
                .execute(&request)
                .await
                .unwrap()
                .status,
            404
        );
        assert_eq!(
            store.counts(),
            if missing_namespace {
                [2, 3, 2, 0, 0, 0]
            } else {
                [2, 3, 3, 1, 0, 0]
            }
        );
        assert_eq!(test.fixture.store.writes.load(Ordering::SeqCst), writes);
    }
}

fn request(test: &creation::TestCreation, purge_requested: bool) -> TableLifecycleRequest {
    TableLifecycleRequest {
        context: test.fixture.context,
        identity: RequestIdentity {
            operation: OperationId::random(),
            issued_ms: 100,
        },
        principal: "writer".into(),
        namespace: test.request.namespace.clone(),
        name: "events".into(),
        action: TableLifecycleAction::Drop { purge_requested },
    }
}

#[tokio::test]
async fn drop_reads_each_binding_once_under_three_request_context_guards() {
    let mut observed = Vec::new();
    for depth in [1, 3] {
        for native in [false, true] {
            for purge in [false, true] {
                let test = setup(depth, native).await;
                let store = Arc::new(reads::TestDropReads::new(test.fixture.store.clone()));
                let writes = test.fixture.store.writes.load(Ordering::SeqCst);
                assert_eq!(
                    TableLifecycles::new(store.clone())
                        .execute(&request(&test, purge))
                        .await
                        .unwrap()
                        .status,
                    204
                );
                eprintln!(
                    "depth={depth}, native={native}, purge={purge}, reads={:?}",
                    store.counts()
                );
                observed.push((depth, native, purge, store.counts()));
                assert_eq!(
                    test.fixture.store.writes.load(Ordering::SeqCst) - writes,
                    if purge { 3 } else { 2 }
                );
                assert_eq!(store.deletes.load(Ordering::SeqCst), 1);
            }
        }
    }
    assert!(
        observed
            .iter()
            .all(|(depth, _, _, counts)| *counts == [3, *depth, *depth, 1, 1, 1]),
        "{observed:?}"
    );
}
