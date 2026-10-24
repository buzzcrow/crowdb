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

use crowdb_access_iceberg::{
    catalog::{CatalogError, RootState},
    key::{CatalogId, OperationId},
    namespace::{
        authority_key, NamespaceDropRequest, NamespaceDropper, NamespaceIdentifier, NamespaceLifecycle,
    },
    operation::RequestIdentity,
    record::StorageRecord,
    table::{SnapshotLoadingMode, TableLoad, TableLoadError},
};
use fixture::TestTable;
use std::sync::atomic::Ordering;

async fn nested() -> TestTable {
    let mut test = TestTable::new().await;
    let root = test.fixture.authority(None, &["root"]);
    test.fixture.publish(&root).await;
    let branch = test.fixture.authority(Some(root.namespace), &["root", "branch"]);
    test.fixture.publish(&branch).await;
    test.parent.parent = Some(branch.namespace);
    test.parent.identifier =
        NamespaceIdentifier::new(vec!["root".into(), "branch".into(), "analytics".into()]).unwrap();
    test.fixture.publish(&test.parent).await;
    test
}

#[tokio::test]
async fn table_requests_resolve_ancestors_once_and_recheck_the_bound_leaf() {
    for depth in [1, 3] {
        let test = if depth == 1 {
            TestTable::new().await
        } else {
            nested().await
        };
        let reads = test.fixture.store.reads.load(Ordering::SeqCst);
        let mutations = test.fixture.store.cas_attempts.load(Ordering::SeqCst);
        assert!(test
            .loader()
            .exists(test.fixture.context, &test.parent.identifier, &test.head.name)
            .await
            .unwrap());
        assert_eq!(
            test.fixture.store.reads.load(Ordering::SeqCst) - reads,
            14 + 2 * depth
        );
        assert_eq!(test.fixture.store.cas_attempts.load(Ordering::SeqCst), mutations);
    }
}

#[tokio::test]
async fn a_live_leaf_prevents_ancestor_deletion_during_table_materialization() {
    let test = nested().await;
    let loader = test.loader();
    test.blocks.pause_reads.store(true, Ordering::SeqCst);
    let read = loader.load(
        test.fixture.context,
        &test.parent.identifier,
        &test.head.name,
        SnapshotLoadingMode::All,
        None,
    );
    tokio::pin!(read);
    tokio::select! {
        result = &mut read => panic!("unexpected completion {result:?}"),
        () = test.blocks.read_entered.notified() => {}
    }
    let outcome = NamespaceDropper::new(test.fixture.store.clone())
        .drop_namespace(&NamespaceDropRequest {
            context: test.fixture.context,
            identity: RequestIdentity {
                operation: OperationId::random(),
                issued_ms: 100,
            },
            principal: "writer".into(),
            identifier: NamespaceIdentifier::new(vec!["root".into()]).unwrap(),
        })
        .await
        .unwrap()
        .unwrap();
    assert_eq!(outcome.status, 409);
    test.blocks.pause_reads.store(false, Ordering::SeqCst);
    test.blocks.read_release.notify_one();
    assert!(matches!(read.await.unwrap(), TableLoad::Loaded { metadata, .. } if metadata == test.bytes));
}

#[tokio::test]
async fn leaf_tombstone_replacement_and_catalog_clear_invalidate_an_admitted_read() {
    for change in 0..3 {
        let test = nested().await;
        let loader = test.loader();
        test.blocks.pause_reads.store(true, Ordering::SeqCst);
        let read = loader.load(
            test.fixture.context,
            &test.parent.identifier,
            &test.head.name,
            SnapshotLoadingMode::Refs,
            Some("*"),
        );
        tokio::pin!(read);
        tokio::select! {
            result = &mut read => panic!("unexpected completion {result:?}"),
            () = test.blocks.read_entered.notified() => {}
        }
        if change == 0 {
            let mut leaf = test.parent.clone();
            leaf.lifecycle = NamespaceLifecycle::Tombstone;
            leaf.pending_operation = Some(OperationId::random());
            leaf.admission_fence += 1;
            leaf.mutation_revision += 1;
            test.fixture
                .put(
                    authority_key(leaf.catalog, leaf.namespace),
                    StorageRecord::NamespaceAuthority(Box::new(leaf)),
                )
                .await;
        } else if change == 1 {
            let replacement = test
                .fixture
                .authority(test.parent.parent, &["root", "branch", "analytics"]);
            test.fixture.publish(&replacement).await;
        } else {
            test.fixture
                .root(
                    test.fixture.context.replacement(CatalogId::random()).unwrap(),
                    RootState::Ready,
                )
                .await;
        }
        test.blocks.pause_reads.store(false, Ordering::SeqCst);
        test.blocks.read_release.notify_one();
        assert!(
            matches!(read.await, Err(TableLoadError::Catalog(CatalogError::Conflict))),
            "change {change}"
        );
    }
}

#[tokio::test]
async fn dropping_leaf_keeps_the_existing_read_visibility_contract() {
    let test = nested().await;
    let loader = test.loader();
    test.blocks.pause_reads.store(true, Ordering::SeqCst);
    let read = loader.load(
        test.fixture.context,
        &test.parent.identifier,
        &test.head.name,
        SnapshotLoadingMode::All,
        None,
    );
    tokio::pin!(read);
    tokio::select! {
        result = &mut read => panic!("unexpected completion {result:?}"),
        () = test.blocks.read_entered.notified() => {}
    }
    let mut leaf = test.parent.clone();
    leaf.lifecycle = NamespaceLifecycle::Dropping;
    leaf.pending_operation = Some(OperationId::random());
    leaf.admission_fence += 1;
    leaf.mutation_revision += 1;
    test.fixture
        .put(
            authority_key(leaf.catalog, leaf.namespace),
            StorageRecord::NamespaceAuthority(Box::new(leaf)),
        )
        .await;
    test.blocks.pause_reads.store(false, Ordering::SeqCst);
    test.blocks.read_release.notify_one();
    assert!(matches!(read.await.unwrap(), TableLoad::Loaded { metadata, .. } if metadata == test.bytes));
}
