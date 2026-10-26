#[path = "common/file_blocks.rs"]
mod blocks;
#[path = "common/store.rs"]
mod common;
#[path = "common/manifest_entry.rs"]
#[allow(dead_code)]
mod fixture;
#[path = "common/manifest_list.rs"]
#[allow(dead_code)]
mod list_fixture;
#[path = "common/table_metadata.rs"]
#[allow(dead_code)]
mod metadata;
#[path = "common/namespace_store.rs"]
mod namespace_store;
#[path = "common/namespace.rs"]
#[allow(dead_code)]
mod namespaces;
#[path = "common/parquet_metadata.rs"]
#[allow(dead_code)]
mod parquet;
#[path = "common/snapshot_files.rs"]
#[allow(dead_code)]
mod snapshot;
#[path = "common/table_staging.rs"]
#[allow(dead_code)]
mod staging;

use common::TestStore;
use crowdb_access_iceberg::{
    catalog::CatalogError,
    commit::CommitPublicationError,
    key::OperationId,
    namespace::NamespaceDropper,
    table::{storage_head_key, TableRepository},
};
use staging::TestStaged;
use std::sync::atomic::Ordering;
use std::sync::Arc;

#[tokio::test]
async fn identical_complete_inputs_share_only_the_resource_reservation() {
    let store = Arc::new(TestStore {
        table_reservation_barrier: Some(Arc::new(tokio::sync::Barrier::new(2))),
        ..TestStore::default()
    });
    let test = TestStaged::with_store(store).await;
    let request = test.commit_request().await;
    let mut retry = request.clone();
    retry.identity.operation = OperationId::random();
    retry.timestamp_ms += 1;
    let creator = test.creator();
    let (commit, retry) = tokio::join!(creator.commit_staged(&request), creator.commit_staged(&retry));
    assert_eq!(usize::from(commit.is_ok()) + usize::from(retry.is_ok()), 1);
    let loser = if commit.is_err() { commit } else { retry };
    assert!(matches!(loser, Err(CommitPublicationError::TableExists)));
    let selected = TableRepository::new(test.namespace.store.clone())
        .select(test.namespace.context, test.parent.namespace, "events")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(selected.head.generation, 1);
    assert_eq!(selected.head.pending_operation, None);
}

#[tokio::test]
async fn namespace_drop_at_every_interrupted_final_commit_boundary_recovers() {
    let baseline = TestStaged::new().await;
    let request = baseline.commit_request().await;
    let before = baseline.namespace.store.writes.load(Ordering::SeqCst);
    baseline.creator().commit_staged(&request).await.unwrap();
    let writes = baseline.namespace.store.writes.load(Ordering::SeqCst) - before;
    for offset in 1..=writes {
        let test = TestStaged::new().await;
        let request = test.commit_request().await;
        let store = &test.namespace.store;
        store
            .fail_after
            .store(store.writes.load(Ordering::SeqCst) + offset, Ordering::SeqCst);
        assert!(test.creator().commit_staged(&request).await.is_err());
        store.fail_after.store(0, Ordering::SeqCst);
        let dropper = NamespaceDropper::new(store.clone());
        let drop_result = dropper
            .drop_namespace(&test.drop_request())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            drop_result.status,
            if offset < 3 { 204 } else { 409 },
            "offset {offset}"
        );
        let committed = test.creator().commit_staged(&request).await;
        if offset < 3 {
            assert!(matches!(committed, Err(CommitPublicationError::NamespaceMissing)));
        } else {
            assert!(matches!(committed, Err(CommitPublicationError::TableExists)));
        }
        let selected = TableRepository::new(store.clone())
            .select(test.namespace.context, test.parent.namespace, "events")
            .await
            .unwrap();
        assert_eq!(selected.is_some(), offset >= 3, "offset {offset}");
    }
}

#[tokio::test]
async fn different_complete_material_cannot_adopt_an_interrupted_reservation() {
    let test = TestStaged::new().await;
    let first = test.commit_request().await;
    let mut second = first.clone();
    second.body.push(b' ');
    let store = &test.namespace.store;
    store
        .fail_after
        .store(store.writes.load(Ordering::SeqCst) + 2, Ordering::SeqCst);
    assert!(test.creator().commit_staged(&first).await.is_err());
    store.fail_after.store(0, Ordering::SeqCst);
    assert!(matches!(
        test.creator().commit_staged(&second).await,
        Err(CommitPublicationError::Catalog(CatalogError::Busy))
    ));
    assert!(matches!(
        test.creator().commit_staged(&first).await,
        Err(CommitPublicationError::Catalog(CatalogError::Conflict))
    ));
    assert!(TableRepository::new(store.clone())
        .select(test.namespace.context, test.parent.namespace, "events")
        .await
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn unknown_resource_replies_stop_then_complete_input_retries_use_resource_facts() {
    for boundary in 1..=4 {
        let test = TestStaged::new().await;
        let request = test.commit_request().await;
        let store = &test.namespace.store;
        store
            .fail_after
            .store(store.writes.load(Ordering::SeqCst) + boundary, Ordering::SeqCst);
        assert!(
            test.creator().commit_staged(&request).await.is_err(),
            "boundary {boundary}"
        );
        store.fail_after.store(0, Ordering::SeqCst);
        let mut retry = request.clone();
        retry.identity.operation = OperationId::random();
        retry.identity.issued_ms += 1;
        retry.timestamp_ms += 10;
        let result = test.creator().commit_staged(&retry).await;
        if boundary <= 2 {
            assert_eq!(result.unwrap().status, 200, "boundary {boundary}");
        } else {
            assert!(
                matches!(result, Err(CommitPublicationError::TableExists)),
                "boundary {boundary}: {result:?}"
            );
        }
        let selected = TableRepository::new(store.clone())
            .select(test.namespace.context, test.parent.namespace, "events")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(selected.head.generation, 1);
        assert_eq!(selected.head.pending_operation, None);
    }
}

#[tokio::test]
async fn namespace_drop_retires_the_table_id_before_a_delayed_first_head() {
    let test = TestStaged::new().await;
    let request = test.commit_request().await;
    let store = test.namespace.store.clone();
    store.table_head_pause_before.store(true, Ordering::SeqCst);
    let creator = test.creator();
    let create = tokio::spawn(async move { creator.commit_staged(&request).await });
    tokio::time::timeout(
        std::time::Duration::from_secs(3),
        store.table_head_entered.notified(),
    )
    .await
    .unwrap();
    assert_eq!(
        NamespaceDropper::new(store.clone())
            .drop_namespace(&test.drop_request())
            .await
            .unwrap()
            .unwrap()
            .status,
        204
    );
    let retired: Vec<_> = store
        .values
        .load()
        .iter()
        .filter_map(|(key, value)| {
            let key = crowdb_access_iceberg::key::StorageKey::decode(key).ok()?;
            match crowdb_access_iceberg::record::StorageRecord::decode(&key, &value.bytes).ok()? {
                crowdb_access_iceberg::record::StorageRecord::RetiredTable(table) => Some(table),
                _ => None,
            }
        })
        .collect();
    assert_eq!(retired.len(), 1);
    let bytes = store
        .values
        .load()
        .get(&storage_head_key(retired[0]).encode().unwrap())
        .unwrap()
        .bytes
        .clone();
    let reads = store.reads.load(Ordering::SeqCst);
    store.table_head_release.notify_one();
    assert!(matches!(
        create.await.unwrap(),
        Err(CommitPublicationError::Catalog(CatalogError::Conflict))
    ));
    assert_eq!(store.reads.load(Ordering::SeqCst), reads);
    assert_eq!(
        store
            .values
            .load()
            .get(&storage_head_key(retired[0]).encode().unwrap())
            .unwrap()
            .bytes,
        bytes
    );
}
