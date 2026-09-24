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
    commit::{CommitPublicationError, TableCreatePhase},
    table::TableRepository,
};
use staging::TestStaged;
use std::sync::atomic::Ordering;
use std::sync::Arc;

#[tokio::test]
async fn expiry_and_commit_binding_have_one_phase_cas_winner() {
    let store = Arc::new(TestStore {
        stage_transition_barrier: Some(Arc::new(tokio::sync::Barrier::new(2))),
        ..TestStore::default()
    });
    let test = TestStaged::with_store(store).await;
    let request = test.commit_request().await;
    let operation = test.operation().await;
    let creator = test.creator();
    let (commit, expiry) = tokio::join!(
        creator.commit_staged(&request),
        creator.expire_stage(test.namespace.context, operation.candidate.table, 2000)
    );
    let final_operation = test.operation().await;
    if final_operation.stage.unwrap().binding.is_some() {
        assert_eq!(commit.unwrap().status, 200);
        assert!(matches!(
            expiry,
            Err(CommitPublicationError::Catalog(CatalogError::Busy))
        ));
        assert!(!creator
            .expire_stage(test.namespace.context, operation.candidate.table, 9999)
            .await
            .unwrap());
    } else {
        assert!(expiry.unwrap());
        assert!(matches!(
            commit,
            Err(CommitPublicationError::Catalog(CatalogError::Busy))
        ));
        assert_eq!(final_operation.phase, TableCreatePhase::Aborted);
        assert!(creator.commit_staged(&request).await.is_err());
        assert!(TableRepository::new(test.namespace.store.clone())
            .select(test.namespace.context, test.parent.namespace, "events")
            .await
            .unwrap()
            .is_none());
    }
}

#[tokio::test]
async fn namespace_drop_at_every_interrupted_final_commit_boundary_preserves_admission() {
    use crowdb_access_iceberg::namespace::NamespaceDropper;
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
        let dropped = NamespaceDropper::new(store.clone())
            .drop_namespace(&test.drop_request())
            .await
            .unwrap()
            .unwrap();
        let committed = test.creator().commit_staged(&request).await.unwrap();
        assert!(
            matches!((dropped.status, committed.status), (204, 404) | (409, 200)),
            "offset {offset}"
        );
        let selected = TableRepository::new(store.clone())
            .select(test.namespace.context, test.parent.namespace, "events")
            .await
            .unwrap();
        assert_eq!(selected.is_some(), dropped.status == 409, "offset {offset}");
    }
}

#[tokio::test]
async fn competing_final_identities_cannot_rebind_the_same_draft() {
    let store = Arc::new(TestStore {
        stage_transition_barrier: Some(Arc::new(tokio::sync::Barrier::new(2))),
        ..TestStore::default()
    });
    let test = TestStaged::with_store(store).await;
    let first = test.commit_request().await;
    let mut second = first.clone();
    second.identity.operation = crowdb_access_iceberg::key::OperationId::random();
    let creator = test.creator();
    let (left, right) = tokio::join!(creator.commit_staged(&first), creator.commit_staged(&second));
    assert_eq!(usize::from(left.is_ok()) + usize::from(right.is_ok()), 1);
    let (winner, loser) = if left.is_ok() {
        (&first, &second)
    } else {
        (&second, &first)
    };
    assert_eq!(creator.commit_staged(winner).await.unwrap().status, 200);
    assert!(matches!(
        creator.commit_staged(loser).await,
        Err(CommitPublicationError::Catalog(CatalogError::Conflict))
    ));
    assert_eq!(
        test.operation().await.stage.unwrap().binding.unwrap().identity,
        winner.identity
    );
}
