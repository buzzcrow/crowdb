#[path = "common/file_blocks.rs"]
mod blocks;
#[path = "common/store.rs"]
mod common;
#[path = "common/table_creation.rs"]
mod creation;
#[path = "common/namespace.rs"]
mod fixture;
#[path = "common/namespace_store.rs"]
mod namespace_store;

use std::sync::atomic::Ordering;

use creation::TestCreation;
use crowdb_access_iceberg::{
    catalog::{CatalogError, CatalogStore, RootState},
    commit::{CommitPublicationError, TableCreateJournal, TableCreatePhase},
    key::{CatalogId, OperationId},
    namespace::{NamespaceDropper, NamespaceRepository},
    operation::PayloadStore,
    record::StorageRecord,
    table::{read_table_metadata_document, TableMetadataLimits, TableRepository},
};

#[tokio::test]
async fn response_reserve_rejects_before_durable_creation() {
    let test = TestCreation::new().await;
    let before = test.fixture.store.writes.load(Ordering::SeqCst);
    let creator = test
        .creator()
        .with_response_reserve(crowdb_access_iceberg::operation::MAX_PAYLOAD_BYTES);
    assert!(matches!(
        creator.create(&test.request).await,
        Err(CommitPublicationError::Metadata(
            crowdb_access_iceberg::table::TableMetadataError::Bounds
        ))
    ));
    assert_eq!(test.fixture.store.writes.load(Ordering::SeqCst), before);
    assert_eq!(test.creator().create(&test.request).await.unwrap().status, 200);
}

#[tokio::test]
async fn immediate_create_publishes_one_table_and_replays_exact_result() {
    let test = TestCreation::new().await;
    let creator = test.creator();
    let result = creator.create(&test.request).await.unwrap();
    assert_eq!(result.status, 200);
    let selected = TableRepository::new(test.fixture.store.clone())
        .select(test.fixture.context, test.parent.namespace, "events")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(selected.head.generation, 1);
    assert_eq!(selected.head.pending_operation, None);
    let document = read_table_metadata_document(
        test.blocks.clone(),
        &selected,
        TableMetadataLimits {
            bytes: 1024 * 1024,
            values: 50_000,
            depth: 32,
            string_bytes: 512 * 1024,
            collection_entries: 1000,
        },
    )
    .await
    .unwrap();
    let response: serde_json::Value = serde_json::from_slice(
        &PayloadStore::new(test.fixture.store.clone())
            .get(&result.body)
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(
        response["metadata"],
        serde_json::Value::Object(document.fields().clone())
    );
    assert_eq!(
        response["metadata-location"],
        selected.head.metadata_location.to_string()
    );
    assert_eq!(creator.create(&test.request).await.unwrap(), result);
    let parent = NamespaceRepository::new(test.fixture.store.clone())
        .load(test.fixture.context, &test.parent.identifier)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(parent.pending_operation, None);
    assert_eq!(parent.admission_fence, test.parent.admission_fence);
    assert_eq!(parent.property_revision, test.parent.property_revision);
    let drop = NamespaceDropper::new(test.fixture.store.clone())
        .drop_namespace(&test.drop_request())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(drop.status, 409);
}

#[tokio::test]
async fn duplicate_name_and_changed_retry_have_durable_conflict_without_replacing_head() {
    let test = TestCreation::new().await;
    let creator = test.creator();
    let first = creator.create(&test.request).await.unwrap();
    let mut duplicate = test.request.clone();
    duplicate.identity.operation = OperationId::random();
    let result = creator.create(&duplicate).await.unwrap();
    assert_eq!(result.status, 409);
    assert_eq!(creator.create(&duplicate).await.unwrap(), result);
    let mut altered = test.request.clone();
    altered.body.push(b' ');
    assert!(matches!(
        creator.create(&altered).await,
        Err(CommitPublicationError::Catalog(CatalogError::Conflict))
    ));
    assert_eq!(creator.create(&test.request).await.unwrap(), first);
}

#[tokio::test]
async fn every_creation_write_reply_loss_recovers_original_identity_and_result() {
    let baseline = TestCreation::new().await;
    let before = baseline.fixture.store.writes.load(Ordering::SeqCst);
    baseline.creator().create(&baseline.request).await.unwrap();
    let writes = baseline.fixture.store.writes.load(Ordering::SeqCst) - before;
    assert!(writes >= 18);
    for offset in 1..=writes {
        let test = TestCreation::new().await;
        let store = test.fixture.store.clone();
        store
            .fail_after
            .store(store.writes.load(Ordering::SeqCst) + offset, Ordering::SeqCst);
        assert!(
            test.creator().create(&test.request).await.is_err(),
            "offset {offset}"
        );
        store.fail_after.store(0, Ordering::SeqCst);
        let journal = TableCreateJournal::new(store.clone());
        let interrupted = journal
            .load(test.fixture.context, test.request.identity.operation)
            .await
            .unwrap();
        let result = test.creator().create(&test.request).await.unwrap();
        assert_eq!(result.status, 200, "offset {offset}");
        let completed = journal
            .load(test.fixture.context, test.request.identity.operation)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(completed.phase, TableCreatePhase::Complete);
        if let Some(interrupted) = interrupted {
            assert_eq!(interrupted.candidate, completed.candidate);
            assert_eq!(interrupted.document, completed.document);
        }
        let selected = TableRepository::new(store.clone())
            .select(test.fixture.context, test.parent.namespace, "events")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(selected.head.generation, 1);
        assert_eq!(selected.head.pending_operation, None);
        let key = completed.key();
        let bytes = store.get(&key.encode().unwrap()).await.unwrap().unwrap().bytes;
        assert_eq!(
            StorageRecord::decode(&key, &bytes).unwrap(),
            StorageRecord::TableCreateOperation(Box::new(completed))
        );
        assert_eq!(test.creator().create(&test.request).await.unwrap(), result);
    }
}

#[tokio::test]
async fn chunk_write_failure_never_admits_parent_or_exposes_partial_table() {
    let mut test = TestCreation::new().await;
    let mut body: serde_json::Value = serde_json::from_slice(&test.request.body).unwrap();
    body["properties"] = serde_json::json!({"large":"value".repeat(18_000)});
    test.request.body = serde_json::to_vec(&body).unwrap();
    test.blocks.fail_after.store(1, Ordering::SeqCst);
    assert!(test.creator().create(&test.request).await.is_err());
    assert!(TableRepository::new(test.fixture.store.clone())
        .select(test.fixture.context, test.parent.namespace, "events")
        .await
        .unwrap()
        .is_none());
    let parent = NamespaceRepository::new(test.fixture.store.clone())
        .load(test.fixture.context, &test.parent.identifier)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(parent, test.parent);
    test.blocks.fail_after.store(0, Ordering::SeqCst);
    assert_eq!(test.creator().create(&test.request).await.unwrap().status, 200);
}

#[tokio::test]
async fn retired_catalog_cannot_replay_created_table() {
    let test = TestCreation::new().await;
    test.creator().create(&test.request).await.unwrap();
    let mut replacement = test.fixture.context;
    replacement.catalog = CatalogId::random();
    replacement.activation_epoch += 1;
    test.fixture.root(replacement, RootState::Ready).await;
    assert!(test.creator().create(&test.request).await.is_err());
}

#[tokio::test]
async fn invalid_creation_has_no_durable_side_effects() {
    let test = TestCreation::new().await;
    let before = test.fixture.store.writes.load(Ordering::SeqCst);
    for body in [
        serde_json::json!({"name":"events","schema":{"type":"struct","fields":[{"id":0,"name":"bad","required":true,"type":"long"}]}}),
        serde_json::json!({"name":"events","schema":{"type":"struct","fields":[]},"location":"s3://external/table"}),
        serde_json::json!({"name":"events","schema":{"type":"struct","fields":[]},"stage-create":true}),
    ] {
        let mut request = test.request.clone();
        request.body = serde_json::to_vec(&body).unwrap();
        assert!(test.creator().create(&request).await.is_err());
        assert_eq!(test.fixture.store.writes.load(Ordering::SeqCst), before);
    }
    let mut request = test.request.clone();
    request.namespace =
        crowdb_access_iceberg::namespace::NamespaceIdentifier::new(vec!["absent".into()]).unwrap();
    assert!(matches!(
        test.creator().create(&request).await,
        Err(CommitPublicationError::NamespaceMissing)
    ));
    assert_eq!(test.fixture.store.writes.load(Ordering::SeqCst), before);
}
