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
    catalog::{CatalogError, RootState},
    commit::CommitPublicationError,
    key::{CatalogId, OperationId},
    namespace::{NamespaceDropper, NamespaceRepository},
    record::StorageRecord,
    table::{read_table_metadata_document, TableMetadataLimits, TableRepository},
};

#[tokio::test]
async fn native_catalog_creation_freezes_namespace_in_storage_identity() {
    use crowdb_access_iceberg::catalog::{CatalogAuthority, CatalogStorageLayout};
    use crowdb_access_iceberg::key::{CatalogScope, IcebergKey, StorageKey, TableStorageScope};
    let test = TestCreation::new().await;
    let authority = CatalogAuthority::new_native(test.fixture.context.catalog, "native".into()).unwrap();
    test.fixture
        .put(
            IcebergKey::Catalog {
                catalog: authority.catalog,
                scope: CatalogScope::Authority,
                suffix: Vec::new(),
            },
            StorageRecord::Authority(authority.clone()),
        )
        .await;
    let creator = test.creator();
    assert_eq!(creator.create(&test.request).await.unwrap().status, 200);
    let selected = TableRepository::new(test.fixture.store.clone())
        .select(test.fixture.context, test.parent.namespace, "events")
        .await
        .unwrap()
        .unwrap();
    let path = selected.head.storage_location().storage_path().unwrap();
    assert_eq!(path.storage_namespace, test.parent.namespace);
    assert_eq!(path.catalog, test.fixture.context.catalog);
    assert!(selected
        .head
        .metadata_location
        .to_string()
        .starts_with(&format!("s3://iceberg/{path}")));
    let keys = test.fixture.store.values.load_full();
    let native: Vec<_> = keys
        .keys()
        .filter_map(|key| match StorageKey::decode(key) {
            Ok(StorageKey::NativeTable(key)) => Some(key),
            _ => None,
        })
        .collect();
    assert_eq!(
        native
            .iter()
            .filter(|key| key.scope == TableStorageScope::Head)
            .count(),
        1
    );
    assert_eq!(
        native
            .iter()
            .filter(|key| key.scope == TableStorageScope::File)
            .count(),
        1
    );
    assert!(native.iter().all(|key| key.table == path));
    assert!(matches!(
        creator.create(&test.request).await,
        Err(CommitPublicationError::TableExists)
    ));

    assert_eq!(
        authority.renamed("renamed".into()).unwrap().storage_layout,
        CatalogStorageLayout::Native
    );
    let mut default = test.request.clone();
    default.namespace =
        crowdb_access_iceberg::namespace::NamespaceIdentifier::new(vec!["default".into()]).unwrap();
    default.identity.operation = OperationId::random();
    assert_eq!(creator.create(&default).await.unwrap().status, 200);
    assert!(matches!(
        creator.create(&default).await,
        Err(CommitPublicationError::TableExists)
    ));
    let namespace = NamespaceRepository::new(test.fixture.store.clone())
        .load(test.fixture.context, &default.namespace)
        .await
        .unwrap()
        .unwrap();
    let selected = TableRepository::new(test.fixture.store.clone())
        .select(test.fixture.context, namespace.namespace, "events")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        selected.head.storage_location().storage_namespace,
        Some(namespace.namespace)
    );
    assert_ne!(namespace.namespace, test.parent.namespace);
}

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
async fn immediate_create_publishes_one_table_with_only_final_resource_writes() {
    let test = TestCreation::new().await;
    let creator = test.creator();
    let writes = test.fixture.store.writes.load(Ordering::SeqCst);
    let reads = test.fixture.store.reads.load(Ordering::SeqCst);
    let result = creator.create(&test.request).await.unwrap();
    assert_eq!(result.status, 200);
    assert_eq!(test.fixture.store.reads.load(Ordering::SeqCst) - reads, 8);
    assert_eq!(test.fixture.store.writes.load(Ordering::SeqCst) - writes, 4);
    assert_no_creation_process(&test);
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
    let response: serde_json::Value = serde_json::from_slice(&result.body).unwrap();
    assert_eq!(
        response["metadata"],
        serde_json::Value::Object(document.fields().clone())
    );
    assert_eq!(
        response["metadata-location"],
        selected.head.metadata_location.to_string()
    );
    assert!(matches!(
        creator.create(&test.request).await,
        Err(CommitPublicationError::TableExists)
    ));
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
async fn duplicate_name_and_changed_input_do_not_replace_a_published_head() {
    let test = TestCreation::new().await;
    let creator = test.creator();
    creator.create(&test.request).await.unwrap();
    let tables = TableRepository::new(test.fixture.store.clone());
    let before = tables
        .select(test.fixture.context, test.parent.namespace, "events")
        .await
        .unwrap()
        .unwrap();
    for changed in 0..3 {
        let mut duplicate = test.request.clone();
        if changed == 0 {
            duplicate.identity.operation = OperationId::random();
        }
        if changed == 1 {
            duplicate.body.push(b' ');
        }
        if changed == 2 {
            duplicate.principal = "another writer".into();
        }
        assert!(matches!(
            creator.create(&duplicate).await,
            Err(CommitPublicationError::TableExists)
        ));
        assert_eq!(
            tables
                .select(test.fixture.context, test.parent.namespace, "events")
                .await
                .unwrap()
                .unwrap(),
            before
        );
    }
    assert_no_creation_process(&test);
}

fn assert_no_creation_process(test: &TestCreation) {
    use crowdb_access_iceberg::key::{CatalogScope, IcebergKey};
    for key in test.fixture.store.values.load().keys() {
        if let Ok(IcebergKey::Catalog { scope, .. }) = IcebergKey::decode(key) {
            assert!(!matches!(
                scope,
                CatalogScope::TableCreateOperation | CatalogScope::OperationPayload
            ));
        }
    }
}

#[tokio::test]
async fn lost_creation_replies_require_complete_client_retry_and_reconcile_final_resources() {
    for boundary in 1..=4 {
        let test = TestCreation::new().await;
        let store = test.fixture.store.clone();
        store
            .fail_after
            .store(store.writes.load(Ordering::SeqCst) + boundary, Ordering::SeqCst);
        assert!(
            test.creator().create(&test.request).await.is_err(),
            "boundary {boundary}"
        );
        store.fail_after.store(0, Ordering::SeqCst);
        // This loop is the client, submitting its complete input on a new request.
        for _ in 0..2 {
            match test.creator().create(&test.request).await {
                Ok(result) => {
                    assert_eq!(result.status, 200);
                    break;
                }
                Err(CommitPublicationError::TableExists) => break,
                Err(CommitPublicationError::Catalog(CatalogError::Busy)) => {}
                result => panic!("boundary {boundary}: {result:?}"),
            }
        }
        let selected = TableRepository::new(store.clone())
            .select(test.fixture.context, test.parent.namespace, "events")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(selected.head.generation, 1);
        assert_eq!(selected.head.pending_operation, None);
        assert_no_creation_process(&test);
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
        serde_json::json!({"name":"events","schema":{"type":"struct","fields":[{"id":-1,"name":"bad","required":true,"type":"long"}]}}),
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
