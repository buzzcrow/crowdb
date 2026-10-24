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
mod staging;

use crowdb_access_iceberg::{
    commit::CommitPublicationError,
    key::{CatalogScope, IcebergKey, StorageKey},
    namespace::NamespaceDropper,
    table::TableRepository,
};
use serde_json::{json, Value};
use staging::TestStaged;
use std::sync::atomic::Ordering;

fn assert_no_process(test: &TestStaged) {
    for key in test.namespace.store.values.load().keys() {
        if let StorageKey::Legacy(IcebergKey::Catalog { scope, .. }) = StorageKey::decode(key).unwrap() {
            assert!(!matches!(
                scope,
                CatalogScope::TableCreateOperation | CatalogScope::OperationPayload
            ));
        }
    }
}

#[tokio::test]
async fn stage_returns_complete_client_material_without_any_server_write() {
    let test = TestStaged::new().await;
    let before = test.namespace.store.writes.load(Ordering::SeqCst);
    let reads = test.namespace.store.reads.load(Ordering::SeqCst);
    let first = test.stage().await;
    assert_eq!(test.namespace.store.reads.load(Ordering::SeqCst) - reads, 9);
    let first = TestStaged::response(&first);
    let second = TestStaged::response(&test.stage().await);
    assert!(first.get("metadata-location").is_none());
    assert_eq!(first["metadata"]["schemas"][0]["fields"][0]["id"], 1);
    assert_ne!(first["metadata"]["location"], second["metadata"]["location"]);
    assert_ne!(first["metadata"]["table-uuid"], second["metadata"]["table-uuid"]);
    assert_eq!(test.namespace.store.writes.load(Ordering::SeqCst), before);
    assert!(TableRepository::new(test.namespace.store.clone())
        .select(test.namespace.context, test.parent.namespace, "events")
        .await
        .unwrap()
        .is_none());
    assert_no_process(&test);
}

#[tokio::test]
async fn complete_initial_updates_publish_on_a_fresh_creator_for_each_format() {
    for version in [1, 2, 3] {
        let mut test = TestStaged::new().await;
        let mut body: Value = serde_json::from_slice(&test.request.body).unwrap();
        body["properties"] = json!({"format-version": version.to_string()});
        test.request.body = serde_json::to_vec(&body).unwrap();
        let staged = test.stage().await;
        let metadata = TestStaged::response(&staged);
        let request = test.commit_from(&staged);
        let reads = test.namespace.store.reads.load(Ordering::SeqCst);
        let writes = test.namespace.store.writes.load(Ordering::SeqCst);
        let result = test.creator().commit_staged(&request).await.unwrap();
        assert_eq!(test.namespace.store.reads.load(Ordering::SeqCst) - reads, 8);
        assert_eq!(test.namespace.store.writes.load(Ordering::SeqCst) - writes, 4);
        let output = TestStaged::response(&result);
        assert_eq!(result.status, 200);
        assert_eq!(
            output["metadata"]["table-uuid"],
            metadata["metadata"]["table-uuid"]
        );
        assert_eq!(output["metadata"]["schemas"], metadata["metadata"]["schemas"]);
        assert_eq!(output["metadata"]["format-version"], version);
        assert_eq!(output["metadata"]["metadata-log"], json!([]));
        assert_eq!(output["metadata"]["properties"]["transaction"], "committed");
        let selected = TableRepository::new(test.namespace.store.clone())
            .select(test.namespace.context, test.parent.namespace, "events")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(selected.head.pending_operation, None);
        assert_eq!(selected.head.generation, 1);
        assert!(matches!(
            test.creator().commit_staged(&request).await,
            Err(CommitPublicationError::TableExists)
        ));
        assert_no_process(&test);
    }
}

#[tokio::test]
async fn incomplete_client_material_is_rejected_before_resource_publication() {
    let test = TestStaged::new().await;
    let staged = test.stage().await;
    let request = test.commit_from(&staged);
    let input: Value = serde_json::from_slice(&request.body).unwrap();
    let before = test.namespace.store.writes.load(Ordering::SeqCst);
    for missing in [0, 2, 3, 4, 5, 6, 7, 8] {
        let mut body = input.clone();
        body["updates"].as_array_mut().unwrap().remove(missing);
        let mut changed = request.clone();
        changed.body = serde_json::to_vec(&body).unwrap();
        assert!(
            test.creator().commit_staged(&changed).await.is_err(),
            "missing {missing}"
        );
        assert_eq!(test.namespace.store.writes.load(Ordering::SeqCst), before);
    }
    assert_no_process(&test);
}

#[tokio::test]
async fn stage_does_not_register_an_owner_or_expiring_server_session() {
    let test = TestStaged::new().await;
    let mut request = test.commit_request().await;
    request.principal = "another independently authorized writer".into();
    request.timestamp_ms += 2 * 24 * 60 * 60 * 1000;
    assert_eq!(test.creator().commit_staged(&request).await.unwrap().status, 200);
    assert_no_process(&test);
}

#[tokio::test]
async fn namespace_deletion_before_final_request_does_not_leave_a_recoverable_publisher() {
    let test = TestStaged::new().await;
    let request = test.commit_request().await;
    assert_eq!(
        NamespaceDropper::new(test.namespace.store.clone())
            .drop_namespace(&test.drop_request())
            .await
            .unwrap()
            .unwrap()
            .status,
        204
    );
    assert!(matches!(
        test.creator().commit_staged(&request).await,
        Err(CommitPublicationError::NamespaceMissing)
    ));
    assert!(TableRepository::new(test.namespace.store.clone())
        .select(test.namespace.context, test.parent.namespace, "events")
        .await
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn final_input_cannot_select_a_foreign_catalog_or_namespace_storage_identity() {
    let test = TestStaged::new().await;
    let request = test.commit_request().await;
    for foreign in ["s3://external/table/", "s3://iceberg/foreign/"] {
        let mut changed = request.clone();
        let mut body: Value = serde_json::from_slice(&changed.body).unwrap();
        body["updates"][8]["location"] = json!(foreign);
        changed.body = serde_json::to_vec(&body).unwrap();
        assert!(test.creator().commit_staged(&changed).await.is_err());
    }
    assert_no_process(&test);
}
