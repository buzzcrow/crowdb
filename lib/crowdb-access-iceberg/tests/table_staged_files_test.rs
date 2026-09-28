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

use crowdb_access_iceberg::{
    catalog::CatalogStore,
    commit::TableCreatePhase,
    file::{file_key, FileRepository},
    namespace::NamespaceDropper,
    table::TableRepository,
};
use serde_json::json;
use staging::TestStaged;

#[tokio::test]
async fn staged_snapshot_files_are_checked_before_any_initial_metadata_publication() {
    for rows in [10, 11] {
        let test = TestStaged::new().await;
        let mut request = test.commit_request().await;
        let data = snapshot::data(test.blocks.clone(), "data/first.parquet").await;
        let input = snapshot::input(
            test.blocks.clone(),
            vec![vec![snapshot::entry(&data, 0, rows)]],
            vec![data.clone()],
        )
        .await;
        let (manifest, _) = input
            .manifests
            .resolve(&fixture::table().file("metadata/0.avro").unwrap())
            .await
            .unwrap();
        let files = FileRepository::new(test.namespace.store.clone());
        for file in [&data, &manifest, &input.list] {
            files.publish(test.namespace.context, file).await.unwrap();
        }
        let mut body: serde_json::Value = serde_json::from_slice(&request.body).unwrap();
        body["updates"][2]["schema"] = json!({"type":"struct","schema-id":0,"fields":[
            {"id":3,"name":"value","required":false,"type":"long"}
        ]});
        let mut snapshot = metadata::snapshot(99, 9);
        snapshot["manifest-list"] = json!(input.list.location.to_string());
        body["updates"].as_array_mut().unwrap().extend([
            json!({"action":"add-snapshot","snapshot":snapshot}),
            json!({"action":"set-snapshot-ref","ref-name":"main","snapshot-id":99,"type":"branch"}),
        ]);
        request.body = serde_json::to_vec(&body).unwrap();
        let outcome = test.creator().commit_staged(&request).await.unwrap();
        let operation = test.operation().await;
        let selected = TableRepository::new(test.namespace.store.clone())
            .select(test.namespace.context, test.parent.namespace, "events")
            .await
            .unwrap();
        if rows == 10 {
            assert_eq!(outcome.status, 200);
            assert!(selected.is_some());
            assert_eq!(
                test.response(&outcome).await["metadata"]["current-snapshot-id"],
                99
            );
            assert_eq!(operation.phase, TableCreatePhase::Complete);
        } else {
            assert_eq!(outcome.status, 400);
            assert!(selected.is_none());
            assert_eq!(operation.phase, TableCreatePhase::Aborted);
            assert!(test
                .namespace
                .store
                .get(
                    &file_key(operation.candidate.catalog, operation.candidate.metadata_file)
                        .encode()
                        .unwrap()
                )
                .await
                .unwrap()
                .is_none());
            assert_eq!(test.creator().commit_staged(&request).await.unwrap(), outcome);
            assert_eq!(
                NamespaceDropper::new(test.namespace.store.clone())
                    .drop_namespace(&test.drop_request())
                    .await
                    .unwrap()
                    .unwrap()
                    .status,
                204
            );
        }
    }
}
