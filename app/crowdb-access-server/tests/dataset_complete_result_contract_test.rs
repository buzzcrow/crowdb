// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[path = "common/dataset.rs"]
mod dataset;
#[path = "../../../lib/crowdb-access-dataset/tests/common/publication.rs"]
mod publication;

use crowdb_access_dataset::{
    DatasetAuthority, DatasetIdentity, DatasetSnapshotRequest, NamespacePath, OperationId,
};
use dataset::{TestDatasetHttp, TestStore};
use reqwest::Client;
use std::sync::Arc;

async fn manifests(
    client: &Client,
    origin: &str,
    snapshot: crowdb_access_dataset::SnapshotId,
    expected: u16,
) {
    let path = format!(
        "{origin}/api/dataset/v1/namespaces/dataset-ns/datasets/legacy/snapshots/{snapshot}/manifest"
    );
    let response = client.get(path).bearer_auth("secret").send().await.unwrap();
    let actual = response.status().as_u16();
    assert_eq!(actual, expected, "{}", response.text().await.unwrap());
    let response = client
        .post(format!("{origin}/v1/dataset/manifest"))
        .bearer_auth("secret")
        .body(bincode::serialize(&DatasetSnapshotRequest { snapshot }).unwrap())
        .send()
        .await
        .unwrap();
    let actual = response.status().as_u16();
    let body = response.bytes().await.unwrap();
    assert_eq!(actual, expected, "{body:?}");
}

#[tokio::test]
async fn json_and_binary_manifest_reads_admit_only_complete_live_resources() {
    let store = Arc::new(TestStore::default());
    let authority = DatasetAuthority::new(store.clone());
    let identity = DatasetIdentity::new(NamespacePath::root(), "legacy").unwrap();
    authority.create_dataset(identity.clone()).await.unwrap();
    let snapshot = authority
        .prepare_snapshot_with_operation(
            &identity,
            None,
            publication::publication_manifest(1),
            OperationId::random(),
        )
        .await
        .unwrap();
    let test = TestDatasetHttp::start(store).await;
    let client = Client::new();
    manifests(&client, &test.origin, snapshot, 404).await;
    authority
        .complete_snapshot_candidate(&identity, snapshot)
        .await
        .unwrap();
    assert_eq!(authority.latest(&identity).await.unwrap(), None);
    manifests(&client, &test.origin, snapshot, 200).await;
    authority.begin_reclaim(&identity, snapshot).await.unwrap();
    manifests(&client, &test.origin, snapshot, 409).await;
    drop(client);
    test.stop().await;
}
