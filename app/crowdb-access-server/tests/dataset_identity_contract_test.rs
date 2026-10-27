// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#[path = "common/dataset.rs"]
mod dataset;
use crowdb_access_dataset::{
    DatasetDeletionSweep, DatasetIdentity, NamespacePath, OperationId, ReadCancellation,
};
use dataset::{TestDatasetHttp, TestStore};
use reqwest::Client;
use serde_json::json;
use std::sync::{atomic::Ordering, Arc};

#[tokio::test]
async fn dataset_identity_header_fences_stale_editors_and_validates_before_reservation() {
    let test = TestDatasetHttp::start_payload(Arc::new(TestStore::default())).await;
    let client = Client::new();
    let name = DatasetIdentity::new(NamespacePath::root(), "reused").unwrap();
    let old = test.authority.create_dataset(name.clone()).await.unwrap();
    let url = format!(
        "{}/api/dataset/v2/namespaces/dataset-ns/datasets/reused",
        test.origin
    );
    let head = format!("{url}/head");
    let response = client
        .get(&head)
        .bearer_auth("secret")
        .header("x-crowdb-dataset-id", old.to_string())
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200, "{}", response.text().await.unwrap());
    for value in ["0".repeat(32), "A".repeat(32), "bad".into()] {
        let response = client
            .get(&head)
            .bearer_auth("secret")
            .header("x-crowdb-dataset-id", value)
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), 400, "{}", response.text().await.unwrap());
    }
    let response = client
        .get(&head)
        .bearer_auth("secret")
        .header("x-crowdb-dataset-id", old.to_string())
        .header("x-crowdb-dataset-id", old.to_string())
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 400, "{}", response.text().await.unwrap());
    test.authority
        .delete_dataset(&name, old, &ReadCancellation::new())
        .await
        .unwrap();
    test.clock.store(2200, Ordering::Release);
    test.authority
        .reclaim_datasets_page(&mut DatasetDeletionSweep::default(), &ReadCancellation::new())
        .await
        .unwrap();
    let current = test.authority.create_dataset(name.clone()).await.unwrap();
    let response = client
        .post(format!("{url}/uploads"))
        .bearer_auth("secret")
        .header("x-crowdb-dataset-id", old.to_string())
        .json(
            &json!({"id":OperationId::random().to_string(),"length":"4","part_size":4,"md5":"1".repeat(32)}),
        )
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 409);
    assert_eq!(
        response.json::<serde_json::Value>().await.unwrap()["code"],
        "dataset_replaced"
    );
    for id in [None, Some(current.to_string())] {
        let mut request = client.get(&head).bearer_auth("secret");
        if let Some(id) = id {
            request = request.header("x-crowdb-dataset-id", id);
        }
        let response = request.send().await.unwrap();
        assert_eq!(response.status(), 200, "{}", response.text().await.unwrap());
    }
    // Rejected stale uploads must not make the replacement Dataset nonempty.
    test.authority
        .delete_dataset(&name, current, &ReadCancellation::new())
        .await
        .unwrap();
    drop(client);
    test.stop().await;
}
