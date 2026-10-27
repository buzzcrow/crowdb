// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#[path = "common/dataset.rs"]
mod dataset;
use crowdb_access_dataset::*;
use dataset::{TestDatasetHttp, TestStore};
use reqwest::{Client, StatusCode};
use serde_json::{json, Value};
use std::sync::Arc;

#[tokio::test]
async fn http_delete_hides_dataset_and_retries_preserve_receipt() {
    let service = TestDatasetHttp::start_payload(Arc::new(TestStore::default())).await;
    let client = Client::new();
    let url = format!(
        "{}/api/dataset/v2/namespaces/dataset-ns/datasets/empty",
        service.origin
    );
    let created: Value = client
        .put(&url)
        .bearer_auth("secret")
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    let id = created["id"].as_str().unwrap();
    // Rejected input must not leave a durable reservation that makes an empty Dataset nonempty.
    for (length, part_size, md5) in [
        ("0", 4, "1".repeat(32)),
        ("4", 0, "1".repeat(32)),
        ("4", 4, "bad".into()),
    ] {
        let invalid = client.post(format!("{url}/uploads")).bearer_auth("secret")
            .json(&json!({"id": OperationId::random().to_string(), "length":length, "part_size":part_size, "md5":md5}))
            .send().await.unwrap();
        assert_eq!(invalid.status(), StatusCode::BAD_REQUEST);
    }
    let response = client
        .delete(&url)
        .bearer_auth("secret")
        .json(&json!({"id":id}))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let mark: Value = response.json().await.unwrap();
    assert_eq!(mark["reclaim_after"], "2200");
    assert_eq!(
        client
            .get(&url)
            .bearer_auth("secret")
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::NOT_FOUND
    );
    let list: Value = client
        .get(url.rsplit_once('/').unwrap().0)
        .bearer_auth("secret")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(list["entries"], json!([]));
    let retry: Value = client
        .delete(&url)
        .bearer_auth("secret")
        .json(&json!({"id":id}))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(retry, mark);
    let status: Value = client
        .get(format!("{url}/deletion/{id}"))
        .bearer_auth("secret")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(status["deletion"], mark);
    drop(client);
    service.stop().await;
}

#[tokio::test]
async fn real_background_worker_obeys_one_second_grace() {
    let store = Arc::new(TestStore::default());
    let authority = Arc::new(DatasetAuthority::new(store).with_reclaim_grace_seconds(1));
    let name = DatasetIdentity::new(NamespacePath::root(), "short-grace").unwrap();
    let id = authority.create_dataset(name.clone()).await.unwrap();
    let cancel = ReadCancellation::new();
    let mark = authority.delete_dataset(&name, id, &cancel).await.unwrap();
    assert!(!mark.complete);
    let worker = crowdb_access_server::dataset::start_dataset_cleanup_for_tests(authority.clone(), None);
    tokio::time::timeout(std::time::Duration::from_secs(3), async {
        loop {
            if authority
                .dataset_deletion(&name, id)
                .await
                .unwrap()
                .unwrap()
                .complete
            {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    worker.abort();
    let _ = worker.await;
    authority.create_dataset(name).await.unwrap();
}
