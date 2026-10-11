// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::setup::{cleanup_client, path, setup};
use crowdb_access_iceberg::file::{FileLocation, FileRepository};
use reqwest::Method;
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "requires the owned release Iceberg FileIO stack"]
async fn concurrent_multipart_creates_use_independent_sessions() {
    let (_stack, _process, client, table) = setup().await;
    let mut requests = tokio::task::JoinSet::new();
    for index in 0..24 {
        let object = path(table, &format!("data/concurrent-{index}.parquet"));
        let request = client.request(Method::POST, &object, "uploads=", b"", false, None);
        requests.spawn(async move { request.send().await.unwrap() });
    }
    while let Some(result) = requests.join_next().await {
        let response = result.unwrap();
        assert_eq!(response.status(), 200, "{}", response.text().await.unwrap());
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires the owned release Iceberg FileIO stack"]
async fn explicit_cleanup_supports_single_batch_and_same_path_reupload() {
    let (stack, _process, client, table) = setup().await;
    let object = path(table, "data/cleanup.parquet");
    let initial = client.send(Method::PUT, &object, "", b"first", false).await;
    assert_eq!(initial.status(), 200, "{}", initial.text().await.unwrap());
    let ordinary = client.send(Method::DELETE, &object, "", b"", false).await;
    assert_eq!(ordinary.status(), 403);

    let cleanup = cleanup_client(&stack, &client, table).await;
    let deleted = cleanup.send(Method::DELETE, &object, "", b"", false).await;
    assert_eq!(deleted.status(), 204, "{}", deleted.text().await.unwrap());
    let missing = client.send(Method::GET, &object, "", b"", false).await;
    assert_eq!(missing.status(), 404);
    let recreated = client.send(Method::PUT, &object, "", b"second", false).await;
    assert_eq!(recreated.status(), 200, "{}", recreated.text().await.unwrap());

    let location = table.file("data/cleanup.parquet").unwrap();
    let missing_key = table.file("data/unknown.parquet").unwrap();
    let xml = format!(
        "<Delete><Object><Key>{}</Key></Object><Object><Key>{}</Key></Object></Delete>",
        location.object_key(),
        missing_key.object_key()
    );
    let response = cleanup
        .send(
            Method::POST,
            &format!("/{}", table.bucket()),
            "delete=",
            xml.as_bytes(),
            true,
        )
        .await;
    let status = response.status();
    let body = response.text().await.unwrap();
    assert_eq!(status, 200, "{body}");
    assert!(body.contains("<Deleted>"), "{body}");
    assert!(!body.contains("<Error>"), "{body}");
    assert_eq!(
        client.send(Method::GET, &object, "", b"", false).await.status(),
        404
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires the owned release Iceberg FileIO stack"]
async fn explicit_cleanup_rejects_a_retained_metadata_file() {
    let (stack, _process, client, _) = setup().await;
    let endpoint = format!("http://{}", client.address);
    let created = client
        .client
        .post(format!("{endpoint}/v1/namespaces/analytics/tables"))
        .bearer_auth("w".repeat(32))
        .json(&serde_json::json!({"name":"protected", "schema":{"type":"struct","fields":[]}}))
        .send()
        .await
        .unwrap();
    assert_eq!(created.status(), 200, "{}", created.text().await.unwrap());
    let created: serde_json::Value = created.json().await.unwrap();
    let location: FileLocation = created["metadata-location"].as_str().unwrap().parse().unwrap();
    let table = location.table();
    let cleanup = cleanup_client(&stack, &client, table).await;
    let object = format!("/{}/{}", table.bucket(), location.object_key());
    let single = cleanup.send(Method::DELETE, &object, "", b"", false).await;
    assert_eq!(single.status(), 409, "{}", single.text().await.unwrap());
    let xml = format!(
        "<Delete><Object><Key>{}</Key></Object></Delete>",
        location.object_key()
    );
    let batch = cleanup
        .send(
            Method::POST,
            &format!("/{}", table.bucket()),
            "delete=",
            xml.as_bytes(),
            true,
        )
        .await;
    let status = batch.status();
    let body = batch.text().await.unwrap();
    assert_eq!(status, 200, "{body}");
    assert!(body.contains("<Error>"), "{body}");
    assert!(FileRepository::new(stack.store().await)
        .load(cleanup.credentials.grant().context, &location)
        .await
        .unwrap()
        .is_some());
}
