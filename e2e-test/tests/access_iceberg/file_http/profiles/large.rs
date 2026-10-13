// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{
    assert_one_file_chunk, catalog_counts, catalog_delta, fixture_config, path, setup, upload_route_counts,
    Instant, Method, TableLocation, TestFileClient, TestNativeStorage,
};

async fn direct(
    stack: &TestNativeStorage,
    client: &TestFileClient,
    table: TableLocation,
    bytes: &[u8],
) -> String {
    let object = path(table, "data/profile-put.bin");
    let before = catalog_counts(client).await;
    let route_before = upload_route_counts(client).await;
    let started = Instant::now();
    let put = client.send(Method::PUT, &object, "", bytes, true).await;
    let put_ms = started.elapsed().as_millis();
    assert_eq!(put.status(), 200, "{}", put.text().await.unwrap());
    println!(
        "iceberg 5MiB PUT: {put_ms}ms {}",
        catalog_delta(before, catalog_counts(client).await)
    );
    let route_after = upload_route_counts(client).await;
    assert_eq!(route_after.0 - route_before.0, 0, "5MiB PUT entered small-write");
    assert!(
        route_after.1 > route_before.1,
        "5MiB PUT did not write large strips"
    );
    println!(
        "iceberg 5MiB PUT route: small_completed={} large_strips={} writer_feeds={}",
        route_after.0 - route_before.0,
        route_after.1 - route_before.1,
        route_after.2 - route_before.2
    );
    assert_one_file_chunk(stack, table).await;

    object
}

async fn multipart(client: &TestFileClient, table: TableLocation, bytes: &[u8]) {
    let multipart = path(table, "data/profile-mpu.bin");
    let created = client
        .send(Method::POST, &multipart, "uploads=", b"", false)
        .await;
    assert_eq!(created.status(), 200);
    let created = created.text().await.unwrap();
    let upload = created
        .split_once("<UploadId>")
        .unwrap()
        .1
        .split_once("</UploadId>")
        .unwrap()
        .0;
    let query = format!("partNumber=1&uploadId={upload}");
    let before = catalog_counts(client).await;
    let route_before = upload_route_counts(client).await;
    let started = Instant::now();
    let part = client.send(Method::PUT, &multipart, &query, bytes, true).await;
    let part_ms = started.elapsed().as_millis();
    assert_eq!(part.status(), 200, "{}", part.text().await.unwrap());
    let etag = part.headers()["etag"].to_str().unwrap();
    println!(
        "iceberg 5MiB UploadPart: {part_ms}ms {}",
        catalog_delta(before, catalog_counts(client).await)
    );
    let route_after = upload_route_counts(client).await;
    assert_eq!(
        route_after.0 - route_before.0,
        0,
        "5MiB UploadPart entered small-write"
    );
    assert!(
        route_after.1 > route_before.1,
        "5MiB UploadPart did not write large strips"
    );
    println!(
        "iceberg 5MiB UploadPart route: small_completed={} large_strips={} writer_feeds={}",
        route_after.0 - route_before.0,
        route_after.1 - route_before.1,
        route_after.2 - route_before.2
    );
    let manifest = format!(
        "<CompleteMultipartUpload><Part><ETag>{etag}</ETag><PartNumber>1</PartNumber></Part></CompleteMultipartUpload>"
    );
    let complete = client
        .send(
            Method::POST,
            &multipart,
            &format!("uploadId={upload}"),
            manifest.as_bytes(),
            false,
        )
        .await;
    assert_eq!(complete.status(), 200);
    assert!(complete
        .text()
        .await
        .unwrap()
        .contains("</CompleteMultipartUploadResult>"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires the owned release storage fixture"]
async fn five_mib_profile() {
    let (stack, _process, client, table) = setup().await;
    let bytes = vec![0x5a; 5 * 1024 * 1024];
    assert!(bytes.len() >= fixture_config().iceberg_small_write().threshold_exclusive());
    let object = direct(&stack, &client, table, &bytes).await;
    multipart(&client, table, &bytes).await;
    let before = catalog_counts(&client).await;
    let started = Instant::now();
    let get = client.send(Method::GET, &object, "", b"", false).await;
    assert_eq!(get.status(), 200);
    assert_eq!(get.bytes().await.unwrap().as_ref(), bytes);
    let get_ms = started.elapsed().as_millis();
    println!(
        "iceberg 5MiB GET: {get_ms}ms {}",
        catalog_delta(before, catalog_counts(&client).await)
    );
}
