// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crate::common::native_s3::{context, payload, TestNativeS3};
use crowdb_access_s3::S3ErrorCode;
use crowdb_sdk_native::s3::{GetOptions, ReadPreconditions};
use crowdb_sdk_native::Error;
use reqwest::{Client, StatusCode};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires owned protected storage and trusted HTTP Access frontend"]
async fn http_and_native_share_bytes_metadata_conditions_copy_and_multipart() {
    let addresses = crate::common::management::addresses(13);
    let fixture = TestNativeS3::connect(&addresses, b"local").await;
    fixture
        .client
        .create_bucket(context(), fixture.bucket)
        .await
        .unwrap();
    let http = Client::builder().no_proxy().build().unwrap();
    let base = format!("http://{}:7500/native-corpus", addresses[12]);
    for length in [0, 513, 65_503, (2 << 20) + 17] {
        let key = format!("native-{length}");
        let expected = payload(length);
        let metadata = fixture.put(key.as_bytes(), expected.clone()).await;
        let response = http.get(format!("{base}/{key}")).send().await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers()["etag"].to_str().unwrap().trim_matches('"'),
            metadata.etag
        );
        assert_eq!(response.bytes().await.unwrap().as_ref(), expected);
        let key = format!("http-{length}");
        let response = http
            .put(format!("{base}/{key}"))
            .header("content-type", "application/parity")
            .header("x-amz-meta-source", "http")
            .body(expected.clone())
            .send()
            .await
            .unwrap();
        assert_eq!(
            response.status(),
            StatusCode::OK,
            "{}",
            response.text().await.unwrap()
        );
        let (metadata, received) = fixture.read(key.as_bytes(), GetOptions::default()).await;
        assert_eq!(received, expected);
        assert_eq!(metadata.content_type, "application/parity");
        assert_eq!(
            metadata.user_metadata.values().get("source").map(String::as_str),
            Some("http")
        );
    }
    assert_error_parity(&fixture, &http, &base, &addresses).await;
    // The same durable multipart object is subsequently read through HTTP.
    fixture.put_framed(b"framed-small", 513).await;
    Box::pin(crate::common::native_s3::multipart::exercise(&fixture)).await;
    let response = http.get(format!("{base}/multipart")).send().await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let mut expected = payload(5 << 20);
    expected.extend_from_slice(&payload(513));
    assert_eq!(response.bytes().await.unwrap().as_ref(), expected);
    fixture.runtime.shutdown().await.unwrap();
}

async fn assert_error_parity(fixture: &TestNativeS3, http: &Client, base: &str, addresses: &[String]) {
    let response = http
        .get(format!("{base}/native-513"))
        .header("if-match", "\"wrong\"")
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::PRECONDITION_FAILED);
    let response = http
        .get(format!("{base}/native-513"))
        .header("range", "bytes=999-1000")
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::RANGE_NOT_SATISFIABLE);
    assert!(matches!(
        fixture
            .client
            .head_object(
                context(),
                fixture.bucket,
                b"missing",
                &ReadPreconditions::default()
            )
            .await,
        Err(Error::S3(S3ErrorCode::NoSuchKey))
    ));
    let response = http.get(format!("{base}/missing")).send().await.unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    let response = crate::common::native_s3::signed_copy::copy(addresses, http, base).await;
    assert_eq!(
        response.status(),
        StatusCode::OK,
        "{}",
        response.text().await.unwrap()
    );
    let result = response.text().await.unwrap();
    assert!(
        result.contains("<CopyObjectResult ") && result.ends_with("</CopyObjectResult>"),
        "COPY did not publish: {result}"
    );
    assert_eq!(
        fixture.read(b"http-copy", GetOptions::default()).await.1,
        payload(513)
    );
}
