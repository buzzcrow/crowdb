// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::time::Duration;

use axum::{body::Body, extract::Request, http::StatusCode, response::Response, Router};
use crowdb_console_shared::ops::s3_bench::{self, MixWeights, S3BenchConfig, S3BenchWorkload};

#[test]
fn mixed_weights_are_case_insensitive_and_deterministic() {
    let lower = MixWeights::parse("w20r70rr5l5").expect("lowercase mix");
    let mixed = MixWeights::parse("W20R70RR5L5").expect("mixed-case mix");
    let first = (0..256).map(|value| lower.select_name(value)).collect::<Vec<_>>();
    let second = (0..256).map(|value| mixed.select_name(value)).collect::<Vec<_>>();
    assert_eq!(first, second);
    for expected in ["write", "read", "range-read", "list"] {
        assert!(first.contains(&expected), "missing {expected}");
    }
}

#[test]
fn mixed_weights_reject_ambiguous_or_invalid_terms() {
    for value in [
        "",
        "w0",
        "w1W2",
        "x1",
        "rr",
        "w18446744073709551616",
        "w18446744073709551615r1",
    ] {
        assert!(MixWeights::parse(value).is_err(), "accepted {value}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn attached_mix_exercises_every_s3_operation_without_local_lifecycle() {
    // Workload accounting is a component proof; real S3 flows have container owners.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let peer = tokio::spawn(async move {
        axum::serve(listener, Router::new().fallback(protocol_peer))
            .await
            .unwrap();
    });
    let result = s3_bench::run(
        S3BenchConfig {
            workload: S3BenchWorkload::Mix,
            object_size: 1024,
            dataset_objects: 8,
            concurrency: 4,
            operations: 200,
            duration: Duration::from_secs(10),
            warmup_operations: 4,
            seed: 7,
            max_dataset_bytes: 2 * 1024 * 1024 * 1024,
            list_limit: 4,
            mix: MixWeights::default(),
        },
        crowdb_console_shared::ops::s3::S3HttpClient::new(endpoint).unwrap(),
    )
    .await
    .expect("attached S3 benchmark");
    peer.abort();
    assert_eq!(result.total_operations, 200);
    assert_eq!(result.total_errors, 0);
    assert!(result.by_operation.write.is_some());
    assert!(result.by_operation.read.is_some());
    assert!(result.by_operation.range_read.is_some());
    assert!(result.by_operation.list.is_some());
    assert!(result.bucket.starts_with("crowdb-bench-"));
    assert!(!result.object_bytes_verified);
}

async fn protocol_peer(request: Request) -> Response {
    let method = request.method().clone();
    let object = request
        .uri()
        .path()
        .trim_start_matches('/')
        .split('/')
        .nth(1)
        .is_some();
    let range = request
        .headers()
        .get("range")
        .map(|value| value.to_str().unwrap().to_owned());
    if method == axum::http::Method::GET && !object {
        assert!(request.uri().query().unwrap().contains("list-type=2"));
        let body = "<ListBucketResult><Contents><Key>dataset-00000000000000000000</Key></Contents><IsTruncated>false</IsTruncated></ListBucketResult>";
        return Response::new(Body::from(body));
    }
    if method == axum::http::Method::GET {
        let length = if let Some(range) = range {
            assert_eq!(range, "bytes=256-767");
            512
        } else {
            1024
        };
        return Response::new(Body::from(vec![0; length]));
    }
    if method == axum::http::Method::PUT {
        let body = axum::body::to_bytes(request.into_body(), 1024).await.unwrap();
        assert_eq!(body.len(), if object { 1024 } else { 0 });
    } else {
        assert_eq!(method, axum::http::Method::DELETE);
    }
    Response::builder()
        .status(StatusCode::NO_CONTENT)
        .body(Body::empty())
        .unwrap()
}
