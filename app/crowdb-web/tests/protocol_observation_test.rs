// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[path = "common/protocol_observation.rs"]
mod protocol_observation;

use crowdb_web::protocol_probe_for_tests;
use protocol_observation::TestListener;
use std::time::{Duration, Instant};

#[tokio::test]
async fn protocols_require_their_own_successful_readiness_body() {
    for (protocol, body, token) in [
        ("s3", r#"{"ready":true}"#, None),
        (
            "iceberg",
            r#"{"defaults":{},"overrides":{}}"#,
            Some("fixture-token"),
        ),
        (
            "dataset",
            r#"{"id":"existing","name":"crowdb_readiness_probe"}"#,
            Some("fixture-token"),
        ),
    ] {
        let listener = TestListener::serve(200, body.into(), Duration::ZERO, token.is_some()).await;
        assert_eq!(
            protocol_probe_for_tests(protocol, &listener.url, token).await,
            Some(true)
        );
    }
    let listener = TestListener::serve(200, r#"{"ready":true}"#.into(), Duration::ZERO, true).await;
    assert_eq!(
        protocol_probe_for_tests("iceberg", &listener.url, Some("fixture-token")).await,
        Some(false)
    );
}

#[tokio::test]
async fn authenticated_dataset_absence_proves_a_read_without_creating_data() {
    let listener = TestListener::serve(404, r#"{"code":"not_found"}"#.into(), Duration::ZERO, true).await;
    assert_eq!(
        protocol_probe_for_tests("dataset", &listener.url, Some("fixture-token")).await,
        Some(true)
    );
    let unauthorized = TestListener::serve(401, r#"{"code":"not_found"}"#.into(), Duration::ZERO, true).await;
    assert_eq!(
        protocol_probe_for_tests("dataset", &unauthorized.url, Some("fixture-token")).await,
        Some(false)
    );
}

#[tokio::test]
async fn malformed_oversized_and_redirected_replies_do_not_prove_readiness() {
    for (status, body, expected) in [
        (200, "not-json".into(), None),
        (200, " ".repeat(65537), None),
        (302, r#"{"ready":true}"#.into(), Some(false)),
        (503, r#"{"ready":true}"#.into(), Some(false)),
    ] {
        let listener = TestListener::serve(status, body, Duration::ZERO, false).await;
        assert_eq!(
            protocol_probe_for_tests("s3", &listener.url, None).await,
            expected
        );
    }
}

#[tokio::test]
async fn a_stalled_listener_has_a_bounded_observation() {
    let listener = TestListener::serve(200, r#"{"ready":true}"#.into(), Duration::from_secs(30), false).await;
    let started = Instant::now();
    assert_eq!(protocol_probe_for_tests("s3", &listener.url, None).await, None);
    assert!(started.elapsed() < Duration::from_secs(3));
}
