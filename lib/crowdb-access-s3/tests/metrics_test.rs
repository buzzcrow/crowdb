// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_access_s3::metrics::{DependencyHealth, OutcomeClass, RequestMeasurement, S3Health, S3Metrics};
use crowdb_access_s3::route::S3Operation;

#[test]
fn request_metrics_reconcile_terminal_phase_and_concurrency_totals() {
    let metrics = S3Metrics::default();
    let first = metrics.begin_request();
    let second = metrics.begin_request();
    assert_eq!(metrics.snapshot().in_flight, 2);

    metrics.finish_request(
        S3Operation::GetObject,
        OutcomeClass::Success,
        RequestMeasurement {
            request_bytes: 11,
            response_bytes: 13,
            latency_ns: 17,
            authentication_latency_ns: 3,
            operation_latency_ns: 7,
        },
    );
    metrics.record_time_to_first_byte(S3Operation::GetObject, OutcomeClass::Success, 19);
    metrics.finish_predispatch(OutcomeClass::Unavailable, 23);
    metrics.record_metadata_retry();
    metrics.record_checksum_bytes(29);
    metrics.enqueue_cleanup(5);
    metrics.complete_cleanup(2);
    metrics.fail_cleanup(1);
    drop(first);
    drop(second);

    let snapshot = metrics.snapshot();
    let operation = S3Operation::GetObject as usize;
    let success = OutcomeClass::Success as usize;
    assert_eq!(snapshot.requests[operation][success], 1);
    assert_eq!(snapshot.request_latency_ns[operation][success], 17);
    assert_eq!(snapshot.authentication_latency_ns[operation][success], 3);
    assert_eq!(snapshot.operation_latency_ns[operation][success], 7);
    assert_eq!(snapshot.time_to_first_byte_ns[operation][success], 19);
    assert_eq!(snapshot.request_bytes, 11);
    assert_eq!(snapshot.response_bytes, 13);
    assert_eq!(
        snapshot.predispatch_requests[OutcomeClass::Unavailable as usize],
        1
    );
    assert_eq!(
        snapshot.predispatch_latency_ns[OutcomeClass::Unavailable as usize],
        23
    );
    assert_eq!(snapshot.in_flight, 0);
    assert_eq!(snapshot.max_in_flight, 2);
    assert_eq!(snapshot.checksum_bytes, 29);
    assert_eq!(snapshot.metadata_retries, 1);
    assert_eq!(snapshot.cleanup_enqueued, 5);
    assert_eq!(snapshot.cleanup_completed, 2);
    assert_eq!(snapshot.cleanup_failed, 1);
}

#[test]
fn readiness_tracks_safe_admission_without_exporter_state() {
    let metrics = S3Metrics::default();
    let health = S3Health::starting(2);
    let starting = health.snapshot(&metrics, None);
    assert!(starting.live);
    assert!(!starting.readiness.is_ready());

    health.set_listener(DependencyHealth::Ready);
    health.set_metadata(DependencyHealth::Ready);
    health.set_chunks(DependencyHealth::Busy);
    health.set_authentication(DependencyHealth::Ready);
    assert!(health.snapshot(&metrics, None).readiness.is_ready());

    metrics.enqueue_cleanup(3);
    let overloaded = health.snapshot(&metrics, None);
    assert_eq!(overloaded.readiness.cleanup, DependencyHealth::Unavailable);
    assert!(!overloaded.readiness.is_ready());
    metrics.complete_cleanup(1);
    assert!(health.snapshot(&metrics, None).readiness.is_ready());

    health.stop();
    assert!(!health.snapshot(&metrics, None).live);
}

#[test]
fn exported_request_series_have_fixed_cardinality_and_no_namespace_labels() {
    let metrics = S3Metrics::default();
    let rendered = metrics.render_prometheus(None, None);
    assert_eq!(
        rendered
            .lines()
            .filter(|line| line.starts_with("crowdb_s3_requests_total{"))
            .count(),
        9 * 6
    );
    assert!(!rendered.contains("bucket="));
    assert!(!rendered.contains("key="));
    assert!(!rendered.contains("credential="));
}
