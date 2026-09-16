// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_access_s3::metrics::{OutcomeClass, RequestMeasurement, S3Metrics};
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
}
