// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_access_s3::{S3Error, S3ErrorCode};
use hyper::StatusCode;

#[test]
fn not_implemented_uses_the_standard_s3_response() {
    let error = S3Error::not_implemented("/bucket/object".into(), "request-1".into(), "host-1".into());

    assert_eq!(error.status_code(), StatusCode::NOT_IMPLEMENTED);
    assert_eq!(error.content_type(), "application/xml");
    assert_eq!(
        error.to_xml(),
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?><Error><Code>NotImplemented</Code><Message>A header you provided implies functionality that is not implemented.</Message><Resource>/bucket/object</Resource><RequestId>request-1</RequestId><HostId>host-1</HostId></Error>"
    );
}

#[test]
fn stable_error_codes_do_not_expose_lower_layer_details() {
    let error = S3Error::new(
        S3ErrorCode::ServiceUnavailable,
        "/bucket/key".into(),
        "request".into(),
        "host".into(),
    );
    assert_eq!(error.status_code(), hyper::StatusCode::SERVICE_UNAVAILABLE);
    assert!(error.to_xml().contains("<Code>ServiceUnavailable</Code>"));
    assert!(!error.to_xml().contains("partition"));
}

#[test]
fn transient_errors_expose_only_bounded_retry_advice() {
    let error = S3Error::new(
        S3ErrorCode::SlowDown,
        "/bucket/key".into(),
        "request".into(),
        "host".into(),
    );
    assert_eq!(error.retry_after_seconds(), Some(1));
    assert_eq!(error.request_id(), "request");
    assert_eq!(error.host_id(), "host");
}

#[test]
fn every_public_error_class_has_a_stable_status_and_code() {
    let cases = [
        (
            S3ErrorCode::NotImplemented,
            StatusCode::NOT_IMPLEMENTED,
            "NotImplemented",
        ),
        (S3ErrorCode::NoSuchBucket, StatusCode::NOT_FOUND, "NoSuchBucket"),
        (S3ErrorCode::NoSuchKey, StatusCode::NOT_FOUND, "NoSuchKey"),
        (
            S3ErrorCode::BucketNotEmpty,
            StatusCode::CONFLICT,
            "BucketNotEmpty",
        ),
        (
            S3ErrorCode::InvalidRequest,
            StatusCode::BAD_REQUEST,
            "InvalidRequest",
        ),
        (
            S3ErrorCode::InvalidRange,
            StatusCode::RANGE_NOT_SATISFIABLE,
            "InvalidRange",
        ),
        (
            S3ErrorCode::PreconditionFailed,
            StatusCode::PRECONDITION_FAILED,
            "PreconditionFailed",
        ),
        (S3ErrorCode::SlowDown, StatusCode::SERVICE_UNAVAILABLE, "SlowDown"),
        (
            S3ErrorCode::InternalError,
            StatusCode::INTERNAL_SERVER_ERROR,
            "InternalError",
        ),
        (
            S3ErrorCode::ServiceUnavailable,
            StatusCode::SERVICE_UNAVAILABLE,
            "ServiceUnavailable",
        ),
        (S3ErrorCode::AccessDenied, StatusCode::FORBIDDEN, "AccessDenied"),
        (
            S3ErrorCode::InvalidAccessKeyId,
            StatusCode::FORBIDDEN,
            "InvalidAccessKeyId",
        ),
        (
            S3ErrorCode::SignatureDoesNotMatch,
            StatusCode::FORBIDDEN,
            "SignatureDoesNotMatch",
        ),
        (
            S3ErrorCode::RequestTimeTooSkewed,
            StatusCode::BAD_REQUEST,
            "RequestTimeTooSkewed",
        ),
        (
            S3ErrorCode::InvalidDigest,
            StatusCode::BAD_REQUEST,
            "InvalidDigest",
        ),
        (S3ErrorCode::BadDigest, StatusCode::BAD_REQUEST, "BadDigest"),
        (
            S3ErrorCode::XAmzContentSHA256Mismatch,
            StatusCode::BAD_REQUEST,
            "XAmzContentSHA256Mismatch",
        ),
    ];

    for (code, status, name) in cases {
        let error = S3Error::new(code, "/resource".into(), "request".into(), "host".into());
        assert_eq!(error.status_code(), status);
        assert!(error.to_xml().contains(&format!("<Code>{name}</Code>")));
        assert_eq!(
            error.retry_after_seconds(),
            (code == S3ErrorCode::SlowDown).then_some(1)
        );
    }
}

#[test]
fn not_implemented_escapes_client_derived_resource() {
    let error = S3Error::not_implemented("/bucket/<&>\"'".into(), "request".into(), "host".into());

    assert!(error
        .to_xml()
        .contains("<Resource>/bucket/&lt;&amp;&gt;&quot;&apos;</Resource>"));
}
