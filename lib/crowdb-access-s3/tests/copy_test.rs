// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_access_s3::{
    condition::ObjectConditions,
    metadata::{BucketId, ObjectRecord},
};
use crowdb_access_s3::{copy, S3ErrorCode};
use hyper::{header::HeaderValue, HeaderMap};

fn record() -> ObjectRecord {
    ObjectRecord {
        bucket_id: BucketId::new([1; 16]),
        key: b"source".to_vec(),
        logical_length: 4,
        checksum: vec![],
        etag: "abc".into(),
        created_at_ms: 1000,
        modified_at_ms: 2000,
        content_type: "text/plain".into(),
        attributes: vec![],
        data_reference: vec![],
        data_length: 4,
    }
}

#[test]
fn conditions_limits_and_self_copy_use_the_captured_record() {
    let mut source = record();
    assert!(copy::validate_object(&source, source.bucket_id, b"target", false).is_ok());
    assert_eq!(
        copy::validate_object(&source, source.bucket_id, b"source", false),
        Err(S3ErrorCode::InvalidRequest)
    );
    assert!(copy::validate_object(&source, source.bucket_id, b"source", true).is_ok());
    source.logical_length = copy::MAX_COPY_BYTES;
    assert!(copy::validate_object(&source, source.bucket_id, b"target", false).is_ok());
    source.logical_length += 1;
    assert_eq!(
        copy::validate_object(&source, source.bucket_id, b"target", false),
        Err(S3ErrorCode::InvalidRequest)
    );
    assert!(copy::check_conditions(
        &source,
        &ObjectConditions {
            if_match: Some("\"abc\""),
            if_unmodified_since: Some("Thu, 01 Jan 1970 00:00:01 GMT"),
            ..ObjectConditions::default()
        }
    )
    .is_ok());
    assert_eq!(
        copy::check_conditions(
            &source,
            &ObjectConditions {
                if_none_match: Some("\"abc\""),
                if_modified_since: Some("Thu, 01 Jan 1970 00:00:01 GMT"),
                ..ObjectConditions::default()
            }
        ),
        Err(S3ErrorCode::PreconditionFailed)
    );
    assert_eq!(
        copy::check_conditions(
            &source,
            &ObjectConditions {
                if_modified_since: Some("invalid"),
                ..ObjectConditions::default()
            }
        ),
        Err(S3ErrorCode::InvalidRequest)
    );
}

#[test]
fn source_addresses_decode_once_and_reject_ambiguous_selectors() {
    assert_eq!(
        copy::source("/bucket/a%2Fb%20%25%2B%E9%9B%AA").unwrap(),
        (b"bucket".to_vec(), "a/b %+雪".as_bytes().to_vec())
    );
    assert_eq!(
        copy::source("bucket/question%3FversionId%3Dliteral").unwrap().1,
        b"question?versionId=literal"
    );
    for invalid in [
        "bucket",
        "/bucket/",
        "/b%2Fucket/key",
        "/bucket/%00",
        "/bucket/%",
        "/bucket/%ag",
        "//bucket/key",
    ] {
        assert!(copy::source(invalid).is_err(), "{invalid}");
    }
    assert_eq!(
        copy::source("bucket/key?versionId=old"),
        Err(S3ErrorCode::NotImplemented)
    );
    assert!(copy::source(&format!("bucket/{}", "a".repeat(1025))).is_err());
}

#[test]
fn part_ranges_are_explicit_bounded_and_require_a_large_source() {
    let length = 6 * 1024 * 1024;
    assert!(copy::validate_range("bytes=0-0", length).is_ok());
    assert!(copy::validate_range(&format!("bytes=3-{}", length - 1), length).is_ok());
    for invalid in [
        "bytes=-1",
        "bytes=1-",
        "bytes=3-2",
        "bytes=0-999999999",
        "bytes=1-2,3-4",
        "bytes=+1-2",
        "bytes=18446744073709551616-2",
    ] {
        assert_eq!(
            copy::validate_range(invalid, length),
            Err(S3ErrorCode::InvalidRange)
        );
    }
    assert_eq!(
        copy::validate_range("bytes=0-1", 5 * 1024 * 1024),
        Err(S3ErrorCode::InvalidRequest)
    );
}

#[test]
fn copy_selectors_must_be_signed_and_unsupported_metadata_is_rejected() {
    let mut headers = HeaderMap::new();
    headers.insert("x-amz-copy-source", HeaderValue::from_static("bucket/key"));
    assert_eq!(
        copy::validate_headers(&headers, "host", false),
        Err(S3ErrorCode::AccessDenied)
    );
    assert!(copy::validate_headers(&headers, "host;x-amz-copy-source", false).is_ok());
    for excluded in [
        "x-amz-copy-source-server-side-encryption-customer-key",
        "x-amz-tagging-directive",
        "x-amz-checksum-algorithm",
        "if-match",
    ] {
        let mut test = headers.clone();
        test.insert(excluded, HeaderValue::from_static("value"));
        assert_eq!(
            copy::validate_headers(&test, "host;x-amz-copy-source", false),
            Err(S3ErrorCode::NotImplemented),
            "{excluded}"
        );
    }
    headers.insert("x-amz-meta-name", HeaderValue::from_static("value"));
    assert_eq!(
        copy::validate_headers(&headers, "host;x-amz-copy-source", false),
        Err(S3ErrorCode::AccessDenied)
    );
    assert!(copy::validate_headers(&headers, "host;x-amz-copy-source;x-amz-meta-name", false).is_ok());
    assert_eq!(
        copy::validate_headers(&headers, "host;x-amz-copy-source;x-amz-meta-name", true),
        Err(S3ErrorCode::NotImplemented)
    );
    headers.remove("x-amz-meta-name");
    headers.insert("x-amz-copy-source-range", HeaderValue::from_static("bytes=0-1"));
    assert_eq!(
        copy::validate_headers(&headers, "host;x-amz-copy-source;x-amz-copy-source-range", false),
        Err(S3ErrorCode::NotImplemented)
    );
    assert!(copy::validate_headers(&headers, "host;x-amz-copy-source;x-amz-copy-source-range", true).is_ok());
}

#[test]
fn copy_sdk_operation_marker_is_bound_to_the_selected_operation() {
    assert!(copy::validate_query(Some("x-id=CopyObject"), false).is_ok());
    assert!(copy::validate_query(Some("uploadId=abc&partNumber=1&x-id=UploadPartCopy"), true).is_ok());
    for query in ["x-id=DeleteObjects", "x-id=CopyObject&x-id=CopyObject", "x-id="] {
        assert_eq!(
            copy::validate_query(Some(query), false),
            Err(S3ErrorCode::InvalidRequest)
        );
    }
    assert_eq!(
        copy::validate_query(Some("versionId=1"), false),
        Err(S3ErrorCode::NotImplemented)
    );
}
