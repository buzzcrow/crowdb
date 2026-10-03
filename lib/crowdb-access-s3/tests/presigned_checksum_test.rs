// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_access_s3::integrity::merge_presigned_upload_checksums;
use crowdb_access_s3::route::S3Operation;
use crowdb_access_s3::S3ErrorCode;
use hyper::{header::HeaderValue, HeaderMap};

#[test]
fn authenticated_upload_query_preserves_base64_and_rejects_ambiguous_headers() {
    let query = "X-Amz-Algorithm=AWS4-HMAC-SHA256&x-amz-checksum-crc32=a%2Bb%2F%3D%3D&x-amz-sdk-checksum-algorithm=CRC32";
    for operation in [S3Operation::PutObject, S3Operation::UploadPart] {
        let mut headers = HeaderMap::new();
        merge_presigned_upload_checksums(operation, Some(query), &mut headers).unwrap();
        assert_eq!(headers["x-amz-checksum-crc32"], "a+b/==");
        assert_eq!(headers["x-amz-sdk-checksum-algorithm"], "CRC32");
        let original = headers.clone();
        assert_eq!(
            merge_presigned_upload_checksums(operation, Some(query), &mut headers),
            Err(S3ErrorCode::InvalidRequest)
        );
        assert_eq!(headers, original);
    }
}

#[test]
fn rejected_queries_leave_all_headers_unchanged() {
    for query in [
        "x-amz-checksum-crc32=AAAAAA%3D%3D",
        "X-Amz-Algorithm=x&x-amz-checksum-crc32=x&x-amz-checksum-crc32=y",
        "X-Amz-Algorithm=x&x-amz-checksum-crc32=x&%78-amz-checksum-crc32=y",
        "X-Amz-Algorithm=x&x-amz-checksum-crc32=%0D%0A",
        "X-Amz-Algorithm=x&x-amz-checksum-crc32=",
    ] {
        let mut headers = HeaderMap::new();
        headers.insert("host", HeaderValue::from_static("localhost"));
        let original = headers.clone();
        assert!(merge_presigned_upload_checksums(S3Operation::PutObject, Some(query), &mut headers).is_err());
        assert_eq!(headers, original);
    }
    let query = "X-Amz-Algorithm=x&x-amz-checksum-crc32=x";
    for operation in [
        S3Operation::CopyObject,
        S3Operation::CreateMultipartUpload,
        S3Operation::GetObject,
    ] {
        assert_eq!(
            merge_presigned_upload_checksums(operation, Some(query), &mut HeaderMap::new()),
            Err(S3ErrorCode::InvalidRequest)
        );
    }
    assert_eq!(
        merge_presigned_upload_checksums(
            S3Operation::PutObject,
            Some("X-Amz-Algorithm=x&x-amz-checksum-unknown=x"),
            &mut HeaderMap::new()
        ),
        Err(S3ErrorCode::NotImplemented)
    );
    assert!(merge_presigned_upload_checksums(
        S3Operation::GetObject,
        Some("X-Amz-Algorithm=x&x-amz-checksum-mode=ENABLED"),
        &mut HeaderMap::new()
    )
    .is_ok());
}
