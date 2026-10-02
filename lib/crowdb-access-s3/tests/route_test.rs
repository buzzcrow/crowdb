// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_access_s3::route::{
    classify, classify_multipart, classify_request, MultipartOperation, RouteError, S3Operation,
};
use hyper::header::HeaderValue;
use hyper::{HeaderMap, Method, Uri};

#[test]
fn classifies_the_finite_path_style_surface() {
    let cases = [
        (Method::GET, "/", S3Operation::ListBuckets),
        (Method::PUT, "/bucket", S3Operation::CreateBucket),
        (Method::HEAD, "/bucket", S3Operation::HeadBucket),
        (Method::DELETE, "/bucket", S3Operation::DeleteBucket),
        (Method::GET, "/bucket?list-type=2", S3Operation::ListObjectsV2),
        (Method::GET, "/bucket/?list-type=2", S3Operation::ListObjectsV2),
        (Method::HEAD, "/bucket/", S3Operation::HeadBucket),
        (Method::GET, "/bucket//", S3Operation::GetObject),
        (Method::PUT, "/bucket/key", S3Operation::PutObject),
        (Method::HEAD, "/bucket/key", S3Operation::HeadObject),
        (Method::GET, "/bucket/key", S3Operation::GetObject),
        (Method::DELETE, "/bucket/key", S3Operation::DeleteObject),
    ];
    for (method, uri, expected) in cases {
        assert_eq!(
            classify(&method, &uri.parse::<Uri>().unwrap()).unwrap().operation,
            expected
        );
    }
}

#[test]
fn rejects_extensions_before_dispatch() {
    assert_eq!(
        classify(&Method::POST, &"/bucket/key?uploads".parse().unwrap()),
        Err(RouteError::NotImplemented)
    );
    assert_eq!(
        classify(&Method::PUT, &"/bucket/key?partNumber=7".parse().unwrap()),
        Err(RouteError::NotImplemented)
    );
}

#[test]
fn copy_headers_select_only_object_or_multipart_part_puts() {
    let mut headers = HeaderMap::new();
    headers.insert("x-amz-copy-source", HeaderValue::from_static("source/key"));
    assert_eq!(
        classify_request(&Method::PUT, &"/bucket/key".parse().unwrap(), &headers)
            .unwrap()
            .operation,
        S3Operation::CopyObject
    );
    let uri = format!("/bucket/key?uploadId={}&partNumber=2", "ab".repeat(16))
        .parse()
        .unwrap();
    assert_eq!(
        classify_request(&Method::PUT, &uri, &headers).unwrap().operation,
        S3Operation::UploadPartCopy
    );
    for (method, uri) in [
        (Method::GET, "/bucket/key"),
        (Method::PUT, "/bucket"),
        (Method::POST, "/bucket/key?uploads"),
    ] {
        assert_eq!(
            classify_request(&method, &uri.parse().unwrap(), &headers),
            Err(RouteError::Invalid)
        );
    }
    headers.remove("x-amz-copy-source");
    headers.insert("x-amz-copy-source-if-match", HeaderValue::from_static("abc"));
    assert_eq!(
        classify_request(&Method::PUT, &"/bucket/key".parse().unwrap(), &headers),
        Err(RouteError::Invalid)
    );
}

#[test]
fn rejects_headers_that_select_excluded_behavior() {
    let mut headers = HeaderMap::new();
    headers.insert("x-amz-storage-class", HeaderValue::from_static("GLACIER"));
    assert_eq!(
        classify_request(&Method::PUT, &"/bucket/key".parse().unwrap(), &headers),
        Err(RouteError::NotImplemented)
    );
    headers.clear();
    headers.insert("x-amz-meta-mtime", HeaderValue::from_static("123"));
    assert_eq!(
        classify_request(&Method::PUT, &"/bucket/key".parse().unwrap(), &headers),
        Err(RouteError::NotImplemented)
    );
    assert_eq!(
        classify(&Method::GET, &"/bucket?location".parse().unwrap()),
        Err(RouteError::NotImplemented)
    );
}

#[test]
fn multipart_queries_have_unambiguous_paths_and_identities() {
    let id = "ab".repeat(16);
    let cases = [
        (
            Method::POST,
            "/bucket/key?uploads".to_string(),
            MultipartOperation::Create,
        ),
        (
            Method::GET,
            "/bucket?uploads".to_string(),
            MultipartOperation::ListUploads,
        ),
        (
            Method::PUT,
            format!("/bucket/key?partNumber=7&uploadId={id}"),
            MultipartOperation::UploadPart,
        ),
        (
            Method::GET,
            format!("/bucket/key?uploadId={id}"),
            MultipartOperation::ListParts,
        ),
        (
            Method::POST,
            format!("/bucket/key?uploadId={id}"),
            MultipartOperation::Complete,
        ),
        (
            Method::DELETE,
            format!("/bucket/key?uploadId={id}"),
            MultipartOperation::Abort,
        ),
    ];
    for (method, uri, expected) in cases {
        let uri = uri.parse().unwrap();
        let route = classify_multipart(&method, &uri).unwrap().unwrap();
        assert_eq!(route.operation, expected);
        assert_eq!(route.bucket, b"bucket");
        let authenticated = classify_request(&method, &uri, &HeaderMap::new()).unwrap();
        assert_eq!(authenticated.bucket.as_deref(), Some(b"bucket".as_slice()));
        assert_eq!(authenticated.upload_id, route.upload_id);
        assert_eq!(authenticated.part_number, route.part_number);
    }
    assert_eq!(
        classify_multipart(&Method::GET, &"/bucket/key".parse().unwrap()),
        Ok(None)
    );
    assert_eq!(
        classify_multipart(
            &Method::PUT,
            &format!("/bucket/key?uploadId={id}").parse().unwrap()
        ),
        Err(RouteError::Invalid)
    );
    assert_eq!(
        classify_multipart(&Method::POST, &"/bucket/key?uploadId=bad".parse().unwrap()),
        Err(RouteError::Invalid)
    );
    assert_eq!(
        classify_multipart(&Method::GET, &"/bucket?uploads&uploads".parse().unwrap()),
        Err(RouteError::Invalid)
    );
    assert_eq!(
        classify_multipart(&Method::PUT, &"/bucket/key?partNumber=7".parse().unwrap()),
        Err(RouteError::Invalid)
    );
}
