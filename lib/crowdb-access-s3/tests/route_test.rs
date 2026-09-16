// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_access_s3::route::{classify, classify_request, RouteError, S3Operation};
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
}

#[test]
fn rejects_headers_that_select_excluded_behavior() {
    let mut headers = HeaderMap::new();
    headers.insert("x-amz-storage-class", HeaderValue::from_static("GLACIER"));
    assert_eq!(
        classify_request(&Method::PUT, &"/bucket/key".parse().unwrap(), &headers),
        Err(RouteError::NotImplemented)
    );
}
