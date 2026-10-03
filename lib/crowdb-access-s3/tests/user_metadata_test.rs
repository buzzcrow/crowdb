// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_access_s3::metadata::UserMetadata;
use hyper::{header::HeaderValue, HeaderMap};

#[test]
fn metadata_normalizes_names_and_preserves_opaque_values() {
    let mut input = HeaderMap::new();
    input.insert("X-Amz-Meta-Mtime", HeaderValue::from_static("123.456"));
    input.insert("x-amz-meta-origin", HeaderValue::from_static("a,b = c"));
    input.insert("x-amz-meta-empty", HeaderValue::from_static(""));
    input.insert("content-type", HeaderValue::from_static("text/plain"));
    let encoded = UserMetadata::from_headers(&input).unwrap().encode().unwrap();
    let mut output = HeaderMap::new();
    UserMetadata::decode(&encoded)
        .unwrap()
        .append_headers(&mut output)
        .unwrap();
    assert_eq!(output.len(), 3);
    assert_eq!(output["x-amz-meta-mtime"], "123.456");
    assert_eq!(output["x-amz-meta-origin"], "a,b = c");
    assert_eq!(output["x-amz-meta-empty"], "");
    let mut corrupt = encoded;
    corrupt.push(0);
    assert!(UserMetadata::decode(&corrupt).is_err());
}

#[test]
fn metadata_enforces_exact_combined_size_and_unique_valid_headers() {
    let mut input = HeaderMap::new();
    input.insert("x-amz-meta-k", HeaderValue::from_str(&"x".repeat(2047)).unwrap());
    assert!(UserMetadata::from_headers(&input).is_ok());
    input.insert("x-amz-meta-k", HeaderValue::from_str(&"x".repeat(2048)).unwrap());
    assert!(UserMetadata::from_headers(&input).is_err());
    input.clear();
    input.append("x-amz-meta-a", HeaderValue::from_static("one"));
    input.append("X-Amz-Meta-A", HeaderValue::from_static("two"));
    assert!(UserMetadata::from_headers(&input).is_err());
    for (name, value) in [
        ("x-amz-meta-", "empty-name"),
        ("x-amz-meta-a", "tab\tvalue"),
        ("x-amz-meta-a", "雪"),
    ] {
        input.clear();
        input.insert(name, HeaderValue::from_bytes(value.as_bytes()).unwrap());
        assert!(UserMetadata::from_headers(&input).is_err());
    }
}
