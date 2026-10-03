// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Finite path-style S3 request classification.

use hyper::{HeaderMap, Method, Uri};
use percent_encoding::percent_decode_str;

mod multipart;

pub use multipart::{classify_multipart, MultipartOperation, MultipartRoute};

#[repr(usize)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum S3Operation {
    CreateBucket,
    HeadBucket,
    ListBuckets,
    DeleteBucket,
    PutObject,
    HeadObject,
    GetObject,
    ListObjectsV2,
    DeleteObject,
    CreateMultipartUpload,
    UploadPart,
    ListParts,
    CompleteMultipartUpload,
    AbortMultipartUpload,
    ListMultipartUploads,
    CopyObject,
    UploadPartCopy,
    DeleteObjects,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct S3Route {
    pub operation: S3Operation,
    pub bucket: Option<Vec<u8>>,
    pub key: Option<Vec<u8>>,
    pub upload_id: Option<[u8; 16]>,
    pub part_number: Option<u16>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RouteError {
    Invalid,
    NotImplemented,
}

/// Classifies only the advertised basic S3 surface.
///
/// # Errors
///
/// Returns `NotImplemented` for recognized extensions and `Invalid` for a
/// malformed path or method combination.
pub fn classify(method: &Method, uri: &Uri) -> Result<S3Route, RouteError> {
    if selects_extension(uri.query()) {
        return Err(RouteError::NotImplemented);
    }
    let path = uri.path().strip_prefix('/').ok_or(RouteError::Invalid)?;
    if path.is_empty() {
        return (*method == Method::GET)
            .then_some(S3Route {
                operation: S3Operation::ListBuckets,
                bucket: None,
                key: None,
                upload_id: None,
                part_number: None,
            })
            .ok_or(RouteError::Invalid);
    }
    let (bucket, key) = path.split_once('/').map_or((path, None), |(bucket, key)| {
        (bucket, (!key.is_empty()).then_some(key))
    });
    let bucket = decode(bucket);
    if bucket.is_empty() {
        return Err(RouteError::Invalid);
    }
    let key = key.map(decode);
    let operation = match (method, key.as_deref(), query_value(uri.query(), "list-type")) {
        (&Method::POST, None, _) if uri.query() == Some("delete") || uri.query() == Some("delete=") => {
            S3Operation::DeleteObjects
        }
        (&Method::PUT, None, _) => S3Operation::CreateBucket,
        (&Method::HEAD, None, _) => S3Operation::HeadBucket,
        (&Method::DELETE, None, _) => S3Operation::DeleteBucket,
        (&Method::GET, None, Some("2")) => S3Operation::ListObjectsV2,
        (&Method::PUT, Some(_), _) => S3Operation::PutObject,
        (&Method::HEAD, Some(_), _) => S3Operation::HeadObject,
        (&Method::GET, Some(_), _) => S3Operation::GetObject,
        (&Method::DELETE, Some(_), _) => S3Operation::DeleteObject,
        _ => return Err(RouteError::Invalid),
    };
    Ok(S3Route {
        operation,
        bucket: Some(bucket),
        key,
        upload_id: None,
        part_number: None,
    })
}

/// Classifies the finite surface and rejects headers that select excluded S3
/// behavior before any handler can silently ignore them.
///
/// # Errors
///
/// Returns `NotImplemented` for an excluded header and otherwise delegates to
/// [`classify`].
pub fn classify_request(method: &Method, uri: &Uri, headers: &HeaderMap) -> Result<S3Route, RouteError> {
    if !headers.contains_key("x-amz-copy-source")
        && headers.keys().any(|name| {
            name.as_str().starts_with("x-amz-copy-source") || name.as_str() == "x-amz-metadata-directive"
        })
    {
        return Err(RouteError::Invalid);
    }
    if let Some(name) = headers.keys().find(|name| {
        matches!(
            name.as_str(),
            "x-amz-acl"
                | "x-amz-server-side-encryption"
                | "x-amz-server-side-encryption-aws-kms-key-id"
                | "x-amz-server-side-encryption-context"
                | "x-amz-server-side-encryption-customer-algorithm"
                | "x-amz-server-side-encryption-customer-key"
                | "x-amz-server-side-encryption-customer-key-md5"
                | "x-amz-tagging"
                | "x-amz-website-redirect-location"
                | "x-amz-object-lock-mode"
                | "x-amz-object-lock-retain-until-date"
                | "x-amz-object-lock-legal-hold"
        )
    }) {
        tracing::debug!(header = %name, "unsupported S3 selector");
        return Err(RouteError::NotImplemented);
    }
    if let Some(multipart) = classify_multipart(method, uri)? {
        if headers.contains_key("x-amz-copy-source") && multipart.operation != MultipartOperation::UploadPart
        {
            return Err(RouteError::Invalid);
        }
        let operation = match multipart.operation {
            MultipartOperation::Create => S3Operation::CreateMultipartUpload,
            MultipartOperation::UploadPart if headers.contains_key("x-amz-copy-source") => {
                S3Operation::UploadPartCopy
            }
            MultipartOperation::UploadPart => S3Operation::UploadPart,
            MultipartOperation::ListParts => S3Operation::ListParts,
            MultipartOperation::Complete => S3Operation::CompleteMultipartUpload,
            MultipartOperation::Abort => S3Operation::AbortMultipartUpload,
            MultipartOperation::ListUploads => S3Operation::ListMultipartUploads,
        };
        validate_publication_headers(operation, headers)?;
        return Ok(S3Route {
            operation,
            bucket: Some(multipart.bucket),
            key: multipart.key,
            upload_id: multipart.upload_id,
            part_number: multipart.part_number,
        });
    }
    let mut route = classify(method, uri)?;
    if headers.contains_key("x-amz-copy-source") {
        if route.operation != S3Operation::PutObject {
            return Err(RouteError::Invalid);
        }
        route.operation = S3Operation::CopyObject;
    }
    validate_publication_headers(route.operation, headers)?;
    Ok(route)
}

fn validate_publication_headers(operation: S3Operation, headers: &HeaderMap) -> Result<(), RouteError> {
    let storage_class = headers.get_all("x-amz-storage-class");
    let mut values = storage_class.iter();
    if let Some(value) = values.next() {
        if values.next().is_some() {
            return Err(RouteError::Invalid);
        }
        if value != "STANDARD" {
            return Err(RouteError::NotImplemented);
        }
    }
    if (headers.contains_key("x-amz-storage-class")
        || headers
            .keys()
            .any(|name| name.as_str().starts_with("x-amz-meta-")))
        && !matches!(
            operation,
            S3Operation::PutObject | S3Operation::CreateMultipartUpload | S3Operation::CopyObject
        )
    {
        return Err(RouteError::Invalid);
    }
    Ok(())
}

fn decode(value: &str) -> Vec<u8> {
    percent_decode_str(value).collect()
}

fn selects_extension(query: Option<&str>) -> bool {
    query.is_some_and(|query| {
        query.split('&').any(|part| {
            let name = part.split_once('=').map_or(part, |(name, _)| name);
            matches!(
                name,
                "uploads"
                    | "location"
                    | "acl"
                    | "versioning"
                    | "annotations"
                    | "annotation"
                    | "uploadId"
                    | "partNumber"
                    | "versionId"
                    | "tagging"
                    | "lifecycle"
                    | "replication"
                    | "website"
                    | "notification"
                    | "select"
            )
        })
    })
}

fn query_value<'a>(query: Option<&'a str>, key: &str) -> Option<&'a str> {
    query?.split('&').find_map(|part| {
        let (name, value) = part.split_once('=')?;
        (name == key).then_some(value)
    })
}
