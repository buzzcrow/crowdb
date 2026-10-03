// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Strict source addresses and range selection for server-side copy.

use crate::error::S3ErrorCode;
use hyper::HeaderMap;
use percent_encoding::percent_decode_str;

pub const MAX_COPY_BYTES: u64 = 5 * 1024 * 1024 * 1024;

/// Validates signed query selectors, including the SDK's operation marker.
///
/// # Errors
/// Rejects unknown selectors and duplicate or mismatched operation markers.
pub fn validate_query(query: Option<&str>, part: bool) -> Result<(), S3ErrorCode> {
    let mut operation_seen = false;
    for field in query
        .unwrap_or_default()
        .split('&')
        .filter(|field| !field.is_empty())
    {
        let (name, value) = field.split_once('=').unwrap_or((field, ""));
        if name == "x-id" {
            let operation = if part { "UploadPartCopy" } else { "CopyObject" };
            if operation_seen || value != operation {
                return Err(S3ErrorCode::InvalidRequest);
            }
            operation_seen = true;
            continue;
        }
        let supported = matches!(
            name,
            "X-Amz-Algorithm"
                | "X-Amz-Credential"
                | "X-Amz-Date"
                | "X-Amz-Expires"
                | "X-Amz-SignedHeaders"
                | "X-Amz-Signature"
                | "X-Amz-Security-Token"
        ) || (part && matches!(name, "uploadId" | "partNumber"));
        if !supported {
            tracing::debug!(selector = name, "unsupported S3 copy query selector");
            return Err(S3ErrorCode::NotImplemented);
        }
    }
    Ok(())
}

/// Validates single-copy limits and supported self-copy semantics.
/// # Errors
/// Rejects an oversized source or a no-change self-copy.
pub fn validate_object(
    source: &crate::metadata::ObjectRecord,
    destination: crate::metadata::BucketId,
    key: &[u8],
    replace: bool,
) -> Result<(), S3ErrorCode> {
    if source.logical_length > MAX_COPY_BYTES
        || (source.bucket_id == destination && source.key == key && !replace)
    {
        return Err(S3ErrorCode::InvalidRequest);
    }
    Ok(())
}

/// Validates the finite copy header surface after signature authentication.
/// # Errors
/// Rejects unsupported metadata, body framing and unsigned copy selectors.
pub fn validate_headers(headers: &HeaderMap, signed_headers: &str, part: bool) -> Result<(), S3ErrorCode> {
    if headers.contains_key("transfer-encoding") {
        return Err(S3ErrorCode::InvalidRequest);
    }
    for name in headers.keys() {
        let name = name.as_str();
        if matches!(
            name,
            "cache-control"
                | "content-disposition"
                | "content-encoding"
                | "content-language"
                | "expires"
                | "if-match"
                | "if-none-match"
                | "if-modified-since"
                | "if-unmodified-since"
        ) {
            tracing::debug!(header = name, "unsupported S3 copy header");
            return Err(S3ErrorCode::NotImplemented);
        }
        if !name.starts_with("x-amz-") {
            continue;
        }
        let supported = matches!(
            name,
            "x-amz-date"
                | "x-amz-user-agent"
                | "x-amz-content-sha256"
                | "x-amz-security-token"
                | "x-amz-copy-source"
                | "x-amz-copy-source-if-match"
                | "x-amz-copy-source-if-none-match"
                | "x-amz-copy-source-if-modified-since"
                | "x-amz-copy-source-if-unmodified-since"
        ) || (part && name == "x-amz-copy-source-range")
            || (!part
                && (name == "x-amz-metadata-directive"
                    || name == "x-amz-storage-class"
                    || name.starts_with("x-amz-meta-")));
        if !supported {
            tracing::debug!(header = name, "unsupported S3 copy header");
            return Err(S3ErrorCode::NotImplemented);
        }
        if (name.starts_with("x-amz-copy-source")
            || name == "x-amz-metadata-directive"
            || name == "x-amz-storage-class"
            || name.starts_with("x-amz-meta-"))
            && !signed_headers.split(';').any(|signed| signed == name)
        {
            return Err(S3ErrorCode::AccessDenied);
        }
    }
    Ok(())
}

/// Validates source conditions using the captured generation only.
/// # Errors
/// Rejects malformed dates and maps all failed copy conditions to HTTP 412.
pub fn check_conditions(
    record: &crate::metadata::ObjectRecord,
    conditions: &crate::condition::ObjectConditions<'_>,
) -> Result<(), S3ErrorCode> {
    for date in [conditions.if_modified_since, conditions.if_unmodified_since]
        .into_iter()
        .flatten()
    {
        httpdate::parse_http_date(date).map_err(|_| S3ErrorCode::InvalidRequest)?;
    }
    match crate::condition::evaluate(record, conditions) {
        crate::condition::ConditionOutcome::Proceed => Ok(()),
        _ => Err(S3ErrorCode::PreconditionFailed),
    }
}

/// Decodes one bucket/key address, rejecting version selectors and malformed escapes.
///
/// # Errors
/// Rejects ambiguous or unsupported source addresses.
pub fn source(value: &str) -> Result<(Vec<u8>, Vec<u8>), S3ErrorCode> {
    if value.contains('?') {
        return Err(S3ErrorCode::NotImplemented);
    }
    let value = value.strip_prefix('/').unwrap_or(value);
    for (index, byte) in value.bytes().enumerate() {
        if byte == b'%'
            && !value
                .as_bytes()
                .get(index + 1..index + 3)
                .is_some_and(|pair| pair.iter().all(u8::is_ascii_hexdigit))
        {
            return Err(S3ErrorCode::InvalidRequest);
        }
    }
    let (bucket, key) = value.split_once('/').ok_or(S3ErrorCode::InvalidRequest)?;
    let bucket: Vec<u8> = percent_decode_str(bucket).collect();
    let key: Vec<u8> = percent_decode_str(key).collect();
    if bucket.is_empty()
        || bucket.len() > 63
        || bucket.contains(&b'/')
        || key.is_empty()
        || key.len() > 1_024
        || bucket.contains(&0)
        || key.contains(&0)
    {
        return Err(S3ErrorCode::InvalidRequest);
    }
    Ok((bucket, key))
}

/// Validates the explicit inclusive range required by `UploadPartCopy`.
///
/// # Errors
/// Rejects suffix/open/multiple ranges and bounds outside the source.
pub fn validate_range(value: &str, length: u64) -> Result<(), S3ErrorCode> {
    if length <= 5 * 1024 * 1024 {
        return Err(S3ErrorCode::InvalidRequest);
    }
    let (start, end) = value
        .strip_prefix("bytes=")
        .and_then(|value| value.split_once('-'))
        .ok_or(S3ErrorCode::InvalidRange)?;
    if start.is_empty()
        || end.is_empty()
        || !start.bytes().all(|b| b.is_ascii_digit())
        || !end.bytes().all(|b| b.is_ascii_digit())
    {
        return Err(S3ErrorCode::InvalidRange);
    }
    let start: u64 = start.parse().map_err(|_| S3ErrorCode::InvalidRange)?;
    let end: u64 = end.parse().map_err(|_| S3ErrorCode::InvalidRange)?;
    if start > end || end >= length {
        return Err(S3ErrorCode::InvalidRange);
    }
    Ok(())
}
