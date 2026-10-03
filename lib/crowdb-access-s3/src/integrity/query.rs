// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use hyper::header::{HeaderName, HeaderValue};
use hyper::HeaderMap;
use percent_encoding::percent_decode_str;

use crate::route::S3Operation;
use crate::S3ErrorCode;

/// Carries authenticated, hoisted upload checksums into the body verifier.
///
/// Call only after authenticating the original URI and headers. Signature
/// verification must see the original request, before this normalization.
///
/// # Errors
/// Rejects ambiguous, unsupported or misplaced checksum declarations without
/// mutating the supplied headers. Digest syntax and bytes use the body verifier.
pub fn merge_presigned_upload_checksums(
    operation: S3Operation,
    query: Option<&str>,
    headers: &mut HeaderMap,
) -> Result<(), S3ErrorCode> {
    let mut additions = HeaderMap::new();
    let mut presigned = false;
    for field in query.unwrap_or_default().split('&') {
        let (name, value) = field.split_once('=').unwrap_or((field, ""));
        let name = percent_decode_str(name)
            .decode_utf8()
            .map_err(|_| S3ErrorCode::InvalidRequest)?;
        if name == "X-Amz-Algorithm" {
            presigned = true;
        }
        if name == "x-amz-checksum-mode"
            || !(name.starts_with("x-amz-checksum-") || name == "x-amz-sdk-checksum-algorithm")
        {
            continue;
        }
        if !matches!(
            name.as_ref(),
            "x-amz-checksum-crc32"
                | "x-amz-checksum-crc32c"
                | "x-amz-checksum-crc64nvme"
                | "x-amz-checksum-sha1"
                | "x-amz-checksum-sha256"
                | "x-amz-sdk-checksum-algorithm"
        ) {
            return Err(S3ErrorCode::NotImplemented);
        }
        if headers.contains_key(name.as_ref()) || additions.contains_key(name.as_ref()) {
            return Err(S3ErrorCode::InvalidRequest);
        }
        let value = percent_decode_str(value).collect::<Vec<_>>();
        if value.is_empty() || value.len() > 128 {
            return Err(S3ErrorCode::InvalidRequest);
        }
        additions.insert(
            HeaderName::from_bytes(name.as_bytes()).map_err(|_| S3ErrorCode::InvalidRequest)?,
            HeaderValue::from_bytes(&value).map_err(|_| S3ErrorCode::InvalidRequest)?,
        );
    }
    if !additions.is_empty()
        && (!presigned || !matches!(operation, S3Operation::PutObject | S3Operation::UploadPart))
    {
        return Err(S3ErrorCode::InvalidRequest);
    }
    headers.extend(additions);
    Ok(())
}
