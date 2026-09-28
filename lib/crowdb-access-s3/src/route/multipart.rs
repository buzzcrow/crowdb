// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Parse the S3 multipart query surface without enabling HTTP dispatch yet.

use hyper::{Method, Uri};

use super::{decode, RouteError};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MultipartOperation {
    Create,
    UploadPart,
    ListParts,
    Complete,
    Abort,
    ListUploads,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MultipartRoute {
    pub operation: MultipartOperation,
    pub bucket: Vec<u8>,
    pub key: Option<Vec<u8>>,
    pub upload_id: Option<[u8; 16]>,
    pub part_number: Option<u16>,
}

/// Classifies a multipart query after the common `SigV4` authentication step.
///
/// # Errors
/// Rejects duplicate selectors, malformed identities and unsupported method
/// or path combinations. Returns `None` for ordinary basic S3 requests.
pub fn classify_multipart(method: &Method, uri: &Uri) -> Result<Option<MultipartRoute>, RouteError> {
    let mut uploads = false;
    let mut upload_id = None;
    let mut part_number = None;
    for pair in uri.query().unwrap_or_default().split('&') {
        let (name, value) = pair.split_once('=').unwrap_or((pair, ""));
        match name {
            "uploads" if !uploads && value.is_empty() => uploads = true,
            "uploadId" if upload_id.is_none() => upload_id = Some(parse_upload_id(value)?),
            "partNumber" if part_number.is_none() => {
                let number = value.parse::<u16>().map_err(|_| RouteError::Invalid)?;
                if number == 0 || number > 10_000 {
                    return Err(RouteError::Invalid);
                }
                part_number = Some(number);
            }
            "uploads" | "uploadId" | "partNumber" => return Err(RouteError::Invalid),
            _ => {}
        }
    }
    if !uploads && upload_id.is_none() {
        return Ok(None);
    }
    if uploads && (upload_id.is_some() || part_number.is_some()) {
        return Err(RouteError::Invalid);
    }
    let path = uri.path().strip_prefix('/').ok_or(RouteError::Invalid)?;
    let (bucket, key) = path
        .split_once('/')
        .map_or((path, None), |(bucket, key)| (bucket, Some(key)));
    let bucket = decode(bucket);
    if bucket.is_empty() {
        return Err(RouteError::Invalid);
    }
    let key = key.map(decode);
    let operation = match (method, key.as_deref(), uploads, upload_id, part_number) {
        (&Method::POST, Some(_), true, None, None) => MultipartOperation::Create,
        (&Method::GET, None, true, None, None) => MultipartOperation::ListUploads,
        (&Method::PUT, Some(_), false, Some(_), Some(_)) => MultipartOperation::UploadPart,
        (&Method::GET, Some(_), false, Some(_), None) => MultipartOperation::ListParts,
        (&Method::POST, Some(_), false, Some(_), None) => MultipartOperation::Complete,
        (&Method::DELETE, Some(_), false, Some(_), None) => MultipartOperation::Abort,
        _ => return Err(RouteError::Invalid),
    };
    Ok(Some(MultipartRoute {
        operation,
        bucket,
        key,
        upload_id,
        part_number,
    }))
}

fn parse_upload_id(value: &str) -> Result<[u8; 16], RouteError> {
    if value.len() != 32 || !value.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(RouteError::Invalid);
    }
    let mut id = [0_u8; 16];
    for (output, pair) in id.iter_mut().zip(value.as_bytes().chunks_exact(2)) {
        *output = u8::from_str_radix(std::str::from_utf8(pair).map_err(|_| RouteError::Invalid)?, 16)
            .map_err(|_| RouteError::Invalid)?;
    }
    if id == [0; 16] {
        return Err(RouteError::Invalid);
    }
    Ok(id)
}
