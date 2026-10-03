// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crate::error::S3ErrorCode;
use crate::integrity::{IntegrityError, SinglePartIntegrity};
use base64::{engine::general_purpose::STANDARD, Engine as _};
use hyper::{body::Bytes, HeaderMap};

/// Validates every supported integrity declaration before deletion.
/// # Errors
/// Rejects missing, malformed, unsupported and mismatched checksums.
pub fn validate_integrity(headers: &HeaderMap, bytes: &Bytes) -> Result<(), S3ErrorCode> {
    let md5 = header(headers, "content-md5")?;
    let crc32 = header(headers, "x-amz-checksum-crc32")?;
    if md5.is_none() && crc32.is_none() {
        return Err(S3ErrorCode::InvalidDigest);
    }
    if let Some(algorithm) = header(headers, "x-amz-sdk-checksum-algorithm")? {
        if algorithm != "CRC32" || crc32.is_none() {
            return Err(S3ErrorCode::InvalidDigest);
        }
    }
    for name in headers.keys() {
        if name.as_str().starts_with("x-amz-")
            && !matches!(
                name.as_str(),
                "x-amz-content-sha256"
                    | "x-amz-user-agent"
                    | "x-amz-date"
                    | "x-amz-security-token"
                    | "x-amz-checksum-crc32"
                    | "x-amz-sdk-checksum-algorithm"
            )
        {
            return Err(S3ErrorCode::NotImplemented);
        }
    }
    let sha256 = header(headers, "x-amz-content-sha256")?.filter(|value| *value != "UNSIGNED-PAYLOAD");
    let mut integrity = SinglePartIntegrity::new(sha256.is_some());
    integrity.update(bytes);
    integrity
        .finish_validated_checksums(md5, sha256)
        .map_err(|error| match error {
            IntegrityError::InvalidDigest => S3ErrorCode::InvalidDigest,
            IntegrityError::Mismatch => S3ErrorCode::BadDigest,
            IntegrityError::InvalidPayloadDigest => S3ErrorCode::InvalidRequest,
            IntegrityError::PayloadMismatch => S3ErrorCode::XAmzContentSHA256Mismatch,
        })?;
    if let Some(expected) = crc32 {
        let expected = STANDARD
            .decode(expected)
            .map_err(|_| S3ErrorCode::InvalidDigest)?;
        if expected.len() != 4 {
            return Err(S3ErrorCode::InvalidDigest);
        }
        let actual = crc::Crc::<u32>::new(&crc::CRC_32_ISO_HDLC)
            .checksum(bytes)
            .to_be_bytes();
        if expected != actual {
            return Err(S3ErrorCode::BadDigest);
        }
    }
    Ok(())
}

fn header<'a>(headers: &'a HeaderMap, name: &str) -> Result<Option<&'a str>, S3ErrorCode> {
    let mut values = headers.get_all(name).iter();
    let Some(value) = values.next() else {
        return Ok(None);
    };
    if values.next().is_some() {
        return Err(S3ErrorCode::InvalidDigest);
    }
    value.to_str().map(Some).map_err(|_| S3ErrorCode::InvalidDigest)
}
