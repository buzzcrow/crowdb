// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Single-part S3 integrity values independent of storage chunk boundaries.

mod query;
pub use query::merge_presigned_upload_checksums;

use base64::engine::general_purpose::STANDARD;
use base64::Engine as _;
use hyper::body::Bytes;
use sha2::{Digest as _, Sha256};
use std::fmt::Write as _;

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum IntegrityError {
    #[error("invalid Content-MD5")]
    InvalidDigest,
    #[error("Content-MD5 does not match the object bytes")]
    Mismatch,
    #[error("x-amz-content-sha256 is not a supported payload digest")]
    InvalidPayloadDigest,
    #[error("x-amz-content-sha256 does not match the object bytes")]
    PayloadMismatch,
}

/// Incremental MD5 state for basic single-part S3 `ETags`.
///
/// The `ETag` is lowercase hexadecimal MD5 of the logical object bytes. It is
/// intentionally calculated before metadata publication, never from physical
/// chunks, so frame and EC boundaries cannot change it. Multipart uses the
/// selected parts' raw MD5 digests to calculate a composite `ETag`.
pub struct SinglePartIntegrity {
    md5: md5::Context,
    sha256: Option<Sha256>,
}

impl Default for SinglePartIntegrity {
    fn default() -> Self {
        Self::new(false)
    }
}

impl SinglePartIntegrity {
    /// Enables payload SHA-256 only when the request declares that digest.
    #[must_use]
    pub fn new(check_payload_sha256: bool) -> Self {
        Self {
            md5: md5::Context::new(),
            sha256: check_payload_sha256.then(Sha256::new),
        }
    }

    /// Adds one immutable body frame without copying it.
    pub fn update(&mut self, bytes: &Bytes) {
        self.md5.consume(bytes);
        if let Some(sha256) = &mut self.sha256 {
            sha256.update(bytes);
        }
    }

    /// Returns the persisted single-part `ETag` and its raw checksum bytes.
    #[must_use]
    pub fn finish(self) -> (String, Vec<u8>) {
        let digest = self.md5.compute();
        (format!("{digest:x}"), digest.0.to_vec())
    }

    /// Finishes the digest and validates an optional standard `Content-MD5`.
    ///
    /// # Errors
    ///
    /// Rejects malformed base64 or a digest mismatch before publication.
    pub fn finish_validated(
        self,
        expected_content_md5: Option<&str>,
    ) -> Result<(String, Vec<u8>), IntegrityError> {
        self.finish_validated_checksums(expected_content_md5, None)
    }

    /// Finishes both basic integrity calculations and validates the optional
    /// HTTP MD5 and `SigV4` payload SHA-256 declarations.
    ///
    /// # Errors
    ///
    /// Rejects malformed or mismatched declared digests.
    pub fn finish_validated_checksums(
        self,
        expected_content_md5: Option<&str>,
        expected_payload_sha256: Option<&str>,
    ) -> Result<(String, Vec<u8>), IntegrityError> {
        let Self { md5, sha256 } = self;
        let digest = md5.compute();
        let sha256 = sha256.map(|sha256| sha256.finalize().into());
        validate_completed_digests(digest.0, sha256, expected_content_md5, expected_payload_sha256)
    }
}

/// Validates checksums produced by a separate object-scoped digest worker.
///
/// # Errors
/// Rejects malformed or mismatched declared digests.
pub fn validate_completed_digests(
    md5: [u8; 16],
    sha256: Option<[u8; 32]>,
    expected_content_md5: Option<&str>,
    expected_payload_sha256: Option<&str>,
) -> Result<(String, Vec<u8>), IntegrityError> {
    let result = (hex_digest(&md5), md5.to_vec());
    if let Some(expected) = expected_content_md5 {
        let expected = STANDARD
            .decode(expected)
            .map_err(|_| IntegrityError::InvalidDigest)?;
        if expected.len() != 16 {
            return Err(IntegrityError::InvalidDigest);
        }
        if expected != result.1 {
            return Err(IntegrityError::Mismatch);
        }
    }
    if let Some(expected) = expected_payload_sha256 {
        if expected.len() != 64 || !expected.bytes().all(|byte| byte.is_ascii_hexdigit()) {
            return Err(IntegrityError::InvalidPayloadDigest);
        }
        let sha256 = sha256.ok_or(IntegrityError::InvalidPayloadDigest)?;
        let actual = hex_digest(&sha256);
        if !actual.eq_ignore_ascii_case(expected) {
            return Err(IntegrityError::PayloadMismatch);
        }
    }
    Ok(result)
}

fn hex_digest(bytes: &[u8]) -> String {
    let mut value = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        write!(&mut value, "{byte:02x}").expect("writing to String cannot fail");
    }
    value
}

/// Encodes a composite multipart `ETag` as a distinct 18-byte metadata
/// checksum marker: 16 digest bytes followed by the part count.
#[must_use]
pub fn multipart_checksum_marker(etag: &str) -> Option<[u8; 18]> {
    let (digest, count) = etag.split_once('-')?;
    if digest.len() != 32 || count.starts_with('0') {
        return None;
    }
    let count: u16 = count.parse().ok()?;
    if count == 0 || count > 10_000 {
        return None;
    }
    let mut marker = [0_u8; 18];
    for (byte, pair) in marker[..16].iter_mut().zip(digest.as_bytes().chunks_exact(2)) {
        let pair = std::str::from_utf8(pair).ok()?;
        if pair.bytes().any(|byte| byte.is_ascii_uppercase()) {
            return None;
        }
        *byte = u8::from_str_radix(pair, 16).ok()?;
    }
    marker[16..].copy_from_slice(&count.to_be_bytes());
    Some(marker)
}

#[must_use]
pub fn is_multipart_checksum(checksum: &[u8], etag: &str) -> bool {
    checksum.len() == 18 && multipart_checksum_marker(etag).is_some_and(|marker| checksum == marker)
}
