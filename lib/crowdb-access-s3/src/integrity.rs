// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Single-part S3 integrity values independent of storage chunk boundaries.

use base64::engine::general_purpose::STANDARD;
use base64::Engine as _;
use hyper::body::Bytes;
use sha2::{Digest as _, Sha256};

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
/// chunks, so frame and EC boundaries cannot change it. Multipart has its own
/// future contract.
pub struct SinglePartIntegrity {
    md5: md5::Context,
    sha256: Sha256,
}

impl Default for SinglePartIntegrity {
    fn default() -> Self {
        Self {
            md5: md5::Context::new(),
            sha256: Sha256::new(),
        }
    }
}

impl SinglePartIntegrity {
    /// Adds one immutable body frame without copying it.
    pub fn update(&mut self, bytes: &Bytes) {
        self.md5.consume(bytes);
        self.sha256.update(bytes);
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
        let result = (format!("{digest:x}"), digest.0.to_vec());
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
            let actual = format!("{:x}", sha256.finalize());
            if !actual.eq_ignore_ascii_case(expected) {
                return Err(IntegrityError::PayloadMismatch);
            }
        }
        Ok(result)
    }
}
