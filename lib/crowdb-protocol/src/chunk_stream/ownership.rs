// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Group-0 process identities and lease records for Stream owners.

use serde::{Deserialize, Serialize};
use thiserror::Error;

use super::StreamPurpose;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StreamProcessIdentity {
    pub purpose: StreamPurpose,
    pub instance_id: u64,
    pub process_epoch: u64,
    pub boot_nonce: [u8; 16],
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StreamProcessLease {
    pub identity: StreamProcessIdentity,
    pub expires_at_ms: u64,
    pub prior_exclusion_deadline_ms: u64,
    pub grant_sequence: u64,
    pub grant_expires_at_ms: u64,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum StreamLeaseRecordError {
    #[error("invalid Stream process lease identity or progress")]
    InvalidRecord,
    #[error("cannot generate a Stream process boot identity")]
    EntropyUnavailable,
}

impl StreamProcessLease {
    /// Generate one fixed boot nonce before capturing its predecessor.
    /// # Errors
    /// Returns an error when operating-system entropy is unavailable.
    pub fn generate_boot_nonce() -> Result<[u8; 16], StreamLeaseRecordError> {
        let mut nonce = [0; 16];
        getrandom::getrandom(&mut nonce).map_err(|_| StreamLeaseRecordError::EntropyUnavailable)?;
        if nonce == [0; 16] {
            return Err(StreamLeaseRecordError::EntropyUnavailable);
        }
        Ok(nonce)
    }

    /// # Errors
    /// Rejects absent identity, expiry or inconsistent grant fencing fields.
    pub fn validate(&self) -> Result<(), StreamLeaseRecordError> {
        let identity = &self.identity;
        if identity.instance_id == 0
            || identity.process_epoch == 0
            || identity.boot_nonce == [0; 16]
            || self.expires_at_ms == 0
            || (identity.process_epoch == 1) != (self.prior_exclusion_deadline_ms == 0)
            || (self.grant_sequence == 0) != (self.grant_expires_at_ms == 0)
            || (identity.purpose == StreamPurpose::Stream && self.grant_sequence != 0)
        {
            return Err(StreamLeaseRecordError::InvalidRecord);
        }
        Ok(())
    }
}

/// One stream's owner assignment. Renewal lives in the referenced process row;
/// this record changes only on acquire or drained release.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StreamWriterOwner {
    pub stream_name: super::StreamName,
    pub writer_epoch: u64,
    pub process: StreamProcessIdentity,
    pub assignment_nonce: [u8; 16],
    pub released: bool,
}

impl StreamWriterOwner {
    /// # Errors
    /// Rejects missing writer, process or assignment identity.
    pub fn validate(&self) -> Result<(), StreamLeaseRecordError> {
        if self.writer_epoch == 0
            || self.process.instance_id == 0
            || self.process.process_epoch == 0
            || self.process.boot_nonce == [0; 16]
            || self.assignment_nonce == [0; 16]
        {
            return Err(StreamLeaseRecordError::InvalidRecord);
        }
        Ok(())
    }
}
