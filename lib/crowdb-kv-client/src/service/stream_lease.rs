// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Conditional Group-0 process leases for Stream and WAL owners.

use std::sync::Arc;

use crowdb_protocol::chunk_stream::{StreamProcessIdentity, StreamProcessLease, StreamPurpose};
use crowdb_protocol::key::{StreamProcessLeaseKey, TextKey};

use crate::{CrowdbKvClient, Error, GetOutcome, ReadMode};

#[derive(Debug, thiserror::Error)]
pub enum StreamProcessLeaseError {
    #[error("invalid Stream process lease: {0}")]
    Invalid(String),
    #[error("Stream owner process has been replaced")]
    Replaced,
    #[error("Stream owner process lease has expired")]
    Expired,
    #[error("Stream process lease outcome remains uncertain")]
    OutcomeUnknown,
    #[error(transparent)]
    Kv(#[from] Error),
}

type Result<T> = std::result::Result<T, StreamProcessLeaseError>;

pub struct StreamProcessLeaseClient {
    kv: Arc<CrowdbKvClient>,
}

impl StreamProcessLeaseClient {
    #[must_use]
    pub fn new(kv: Arc<CrowdbKvClient>) -> Self {
        Self { kv }
    }

    /// Capture a predecessor once before attempting a new process claim.
    /// # Errors
    /// Rejects malformed identity, unsupported records and unavailable Group 0.
    pub async fn load(&self, purpose: StreamPurpose, instance_id: u64) -> Result<Option<StreamProcessLease>> {
        Ok(self.read(purpose, instance_id).await?.map(|(record, _)| record))
    }

    /// Claim only from one fixed predecessor; an uncertain result reuses the nonce.
    /// Adoption still requires the returned predecessor boundary, late-request
    /// exclusion and ordered IO completion; the claim alone proves none of those.
    /// # Errors
    /// Rejects changed predecessors, expired own retries and invalid timing.
    pub async fn claim(
        &self,
        purpose: StreamPurpose,
        instance_id: u64,
        boot_nonce: [u8; 16],
        predecessor: Option<StreamProcessIdentity>,
        now_ms: u64,
        lease_ms: u64,
    ) -> Result<StreamProcessLease> {
        validate_claim(purpose, instance_id, boot_nonce, predecessor)?;
        let expires_at_ms = expiry(now_ms, lease_ms)?;
        let epoch = predecessor
            .map_or(Some(1), |previous| previous.process_epoch.checked_add(1))
            .ok_or_else(|| invalid("process epoch overflow"))?;
        for _ in 0..8 {
            let current = self.read(purpose, instance_id).await?;
            if let Some((record, _)) = &current {
                if record.identity.boot_nonce == boot_nonce && record.identity.process_epoch == epoch {
                    if now_ms >= record.expires_at_ms {
                        return Err(StreamProcessLeaseError::Expired);
                    }
                    return Ok(record.clone());
                }
            }
            if current.as_ref().map(|(record, _)| record.identity) != predecessor {
                return Err(StreamProcessLeaseError::Replaced);
            }
            let mut record = StreamProcessLease {
                identity: StreamProcessIdentity {
                    purpose,
                    instance_id,
                    process_epoch: epoch,
                    boot_nonce,
                },
                expires_at_ms,
                prior_exclusion_deadline_ms: 0,
                grant_sequence: 0,
                grant_expires_at_ms: 0,
            };
            if let Some((previous, _)) = &current {
                record.prior_exclusion_deadline_ms = previous
                    .prior_exclusion_deadline_ms
                    .max(previous.expires_at_ms)
                    .max(previous.grant_expires_at_ms);
                record.grant_sequence = previous.grant_sequence;
                record.grant_expires_at_ms = previous.grant_expires_at_ms;
            }
            let revision = current.as_ref().map_or(0, |(_, revision)| *revision);
            if self.write(&record, revision).await? {
                return Ok(record);
            }
        }
        Err(StreamProcessLeaseError::OutcomeUnknown)
    }

    /// Renew the same process without reviving a recorded expiry or replacement.
    /// # Errors
    /// Returns a fence, record, availability or unresolved-write error.
    pub async fn renew(
        &self,
        identity: StreamProcessIdentity,
        now_ms: u64,
        lease_ms: u64,
    ) -> Result<StreamProcessLease> {
        let expires_at_ms = expiry(now_ms, lease_ms)?;
        for _ in 0..8 {
            let Some((mut record, revision)) = self.read(identity.purpose, identity.instance_id).await?
            else {
                return Err(StreamProcessLeaseError::Replaced);
            };
            if record.identity != identity {
                return Err(StreamProcessLeaseError::Replaced);
            }
            if now_ms >= record.expires_at_ms {
                return Err(StreamProcessLeaseError::Expired);
            }
            record.expires_at_ms = record.expires_at_ms.max(expires_at_ms);
            if self.write(&record, revision).await? {
                return Ok(record);
            }
        }
        Err(StreamProcessLeaseError::OutcomeUnknown)
    }

    async fn read(
        &self,
        purpose: StreamPurpose,
        instance_id: u64,
    ) -> Result<Option<(StreamProcessLease, u64)>> {
        if instance_id == 0 {
            return Err(invalid("zero owner instance"));
        }
        let key = StreamProcessLeaseKey { purpose, instance_id }.to_path();
        match self
            .kv
            .get(0, 0, key.as_bytes(), ReadMode::Linearizable, None)
            .await?
        {
            GetOutcome::NotFound => Ok(None),
            GetOutcome::Found { value, revision } => {
                let record: StreamProcessLease =
                    serde_json::from_slice(&value).map_err(|e| invalid(&e.to_string()))?;
                record.validate().map_err(|e| invalid(&e.to_string()))?;
                if record.identity.purpose != purpose || record.identity.instance_id != instance_id {
                    return Err(invalid("record differs from its owner key"));
                }
                Ok(Some((record, revision)))
            }
        }
    }

    async fn write(&self, record: &StreamProcessLease, revision: u64) -> Result<bool> {
        record.validate().map_err(|e| invalid(&e.to_string()))?;
        let identity = record.identity;
        let key = StreamProcessLeaseKey {
            purpose: identity.purpose,
            instance_id: identity.instance_id,
        }
        .to_path();
        let value = serde_json::to_vec(record).map_err(|e| invalid(&e.to_string()))?;
        match self.kv.put_cas(0, 0, key.as_bytes(), &value, revision).await {
            Ok(_) => Ok(true),
            Err(Error::CasFailed { .. } | Error::CasBusy | Error::OutcomeUnknown) => Ok(false),
            Err(error) => Err(error.into()),
        }
    }
}

fn invalid(message: &str) -> StreamProcessLeaseError {
    StreamProcessLeaseError::Invalid(message.into())
}

fn expiry(now: u64, lease: u64) -> Result<u64> {
    if lease == 0 {
        return Err(invalid("zero lease duration"));
    }
    now.checked_add(lease)
        .ok_or_else(|| invalid("lease deadline overflow"))
}

fn validate_claim(
    purpose: StreamPurpose,
    instance_id: u64,
    nonce: [u8; 16],
    previous: Option<StreamProcessIdentity>,
) -> Result<()> {
    if instance_id == 0
        || nonce == [0; 16]
        || previous.is_some_and(|previous| {
            previous.purpose != purpose
                || previous.instance_id != instance_id
                || previous.process_epoch == 0
                || previous.boot_nonce == [0; 16]
                || previous.boot_nonce == nonce
        })
    {
        return Err(invalid("invalid process claim identity or predecessor"));
    }
    Ok(())
}
