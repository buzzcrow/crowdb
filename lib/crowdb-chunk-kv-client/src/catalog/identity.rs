// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::sync::atomic::{AtomicU64, Ordering};

use crowdb_protocol::chunk_kv::{ClientRequestId, Id128};
use rand::rngs::OsRng;
use rand::RngCore;

use crate::{ClientError, Result};

pub struct RequestIdentityAllocator {
    client_instance_id: Id128,
    next_sequence: AtomicU64,
}

impl Default for RequestIdentityAllocator {
    fn default() -> Self {
        Self::new()
    }
}

impl RequestIdentityAllocator {
    #[must_use]
    pub fn new() -> Self {
        let mut bytes = [0_u8; 16];
        loop {
            OsRng.fill_bytes(&mut bytes);
            let mut high = [0_u8; 8];
            let mut low = [0_u8; 8];
            high.copy_from_slice(&bytes[..8]);
            low.copy_from_slice(&bytes[8..]);
            let id = Id128 {
                high: u64::from_le_bytes(high),
                low: u64::from_le_bytes(low),
            };
            if id != Id128::default() {
                return Self {
                    client_instance_id: id,
                    next_sequence: AtomicU64::new(1),
                };
            }
        }
    }

    #[must_use]
    pub fn client_instance_id(&self) -> Id128 {
        self.client_instance_id
    }

    /// Restores a persisted handle namespace before issuing new operations.
    ///
    /// # Errors
    ///
    /// Returns an error for a zero client identity or sequence.
    pub fn resume(client_instance_id: Id128, next_sequence: u64) -> Result<Self> {
        if client_instance_id == Id128::default() || next_sequence == 0 {
            return Err(ClientError::InvalidRequest(
                "persisted client identity and next sequence must be nonzero".into(),
            ));
        }
        Ok(Self {
            client_instance_id,
            next_sequence: AtomicU64::new(next_sequence),
        })
    }

    /// Allocates exactly one identity for a new logical operation.
    ///
    /// # Errors
    ///
    /// Returns `SequenceExhausted` before the counter could wrap to zero.
    pub fn allocate(&self) -> Result<ClientRequestId> {
        let sequence = self
            .next_sequence
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                current.checked_add(1)
            })
            .map_err(|_| ClientError::SequenceExhausted)?;
        Ok(ClientRequestId {
            client_instance_id: self.client_instance_id,
            client_sequence: sequence,
        })
    }
}
