// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! `EcWorker` — streaming EC compute, owned by `EcStripWriter`.
//!
//! Accepts data shards incrementally via `push`, computes parity
//! shards as data arrives (streaming compute — overlaps with
//! data-block writes). `finish` finalizes and returns the code_num
//! parity shards. `reset` clears state for reuse across strips.
//!
//! Pure compute — no IO, no disk. Trivially unit-testable.

use bytes::Bytes;
use crowdb_common::ec::{EcScheme, IncrementalParity};

use crate::{IoError, Result};

/// Streaming EC parity compute worker. Owned by each `EcStripWriter`.
pub struct EcWorker {
    ec_scheme: EcScheme,
    parity: Option<IncrementalParity>,
    shards_received: usize,
}

impl EcWorker {
    /// Construct a new worker for the given EC scheme.
    pub fn new(ec_scheme: EcScheme) -> Self {
        Self {
            ec_scheme,
            parity: IncrementalParity::new(ec_scheme).ok(),
            shards_received: 0,
        }
    }

    /// Feed one data shard and immediately fold it into parity. The worker
    /// does not retain the data shard after this call.
    pub fn push(&mut self, buffer: &Bytes) -> Result<()> {
        self.push_views(std::slice::from_ref(buffer))
    }

    /// Fold one logical data shard represented by immutable owner views.
    pub fn push_views(&mut self, buffers: &[Bytes]) -> Result<()> {
        if self.shards_received >= self.ec_scheme.data_num {
            return Err(IoError::EcEncodeFailed(format!(
                "too many data shards: got {}, max {}",
                self.shards_received + 1,
                self.ec_scheme.data_num
            )));
        }
        self.parity
            .as_mut()
            .ok_or_else(|| IoError::EcEncodeFailed("invalid EC scheme".into()))?
            .push_views(&buffers.iter().map(Bytes::as_ref).collect::<Vec<_>>())
            .map_err(|error| IoError::EcEncodeFailed(error.to_string()))?;
        self.shards_received += 1;
        Ok(())
    }

    /// Return the incrementally encoded `code_num` parity shards.
    pub fn finish(&mut self) -> Result<Vec<Vec<u8>>> {
        self.parity
            .take()
            .ok_or_else(|| IoError::EcEncodeFailed("EC worker was already finished".into()))?
            .finish_partial()
            .map_err(|error| IoError::EcEncodeFailed(error.to_string()))
    }

    /// Reset to accept a new strip.
    pub fn reset(&mut self) {
        self.parity = IncrementalParity::new(self.ec_scheme).ok();
        self.shards_received = 0;
    }

    /// Number of data shards received so far.
    pub fn shards_received(&self) -> usize {
        self.shards_received
    }

    /// The EC scheme this worker is configured for.
    pub fn ec_scheme(&self) -> EcScheme {
        self.ec_scheme
    }
}
