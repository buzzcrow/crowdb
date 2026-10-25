// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::sync::atomic::Ordering;

use super::{lifecycle_code, lifecycle_from_code, write_state_error, Partition};
use crate::{PartitionLifecycle, Result};

impl Partition {
    /// Releases a healthy WAL writer after assignment and split tasks have stopped.
    /// This does not authorize a transfer or commit an unfinished split.
    ///
    /// # Errors
    ///
    /// Preserves recovery and fault fences and propagates WAL release failures.
    pub async fn suspend_for_shutdown(&self, ownership_epoch: u64) -> Result<()> {
        self.validate_epoch(ownership_epoch)?;
        loop {
            let observed = self.lifecycle.load(Ordering::Acquire);
            match lifecycle_from_code(observed) {
                PartitionLifecycle::Serving
                | PartitionLifecycle::Prepared
                | PartitionLifecycle::SplitPreparing
                | PartitionLifecycle::SplitFinalizing => {
                    if self
                        .lifecycle
                        .compare_exchange(
                            observed,
                            lifecycle_code(PartitionLifecycle::TransferFencing),
                            Ordering::AcqRel,
                            Ordering::Acquire,
                        )
                        .is_ok()
                    {
                        break;
                    }
                }
                PartitionLifecycle::TransferFencing => break,
                PartitionLifecycle::TransferQuiesced => return Ok(()),
                other => return Err(write_state_error(other)),
            }
        }
        self.release_drained_writer().await
    }

    pub(super) async fn release_drained_writer(&self) -> Result<()> {
        self.wait_for_admitted_mutations().await;
        if self.lifecycle() != PartitionLifecycle::TransferFencing {
            return Err(write_state_error(self.lifecycle()));
        }
        self.journal.release_writer().await?;
        match self.lifecycle.compare_exchange(
            lifecycle_code(PartitionLifecycle::TransferFencing),
            lifecycle_code(PartitionLifecycle::TransferQuiesced),
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            Ok(_) => Ok(()),
            Err(observed) if lifecycle_from_code(observed) == PartitionLifecycle::TransferQuiesced => Ok(()),
            Err(observed) => Err(write_state_error(lifecycle_from_code(observed))),
        }
    }
}
