// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Freeze one predecessor before claiming a new per-process epoch.
use super::{KeepAlive, Ordering};
use crowdb_kv_client::{Error, Result};
use std::sync::Arc;

pub(super) struct Predecessor {
    boot: Option<String>,
    epoch: u64,
}

impl KeepAlive {
    pub(super) async fn ensure_process_session(&self) -> Result<u64> {
        let epoch = self.process_epoch.load(Ordering::Acquire);
        if epoch != 0 {
            return Ok(epoch);
        }
        let predecessor = if let Some(accepted) = self.previous_session.load_full() {
            accepted
        } else {
            let (boot, epoch) = self
                .svc
                .diskdb_session_predecessor(self.container.instance_id)
                .await?;
            let observed = Arc::new(Predecessor { boot, epoch });
            let accepted = self
                .previous_session
                .compare_and_swap(&None::<Arc<Predecessor>>, Some(Arc::clone(&observed)));
            accepted.as_ref().map_or(observed, Arc::clone)
        };
        let claimed = self
            .svc
            .claim_diskdb_session(
                self.container.instance_id,
                &self.boot_id,
                predecessor.boot.as_deref(),
                predecessor.epoch,
            )
            .await?;
        match self
            .process_epoch
            .compare_exchange(0, claimed, Ordering::AcqRel, Ordering::Acquire)
        {
            Ok(_) => Ok(claimed),
            Err(current) if current == claimed => Ok(current),
            Err(_) => Err(Error::Server(
                "DiskDB process session changed during startup".into(),
            )),
        }
    }
}
