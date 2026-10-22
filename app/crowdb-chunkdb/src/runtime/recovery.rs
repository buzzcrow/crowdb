// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Startup remains initializing while durable block intents await their owners.

use crate::lifecycle::{LifecycleError, LifecycleHandler};
use std::time::Duration;
use tokio::sync::watch;

pub(super) async fn recover(
    handler: &LifecycleHandler,
    mut stop: watch::Receiver<bool>,
) -> Result<(), LifecycleError> {
    let recovery = async {
        let mut last_error = None;
        loop {
            match handler.reconcile_pending_chunks().await {
                Ok(_) => return Ok(()),
                Err(LifecycleError::RecoveryDependency(error)) => {
                    if last_error.as_ref() != Some(&error) {
                        tracing::warn!(%error, "startup recovery waiting for block owner; retaining durable intents");
                        last_error = Some(error);
                    }
                }
                Err(error) => return Err(error),
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    };
    tokio::select! {
        biased;
        () = async {
            while !*stop.borrow_and_update() {
                if stop.changed().await.is_err() { break; }
            }
        } => Err(LifecycleError::InvalidRequest("startup recovery stopped".into())),
        result = recovery => result,
    }
}
