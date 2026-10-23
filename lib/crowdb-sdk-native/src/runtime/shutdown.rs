// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! One shared close/drain outcome, independent of each shutdown caller's lifetime.

use super::ClientRuntime;
use crate::{Error, Result};
use arc_swap::ArcSwapOption;
use futures::FutureExt;
use std::panic::AssertUnwindSafe;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::sync::Notify;

#[derive(Default)]
pub(super) struct ShutdownState {
    started: AtomicBool,
    result: ArcSwapOption<Result<()>>,
    changed: Notify,
}

impl ClientRuntime {
    /// Closes admission, drains owned writer groups, and joins owned RPC workers.
    /// Borrowed clients remain usable by their original owner. Held buffers retain
    /// storage until final release. Cancelling this call does not cancel cleanup;
    /// every subsequent caller observes the same terminal result.
    /// # Errors
    /// Returns the first drain/worker failure after attempting every owned close.
    pub async fn shutdown(&self) -> Result<()> {
        self.resources.shutdown();
        if !self.shutdown.started.swap(true, Ordering::AcqRel) {
            let runtime = self.clone();
            tokio::spawn(async move {
                let result = AssertUnwindSafe(runtime.finish_shutdown())
                    .catch_unwind()
                    .await
                    .unwrap_or_else(|_| Err(Error::Storage("native runtime shutdown panicked".into())));
                runtime.shutdown.result.store(Some(Arc::new(result)));
                runtime.shutdown.changed.notify_waiters();
            });
        }
        loop {
            let changed = self.shutdown.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if let Some(result) = self.shutdown.result.load_full() {
                return result.as_ref().clone();
            }
            changed.await;
        }
    }

    async fn finish_shutdown(&self) -> Result<()> {
        let groups = self.groups.load_full();
        let mut failure = None;
        for group in groups.iter().filter(|group| group.owns_small_pool) {
            if let Err(error) = group.chunks.shutdown_small_writes().await {
                failure.get_or_insert_with(|| Error::Storage(error.to_string()));
            }
        }
        let workers = groups.clone();
        let stopped = tokio::task::spawn_blocking(move || {
            for group in workers.iter().filter(|group| group.owns_transports) {
                group.chunks.shutdown_transports();
                group.metadata_transport.shutdown();
            }
        })
        .await;
        if let Err(error) = stopped {
            failure.get_or_insert_with(|| Error::Storage(error.to_string()));
        }
        failure.map_or(Ok(()), Err)
    }
}
