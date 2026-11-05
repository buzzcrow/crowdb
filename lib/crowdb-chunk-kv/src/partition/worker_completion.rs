// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use futures::{
    future::{BoxFuture, Shared},
    FutureExt,
};
use tokio::sync::{mpsc, oneshot};

use super::{run_worker, Partition, WorkerRequest, WorkerState};
use crate::{ChunkKvError, Result};

pub(super) type WorkerCompletion = Shared<BoxFuture<'static, Result<()>>>;

pub(super) fn spawn(state: WorkerState, receiver: mpsc::Receiver<WorkerRequest>) -> WorkerCompletion {
    let (completed, completion) = oneshot::channel();
    tokio::spawn(async move {
        run_worker(state, receiver).await;
        let _ = completed.send(());
    });
    async move { completion.await.map_err(|_| ChunkKvError::WriteStalled) }
        .boxed()
        .shared()
}

impl Partition {
    /// Consumes this handle and waits until the mutation worker releases its
    /// Tree and WAL owners. Other owning partition clones must also be dropped.
    /// Complete the transfer or shutdown protocol before joining a serving owner;
    /// this join does not change fencing, checkpoints, or repair state.
    ///
    /// # Errors
    /// Returns `WriteStalled` if the worker was cancelled or panicked.
    pub async fn close(self) -> Result<()> {
        let completion = self.worker_completion.clone();
        drop(self);
        completion.await
    }
}
