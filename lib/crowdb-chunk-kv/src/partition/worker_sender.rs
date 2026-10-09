// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Mutation worker ownership for retained-parent routing handles.

use tokio::sync::mpsc;

use super::WorkerRequest;
use crate::{ChunkKvError, Result};

#[derive(Clone)]
pub(super) enum WorkerSender {
    Owned(mpsc::Sender<WorkerRequest>),
    Borrowed(mpsc::WeakSender<WorkerRequest>),
}

impl WorkerSender {
    pub(super) fn owned(sender: mpsc::Sender<WorkerRequest>) -> Self {
        Self::Owned(sender)
    }

    pub(super) fn borrowed(&self) -> Self {
        match self {
            Self::Owned(sender) => Self::Borrowed(sender.downgrade()),
            Self::Borrowed(sender) => Self::Borrowed(sender.clone()),
        }
    }

    pub(super) fn strong_clone(&self) -> Self {
        self.upgrade().map_or_else(|_| self.clone(), Self::Owned)
    }

    fn upgrade(&self) -> Result<mpsc::Sender<WorkerRequest>> {
        match self {
            Self::Owned(sender) => Ok(sender.clone()),
            Self::Borrowed(sender) => sender.upgrade().ok_or(ChunkKvError::WriteStalled),
        }
    }

    pub(super) fn try_send(&self, request: WorkerRequest) -> Result<()> {
        match self {
            Self::Owned(sender) => sender.try_send(request).map_err(|_| ChunkKvError::Overloaded),
            Self::Borrowed(_) => self
                .upgrade()?
                .try_send(request)
                .map_err(|_| ChunkKvError::Overloaded),
        }
    }

    pub(super) async fn send(&self, request: WorkerRequest) -> Result<()> {
        match self {
            Self::Owned(sender) => sender.send(request).await.map_err(|_| ChunkKvError::WriteStalled),
            Self::Borrowed(_) => self
                .upgrade()?
                .send(request)
                .await
                .map_err(|_| ChunkKvError::WriteStalled),
        }
    }
}
