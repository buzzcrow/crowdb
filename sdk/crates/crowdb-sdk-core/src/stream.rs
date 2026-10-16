// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crate::{admission::Permit, Error};
use bytes::Bytes;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use tokio::sync::Notify;

/// A transport stream owns request capacity through EOF/drop. Retained frame
/// bytes additionally carry a bounded local window until their final owner drops.
pub struct ResponseStream {
    pub(crate) response: reqwest::Response,
    pub(crate) permit: Arc<Permit>,
    pub(crate) limit: usize,
    window: Arc<Window>,
    deadline: tokio::time::Instant,
}
impl ResponseStream {
    pub(crate) fn new(
        response: reqwest::Response,
        permit: Permit,
        limit: usize,
        deadline: tokio::time::Instant,
    ) -> Self {
        Self {
            deadline,
            response,
            permit: Arc::new(permit),
            limit,
            window: Arc::new(Window {
                used: AtomicUsize::new(0),
                limit,
                changed: Notify::new(),
            }),
        }
    }
    /// # Errors
    /// Reports deadline, transport or configured chunk-window errors.
    pub async fn next_chunk(&mut self) -> Result<Option<Bytes>, Error> {
        let bytes = self.response.chunk().await?;
        if bytes.is_none() {
            self.permit.release_request();
        }
        if bytes.as_ref().is_some_and(|v| v.len() > self.limit) {
            return Err(Error::Limit);
        }
        Ok(bytes)
    }
    /// Admits an owned decoded payload frame. This applies backpressure when
    /// prior frames are retained; callers release them before asking for more.
    /// # Errors
    /// Rejects one frame larger than the configured response window.
    pub async fn retain_frame(&self, bytes: Bytes) -> Result<Bytes, Error> {
        if bytes.len() > self.window.limit {
            return Err(Error::Limit);
        }
        let amount = bytes.len();
        loop {
            let notified = self.window.changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self
                .window
                .used
                .fetch_update(Ordering::AcqRel, Ordering::Acquire, |used| {
                    used.checked_add(amount).filter(|n| *n <= self.window.limit)
                })
                .is_ok()
            {
                return Ok(Bytes::from_owner(FrameOwner {
                    bytes,
                    amount,
                    window: self.window.clone(),
                    permit: self.permit.clone(),
                }));
            }
            tokio::time::timeout_at(self.deadline, notified)
                .await
                .map_err(|_| Error::Deadline)?;
        }
    }
}
struct Window {
    used: AtomicUsize,
    limit: usize,
    changed: Notify,
}
struct FrameOwner {
    bytes: Bytes,
    amount: usize,
    window: Arc<Window>,
    permit: Arc<Permit>,
}
impl AsRef<[u8]> for FrameOwner {
    fn as_ref(&self) -> &[u8] {
        let _ = &self.permit;
        &self.bytes
    }
}
impl Drop for FrameOwner {
    fn drop(&mut self) {
        self.window.used.fetch_sub(self.amount, Ordering::AcqRel);
        self.window.changed.notify_waiters();
    }
}
