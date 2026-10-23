// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Caller request identity, deadline and cancellation ownership.

use std::future::Future;
use std::num::NonZeroU64;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Instant;

use tokio::sync::Notify;

use crate::{Error, Result};

#[derive(Default)]
struct CancellationState {
    cancelled: AtomicBool,
    wake: Notify,
}

#[derive(Clone, Default)]
pub struct Cancellation(Arc<CancellationState>);

impl Cancellation {
    pub fn cancel(&self) {
        self.0.cancelled.store(true, Ordering::Release);
        self.0.wake.notify_waiters();
    }

    #[must_use]
    pub fn is_cancelled(&self) -> bool {
        self.0.cancelled.load(Ordering::Acquire)
    }

    pub async fn cancelled(&self) {
        loop {
            let wake = self.0.wake.notified();
            tokio::pin!(wake);
            wake.as_mut().enable();
            if self.is_cancelled() {
                return;
            }
            wake.await;
        }
    }
}

#[derive(Clone)]
pub struct OperationContext {
    pub request_id: NonZeroU64,
    pub deadline: Instant,
    pub cancellation: Cancellation,
}

impl OperationContext {
    #[must_use]
    pub fn new(request_id: NonZeroU64, deadline: Instant) -> Self {
        Self {
            request_id,
            deadline,
            cancellation: Cancellation::default(),
        }
    }

    /// # Errors
    /// Returns cancellation or an expired total deadline before starting work.
    pub fn check(&self) -> Result<()> {
        if self.cancellation.is_cancelled() {
            Err(Error::Cancelled)
        } else if Instant::now() >= self.deadline {
            Err(Error::Deadline)
        } else {
            Ok(())
        }
    }

    /// Bounds a cancellable read or wait. Mutation publication must retain its
    /// own uncertain outcome if this future is interrupted after acceptance.
    /// # Errors
    /// Returns cancellation, deadline, or the supplied operation's failure.
    pub async fn run<T>(&self, future: impl Future<Output = Result<T>>) -> Result<T> {
        self.check()?;
        tokio::select! {
            biased;
            () = self.cancellation.cancelled() => Err(Error::Cancelled),
            () = tokio::time::sleep_until(self.deadline.into()) => Err(Error::Deadline),
            result = future => result,
        }
    }
}
