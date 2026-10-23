// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use crate::admission::{Budget, Credit};
use crate::buffer::AllocationRegistry;
use crate::{Cancellation, CapacityKind, Error, OperationContext, Result};

#[derive(Clone, Copy, Debug)]
pub struct RuntimeLimits {
    pub requests: usize,
    pub producer_bytes: usize,
    pub held_bytes: usize,
}

impl Default for RuntimeLimits {
    fn default() -> Self {
        Self {
            requests: 1024,
            producer_bytes: 64 << 20,
            held_bytes: 256 << 20,
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub struct ResourceStatus {
    pub closed: bool,
    pub requests: usize,
    pub producer_bytes: usize,
    pub producer_bytes_high_water: usize,
    pub held_bytes: usize,
    pub held_bytes_high_water: usize,
    pub compatibility_copy_bytes: u64,
}

pub struct RuntimeResources {
    pub(crate) producers: Arc<Budget>,
    pub(crate) held: Arc<Budget>,
    requests: Arc<Budget>,
    pub(crate) allocations: Arc<AllocationRegistry>,
    pub(crate) compatibility_copy_bytes: AtomicU64,
    pub(crate) closing: Cancellation,
}

impl RuntimeResources {
    /// Builds shared hard limits. No background worker or waiting queue is created.
    /// # Errors
    /// Rejects zero, overflowing or excessive request admission.
    pub fn new(limits: RuntimeLimits) -> Result<Arc<Self>> {
        if limits.requests == 0
            || limits.requests > 1 << 20
            || limits.producer_bytes == 0
            || limits.held_bytes == 0
            || limits.producer_bytes > isize::MAX as usize
            || limits.held_bytes > isize::MAX as usize
        {
            return Err(Error::Configuration("invalid request or memory limits".into()));
        }
        Ok(Arc::new(Self {
            requests: Budget::new(limits.requests, CapacityKind::Requests),
            producers: Budget::new(limits.producer_bytes, CapacityKind::Producers),
            held: Budget::new(limits.held_bytes, CapacityKind::HeldBuffers),
            closing: Cancellation::default(),
            allocations: Arc::new(AllocationRegistry::default()),
            compatibility_copy_bytes: AtomicU64::new(0),
        }))
    }

    /// # Errors
    /// Rejects closed runtime, expired/cancelled work or exhausted request slots.
    pub fn admit(self: &Arc<Self>, context: OperationContext) -> Result<RequestLease> {
        context.check()?;
        self.check_open()?;
        let credit = self.requests.charge(1)?;
        self.check_open()?;
        Ok(RequestLease {
            resources: Arc::clone(self),
            context,
            _credit: credit,
        })
    }

    pub fn shutdown(&self) {
        self.closing.cancel();
    }

    pub(crate) fn check_open(&self) -> Result<()> {
        if self.closing.is_cancelled() {
            Err(Error::Shutdown)
        } else {
            Ok(())
        }
    }

    #[must_use]
    pub fn status(&self) -> ResourceStatus {
        ResourceStatus {
            closed: self.closing.is_cancelled(),
            requests: self.requests.used(),
            producer_bytes: self.producers.used(),
            producer_bytes_high_water: self.producers.high_water(),
            held_bytes: self.held.used(),
            held_bytes_high_water: self.held.high_water(),
            compatibility_copy_bytes: self.compatibility_copy_bytes.load(Ordering::Relaxed),
        }
    }
}

/// Request admission lasts through a stream's terminal result or cancellation.
/// Its release is independent of any buffer leases delivered to the caller.
pub struct RequestLease {
    resources: Arc<RuntimeResources>,
    context: OperationContext,
    _credit: Credit,
}

impl RequestLease {
    #[must_use]
    pub fn context(&self) -> &OperationContext {
        &self.context
    }

    /// # Errors
    /// Returns shutdown, cancellation, deadline or the underlying operation error.
    pub async fn run<T>(&self, future: impl Future<Output = Result<T>>) -> Result<T> {
        self.resources.check_open()?;
        tokio::select! {
            biased;
            () = self.resources.closing.cancelled() => Err(Error::Shutdown),
            result = self.context.run(future) => result,
        }
    }

    /// Preserves uncertainty if cancellation interrupts an accepted mutation.
    /// There is no implicit replay of the supplied future.
    /// # Errors
    /// Returns definite pre-admission errors, semantic failures or uncertainty.
    pub async fn run_mutation<T>(&self, future: impl Future<Output = Result<T>>) -> Result<T> {
        use crate::error::MutationInterruption;
        self.context.check()?;
        self.resources.check_open()?;
        self.run(future).await.map_err(|error| {
            let reason = match error {
                Error::Cancelled => MutationInterruption::Cancelled,
                Error::Deadline => MutationInterruption::Deadline,
                Error::Shutdown => MutationInterruption::Shutdown,
                other => return other,
            };
            Error::UncertainMutation {
                request_id: self.context.request_id.get(),
                reason,
            }
        })
    }
}
