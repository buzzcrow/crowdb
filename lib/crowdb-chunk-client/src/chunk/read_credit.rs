// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Atomic credits retained through the last HTTP-owned buffer view.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use bytes::Bytes;
use tokio::sync::Notify;

use crate::metrics::ReadFlowMetrics;

pub(super) struct ReadBudget {
    limit: usize,
    used: AtomicUsize,
    waiters: AtomicUsize,
    pub wake: Notify,
    metrics: Arc<ReadFlowMetrics>,
}

impl ReadBudget {
    pub fn new(limit: usize, metrics: Arc<ReadFlowMetrics>) -> Self {
        Self {
            limit,
            used: AtomicUsize::new(0),
            waiters: AtomicUsize::new(0),
            wake: Notify::new(),
            metrics,
        }
    }

    fn try_charge(&self, amount: usize) -> bool {
        let accepted = self
            .used
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |used| {
                used.checked_add(amount).filter(|next| *next <= self.limit)
            })
            .is_ok();
        if accepted {
            self.metrics.stream_bytes_reserved.inc_by(amount as u64);
        }
        accepted
    }

    fn release(&self, amount: usize) {
        self.used.fetch_sub(amount, Ordering::AcqRel);
        self.metrics.stream_bytes_released.inc_by(amount as u64);
        if self.waiters.load(Ordering::Acquire) != 0 {
            self.wake.notify_waiters();
        }
    }

    pub fn register(self: &Arc<Self>) -> WaitRegistration {
        self.waiters.fetch_add(1, Ordering::AcqRel);
        WaitRegistration(Arc::clone(self))
    }
}

pub(super) struct WaitRegistration(Arc<ReadBudget>);

impl Drop for WaitRegistration {
    fn drop(&mut self) {
        self.0.waiters.fetch_sub(1, Ordering::AcqRel);
    }
}

pub(super) struct StreamSlots {
    limit: usize,
    used: AtomicUsize,
}

impl StreamSlots {
    pub fn new(limit: usize) -> Self {
        Self {
            limit,
            used: AtomicUsize::new(0),
        }
    }

    pub fn try_reserve(self: &Arc<Self>, global: &Arc<ReadBudget>, bytes: usize) -> Option<Arc<ReadLease>> {
        if self
            .used
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |used| {
                used.checked_add(1).filter(|next| *next <= self.limit)
            })
            .is_err()
        {
            return None;
        }
        if !global.try_charge(bytes) {
            self.used.fetch_sub(1, Ordering::AcqRel);
            return None;
        }
        Some(Arc::new(ReadLease {
            slots: Arc::clone(self),
            global: Arc::clone(global),
            bytes,
        }))
    }
}

pub(super) struct ReadLease {
    slots: Arc<StreamSlots>,
    global: Arc<ReadBudget>,
    bytes: usize,
}

impl Drop for ReadLease {
    fn drop(&mut self) {
        self.slots.used.fetch_sub(1, Ordering::AcqRel);
        self.global.release(self.bytes);
    }
}

struct LeasedView {
    data: Bytes,
    _lease: Arc<ReadLease>,
}

impl AsRef<[u8]> for LeasedView {
    fn as_ref(&self) -> &[u8] {
        &self.data
    }
}

pub(super) fn retain(data: Bytes, lease: Arc<ReadLease>) -> Bytes {
    Bytes::from_owner(LeasedView { data, _lease: lease })
}
