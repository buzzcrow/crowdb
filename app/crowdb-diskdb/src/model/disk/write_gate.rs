// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Lock-free administrative closure and durable-write quiescence.

use std::sync::atomic::{AtomicU64, Ordering};

const CLOSED: u64 = 1 << 63;
const UNCERTAIN: u64 = 1 << 62;

#[derive(Default)]
pub struct DiskWriteGate(AtomicU64);

impl DiskWriteGate {
    /// Reserve a write before any asynchronous persistence. Closing and entry
    /// share one CAS word, so no new permit can race past exclusion.
    pub fn enter(&self) -> Option<DiskWritePermit<'_>> {
        self.0
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |state| {
                (state < UNCERTAIN - 1).then_some(state + 1)
            })
            .ok()
            .map(|_| DiskWritePermit {
                gate: self,
                confirmed: false,
            })
    }

    pub fn close(&self) {
        self.0.fetch_or(CLOSED, Ordering::AcqRel);
    }

    pub fn closed(&self) -> bool {
        self.0.load(Ordering::Acquire) & CLOSED != 0
    }

    /// An acknowledgement may only follow this exact closed, drained state.
    pub fn quiescent(&self) -> bool {
        self.0.load(Ordering::Acquire) == CLOSED
    }
}

pub struct DiskWritePermit<'a> {
    gate: &'a DiskWriteGate,
    confirmed: bool,
}

impl DiskWritePermit<'_> {
    /// Only a definite persistence outcome releases the quiescence obligation.
    pub fn complete(mut self) {
        self.confirmed = true;
    }
}

impl Drop for DiskWritePermit<'_> {
    fn drop(&mut self) {
        if !self.confirmed {
            self.gate.0.fetch_or(CLOSED | UNCERTAIN, Ordering::AcqRel);
        }
        self.gate.0.fetch_sub(1, Ordering::Release);
    }
}
