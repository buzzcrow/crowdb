// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Lock-free reservation-capacity admission and durable usage rebuild.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use crate::metrics::ChunkdbMetrics;

pub struct ReservationAdmission {
    max_blocks: AtomicU64,
    max_bytes: AtomicU64,
    blocks: AtomicU64,
    bytes: AtomicU64,
    metrics: Option<Arc<ChunkdbMetrics>>,
}

pub struct ReservationPermit {
    admission: Arc<ReservationAdmission>,
    blocks: u64,
    bytes: u64,
    retained: bool,
}

impl ReservationAdmission {
    pub fn new(max_blocks: u64, max_bytes: u64, metrics: Option<Arc<ChunkdbMetrics>>) -> Self {
        Self {
            max_blocks: AtomicU64::new(max_blocks),
            max_bytes: AtomicU64::new(max_bytes),
            blocks: AtomicU64::new(0),
            bytes: AtomicU64::new(0),
            metrics,
        }
    }

    pub fn update_limits(&self, max_blocks: u64, max_bytes: u64) {
        self.max_blocks.store(max_blocks, Ordering::Release);
        self.max_bytes.store(max_bytes, Ordering::Release);
    }

    pub fn rebuild(&self, blocks: u64, bytes: u64) {
        let previous_blocks = self.blocks.swap(blocks, Ordering::AcqRel);
        let previous_bytes = self.bytes.swap(bytes, Ordering::AcqRel);
        self.adjust_gauges(previous_blocks, previous_bytes, blocks, bytes);
    }

    pub fn try_acquire(self: &Arc<Self>, blocks: u64, bytes: u64) -> Option<ReservationPermit> {
        if !reserve(&self.blocks, blocks, self.max_blocks.load(Ordering::Acquire)) {
            self.reject();
            return None;
        }
        if !reserve(&self.bytes, bytes, self.max_bytes.load(Ordering::Acquire)) {
            release(&self.blocks, blocks);
            self.reject();
            return None;
        }
        self.adjust_gauges(0, 0, blocks, bytes);
        Some(ReservationPermit {
            admission: Arc::clone(self),
            blocks,
            bytes,
            retained: false,
        })
    }

    pub fn release(&self, blocks: u64, bytes: u64) {
        let released_blocks = release(&self.blocks, blocks);
        let released_bytes = release(&self.bytes, bytes);
        self.adjust_gauges(released_blocks, released_bytes, 0, 0);
    }

    pub fn usage(&self) -> (u64, u64) {
        (
            self.blocks.load(Ordering::Acquire),
            self.bytes.load(Ordering::Acquire),
        )
    }

    fn reject(&self) {
        if let Some(metrics) = &self.metrics {
            metrics.reservation_rejections.inc();
        }
    }

    fn adjust_gauges(&self, old_blocks: u64, old_bytes: u64, blocks: u64, bytes: u64) {
        if let Some(metrics) = &self.metrics {
            metrics
                .reservation_blocks
                .inc_by(blocks.saturating_sub(old_blocks));
            metrics
                .reservation_blocks
                .dec_by(old_blocks.saturating_sub(blocks));
            metrics.reservation_bytes.inc_by(bytes.saturating_sub(old_bytes));
            metrics.reservation_bytes.dec_by(old_bytes.saturating_sub(bytes));
        }
    }
}

impl ReservationPermit {
    pub fn retain(mut self) {
        self.retained = true;
    }
}

impl Drop for ReservationPermit {
    fn drop(&mut self) {
        if !self.retained {
            self.admission.release(self.blocks, self.bytes);
        }
    }
}

fn reserve(counter: &AtomicU64, delta: u64, limit: u64) -> bool {
    counter
        .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
            current.checked_add(delta).filter(|next| *next <= limit)
        })
        .is_ok()
}

fn release(counter: &AtomicU64, delta: u64) -> u64 {
    counter
        .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
            Some(current.saturating_sub(delta))
        })
        .map_or(0, |previous| previous.min(delta))
}

impl Drop for ReservationAdmission {
    fn drop(&mut self) {
        let blocks = *self.blocks.get_mut();
        let bytes = *self.bytes.get_mut();
        self.adjust_gauges(blocks, bytes, 0, 0);
    }
}
