// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{StreamChunkRecordSnapshot, StreamName};
use arc_swap::ArcSwapOption;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

const SLOTS: usize = 32;
const SLOT_BYTES: usize = 1024 * 1024;

pub(super) struct RecordCache {
    slots: [ArcSwapOption<StreamChunkRecordSnapshot>; SLOTS],
    next: AtomicUsize,
}

impl Default for RecordCache {
    fn default() -> Self {
        Self {
            slots: std::array::from_fn(|_| ArcSwapOption::empty()),
            next: AtomicUsize::new(0),
        }
    }
}

impl RecordCache {
    pub(super) fn find(&self, id: StreamName, offset: u64) -> Option<StreamChunkRecordSnapshot> {
        self.slots.iter().find_map(|slot| {
            let record = slot.load_full()?;
            (record.record.stream_id == id
                && record.record.logical_start.is_some_and(|start| start <= offset)
                && record.record.logical_end.is_some_and(|end| offset < end))
            .then(|| record.as_ref().clone())
        })
    }
    pub(super) fn insert(&self, record: StreamChunkRecordSnapshot) {
        if record.record.mappings.encoded_len() + record.index.resident_bytes() > SLOT_BYTES {
            return;
        }
        self.slots[self.next.fetch_add(1, Ordering::Relaxed) % SLOTS].store(Some(Arc::new(record)));
    }
}
