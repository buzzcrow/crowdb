// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Fixed-slot immutable page cache; collisions replace one slot without locks.

use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};

use arc_swap::ArcSwapOption;
use bytes::Bytes;
use crowdb_protocol::chunk_stream::{stream_directory_page_id, StreamDirectoryPageId, StreamName};

use crate::{Result, StreamError};

pub const DIRECTORY_PAGE_CACHE_SLOTS: usize = 64;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DirectoryCacheSnapshot {
    pub hits: u64,
    pub misses: u64,
    pub replacements: u64,
    pub resident_pages: usize,
    pub resident_bytes: usize,
    pub slot_limit: usize,
}

struct CachedPage {
    stream_name: StreamName,
    id: StreamDirectoryPageId,
    bytes: Bytes,
}

pub(super) struct DirectoryRecordCache {
    slots: [ArcSwapOption<CachedPage>; DIRECTORY_PAGE_CACHE_SLOTS],
    hits: AtomicU64,
    misses: AtomicU64,
    replacements: AtomicU64,
}

impl Default for DirectoryRecordCache {
    fn default() -> Self {
        Self {
            slots: std::array::from_fn(|_| ArcSwapOption::empty()),
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
            replacements: AtomicU64::new(0),
        }
    }
}

impl DirectoryRecordCache {
    pub(super) fn get(&self, name: StreamName, id: StreamDirectoryPageId) -> Option<Bytes> {
        let entry = self.slots[slot(name, id)].load_full();
        if let Some(entry) = entry.filter(|entry| entry.stream_name == name && entry.id == id) {
            self.hits.fetch_add(1, Ordering::Relaxed);
            Some(entry.bytes.clone())
        } else {
            self.misses.fetch_add(1, Ordering::Relaxed);
            None
        }
    }

    pub(super) fn insert(&self, name: StreamName, id: StreamDirectoryPageId, bytes: &[u8]) -> Result<()> {
        if stream_directory_page_id(bytes).map_err(|error| StreamError::Corruption(error.to_string()))? != id
        {
            return Err(StreamError::Corruption(
                "directory page content does not match its key".into(),
            ));
        }
        // Retain only the page bytes, never a larger transport backing allocation.
        let bytes = Bytes::copy_from_slice(bytes);
        let previous = self.slots[slot(name, id)].swap(Some(Arc::new(CachedPage {
            stream_name: name,
            id,
            bytes,
        })));
        if previous.is_some_and(|entry| entry.stream_name != name || entry.id != id) {
            self.replacements.fetch_add(1, Ordering::Relaxed);
        }
        Ok(())
    }

    pub(super) fn snapshot(&self) -> DirectoryCacheSnapshot {
        let mut snapshot = DirectoryCacheSnapshot {
            hits: self.hits.load(Ordering::Relaxed),
            misses: self.misses.load(Ordering::Relaxed),
            replacements: self.replacements.load(Ordering::Relaxed),
            resident_pages: 0,
            resident_bytes: 0,
            slot_limit: DIRECTORY_PAGE_CACHE_SLOTS,
        };
        for slot in &self.slots {
            if let Some(entry) = slot.load_full() {
                snapshot.resident_pages += 1;
                snapshot.resident_bytes += entry.bytes.len();
            }
        }
        snapshot
    }

    pub(super) fn invalidate(&self, name: StreamName, id: StreamDirectoryPageId) {
        self.slots[slot(name, id)].rcu(|entry| match entry {
            Some(page) if page.stream_name == name && page.id == id => None,
            entry => entry.clone(),
        });
    }
}

fn slot(name: StreamName, id: StreamDirectoryPageId) -> usize {
    (usize::from(id.0[0]) ^ usize::from(name.high.to_le_bytes()[0]) ^ usize::from(name.low.to_le_bytes()[0]))
        & (DIRECTORY_PAGE_CACHE_SLOTS - 1)
}
