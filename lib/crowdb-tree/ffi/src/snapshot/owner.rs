// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
use bytes::Bytes;

use crate::error::{check, CtError};
use crate::sys;

/// A native chunk whose allocation remains charged through every byte slice.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OwnedSnapshotChunk {
    pub offset: u64,
    pub bytes: Bytes,
    pub done: bool,
}

struct ChunkOwner {
    handle: *mut sys::ct_snapshot_chunk,
    data: *const u8,
    len: usize,
}

// SAFETY: native chunks expose immutable bytes and retain an independent
// allocation owner. Bytes invokes this owner's destructor exactly once.
unsafe impl Send for ChunkOwner {}

impl AsRef<[u8]> for ChunkOwner {
    fn as_ref(&self) -> &[u8] {
        if self.len == 0 {
            &[]
        } else {
            // SAFETY: the native handle retains this immutable byte range.
            unsafe { std::slice::from_raw_parts(self.data, self.len) }
        }
    }
}

impl Drop for ChunkOwner {
    fn drop(&mut self) {
        unsafe { sys::ct_snapshot_chunk_free(self.handle) };
    }
}

pub(super) fn read(export: *mut sys::ct_export, offset: u64) -> Result<OwnedSnapshotChunk, CtError> {
    let mut handle = std::ptr::null_mut();
    let mut done = 0;
    check(unsafe { sys::ct_snapshot_export_next_v2(export, offset, &mut handle, &mut done) })?;
    if handle.is_null() {
        return Err(CtError::Internal);
    }
    let mut owner = ChunkOwner {
        handle,
        data: std::ptr::null(),
        len: 0,
    };
    check(unsafe { sys::ct_snapshot_chunk_data(handle, &mut owner.data, &mut owner.len) })?;
    let budget = crate::owner_budget::OwnerBudget::snapshot(handle)?;
    Ok(OwnedSnapshotChunk {
        offset,
        bytes: budget.wrap(owner)?,
        done: done != 0,
    })
}
