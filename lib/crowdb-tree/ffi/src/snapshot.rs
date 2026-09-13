// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::os::raw::c_int;
use std::ptr::NonNull;
use std::sync::Arc;

use crate::error::{check, take_buf, CtError};
use crate::scan::ViewEntry;
use crate::sys;
use crate::tree::Crowdbtree;

const DEFAULT_SNAPSHOT_CHUNK_BYTES: usize = 1 << 20;

/// Immutable metadata captured when a portable snapshot export begins.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SnapshotMetadata {
    pub at_slot: u64,
    pub total_bytes: u64,
    pub final_crc32c: u32,
    pub chunk_bytes: usize,
}

/// One sequential chunk returned by a snapshot export session.
#[derive(Debug, PartialEq, Eq)]
pub struct SnapshotChunk {
    pub offset: u64,
    pub bytes: Vec<u8>,
    pub done: bool,
}

/// Non-clone owner of one C snapshot-export handle.
pub struct SnapshotExportSession {
    _tree: Arc<Crowdbtree>,
    ptr: NonNull<sys::ct_export>,
    metadata: SnapshotMetadata,
}

// The C handle is used sequentially by this owner and never shared. The held
// tree Arc keeps its backing engine alive while the session moves to an actor.
unsafe impl Send for SnapshotExportSession {}

impl SnapshotExportSession {
    #[must_use]
    pub const fn metadata(&self) -> SnapshotMetadata {
        self.metadata
    }

    #[must_use]
    pub fn offset(&self) -> u64 {
        unsafe { sys::ct_snapshot_export_offset(self.ptr.as_ptr()) }
    }

    /// Read the next sequential chunk. A source registry may cache this result
    /// to serve an exact retry without advancing the C encoder twice.
    pub fn read(&mut self, offset: u64) -> Result<SnapshotChunk, CtError> {
        let mut chunk = sys::ct_buf {
            data: std::ptr::null_mut(),
            len: 0,
        };
        let mut done: c_int = 0;
        check(unsafe { sys::ct_snapshot_export_next(self.ptr.as_ptr(), offset, &mut chunk, &mut done) })?;
        Ok(SnapshotChunk {
            offset,
            bytes: take_buf(chunk),
            done: done != 0,
        })
    }
}

impl Drop for SnapshotExportSession {
    fn drop(&mut self) {
        unsafe { sys::ct_snapshot_export_end(self.ptr.as_ptr()) };
    }
}

/// Non-clone owner of one C snapshot-import handle.
pub struct SnapshotImportSession {
    _tree: Arc<Crowdbtree>,
    ptr: NonNull<sys::ct_import>,
}

// As with export, the importer is moved between tasks but is only ever
// accessed through its unique mutable owner.
unsafe impl Send for SnapshotImportSession {}

impl SnapshotImportSession {
    pub fn feed(&mut self, chunk: &[u8]) -> Result<(), CtError> {
        check(unsafe { sys::ct_snapshot_import_feed(self.ptr.as_ptr(), chunk.as_ptr(), chunk.len()) })
    }

    pub fn finish(self) -> Result<u64, CtError> {
        let mut at_slot = 0;
        check(unsafe { sys::ct_snapshot_import_finish(self.ptr.as_ptr(), &mut at_slot) })?;
        Ok(at_slot)
    }

    pub fn abort(self) {}
}

impl Drop for SnapshotImportSession {
    fn drop(&mut self) {
        unsafe { sys::ct_snapshot_import_end(self.ptr.as_ptr()) };
    }
}

impl Crowdbtree {
    /// Begin a bounded portable export. The returned owner keeps this tree
    /// alive and closes the C handle exactly once on drop.
    pub fn snapshot_export_begin(
        self: &Arc<Self>,
        chunk_bytes: usize,
    ) -> Result<SnapshotExportSession, CtError> {
        let mut raw: *mut sys::ct_export = std::ptr::null_mut();
        check(unsafe { sys::ct_snapshot_export_begin(self.as_ptr(), chunk_bytes, &mut raw) })?;
        let ptr = NonNull::new(raw).ok_or(CtError::Internal)?;
        let metadata = SnapshotMetadata {
            at_slot: unsafe { sys::ct_snapshot_export_at_slot(ptr.as_ptr()) },
            total_bytes: unsafe { sys::ct_snapshot_export_total_bytes(ptr.as_ptr()) },
            final_crc32c: unsafe { sys::ct_snapshot_export_final_crc32c(ptr.as_ptr()) },
            chunk_bytes: unsafe { sys::ct_snapshot_export_chunk_bytes(ptr.as_ptr()) },
        };
        Ok(SnapshotExportSession {
            _tree: Arc::clone(self),
            ptr,
            metadata,
        })
    }

    /// Begin a portable import. Feeding does not change the tree; finish
    /// verifies the complete stream before installing it.
    pub fn snapshot_import_begin(self: &Arc<Self>) -> Result<SnapshotImportSession, CtError> {
        let mut raw: *mut sys::ct_import = std::ptr::null_mut();
        check(unsafe { sys::ct_snapshot_import_begin(self.as_ptr(), &mut raw) })?;
        Ok(SnapshotImportSession {
            _tree: Arc::clone(self),
            ptr: NonNull::new(raw).ok_or(CtError::Internal)?,
        })
    }

    pub fn snapshot(&self) -> Result<u64, CtError> {
        let mut last = 0u64;
        check(unsafe { sys::ct_snapshot(self.as_ptr(), &mut last) })?;
        Ok(last)
    }

    /// Persists a snapshot and returns `(snapshot generation, applied slot)`.
    pub fn snapshot_info(&self) -> Result<(u64, u64), CtError> {
        let mut generation = 0_u64;
        let mut last_applied = 0_u64;
        check(unsafe { sys::ct_snapshot_info(self.as_ptr(), &mut generation, &mut last_applied) })?;
        Ok((generation, last_applied))
    }

    /// Return the loaded durable generation and frontier without writing a
    /// new snapshot.
    pub fn snapshot_state(&self) -> Result<(u64, u64), CtError> {
        let mut generation = 0;
        let mut last_applied = 0;
        check(unsafe { sys::ct_snapshot_state(self.as_ptr(), &mut generation, &mut last_applied) })?;
        Ok((generation, last_applied))
    }

    /// Run one bounded pass that replaces shared immutable backend objects
    /// with objects owned by this tree's lineage.
    pub fn materialize_ownership(&self) -> Result<(u64, bool), CtError> {
        let mut bytes_written = 0;
        let mut complete = 0;
        check(unsafe { sys::ct_materialize_ownership(self.as_ptr(), &mut bytes_written, &mut complete) })?;
        Ok((bytes_written, complete != 0))
    }

    /// Materialize the durable snapshot view (key-sorted, includes tombstones).
    pub fn snapshot_view(&self) -> Result<(u64, Vec<ViewEntry>), CtError> {
        let mut view: *mut sys::ct_view = std::ptr::null_mut();
        check(unsafe { sys::ct_snapshot_view(self.as_ptr(), &mut view) })?;
        let at = unsafe { sys::ct_view_at_slot(view) };
        let mut it: *mut sys::ct_iter = std::ptr::null_mut();
        let rc = unsafe { sys::ct_view_iter(view, &mut it) };
        if rc != 0 {
            unsafe { sys::ct_view_release(view) };
            return Err(check(rc).unwrap_err());
        }
        let mut out = Vec::new();
        loop {
            let mut key = sys::ct_buf {
                data: std::ptr::null_mut(),
                len: 0,
            };
            let mut value = sys::ct_buf {
                data: std::ptr::null_mut(),
                len: 0,
            };
            let mut slot = 0u64;
            let mut kind = 0u8;
            let mut valid: c_int = 0;
            let rc = unsafe { sys::ct_iter_next(it, &mut key, &mut slot, &mut kind, &mut value, &mut valid) };
            if rc != 0 {
                unsafe {
                    sys::ct_iter_release(it);
                    sys::ct_view_release(view);
                }
                return Err(check(rc).unwrap_err());
            }
            let k = take_buf(key);
            let v = take_buf(value);
            if valid == 0 {
                break;
            }
            out.push(ViewEntry {
                key: k,
                slot,
                tombstone: kind == 1,
                value: v,
            });
        }
        unsafe {
            sys::ct_iter_release(it);
            sys::ct_view_release(view);
        }
        Ok((at, out))
    }

    /// Export the current durable snapshot as the portable byte stream
    /// (concatenated chunks). The snapshot's slot is carried in the stream.
    pub fn snapshot_export(&self) -> Result<Vec<u8>, CtError> {
        let mut exp: *mut sys::ct_export = std::ptr::null_mut();
        check(unsafe {
            sys::ct_snapshot_export_begin(self.as_ptr(), DEFAULT_SNAPSHOT_CHUNK_BYTES, &mut exp)
        })?;
        let mut stream = Vec::new();
        let mut offset = 0;
        loop {
            let mut chunk = sys::ct_buf {
                data: std::ptr::null_mut(),
                len: 0,
            };
            let mut done: c_int = 0;
            let rc = unsafe { sys::ct_snapshot_export_next(exp, offset, &mut chunk, &mut done) };
            if rc != 0 {
                unsafe { sys::ct_snapshot_export_end(exp) };
                return Err(check(rc).unwrap_err());
            }
            let bytes = take_buf(chunk);
            offset = offset.saturating_add(bytes.len() as u64);
            stream.extend_from_slice(&bytes);
            if done != 0 {
                break;
            }
        }
        unsafe { sys::ct_snapshot_export_end(exp) };
        Ok(stream)
    }

    /// Import a portable snapshot stream, replacing this engine's state.
    pub fn snapshot_import(&self, stream: &[u8]) -> Result<u64, CtError> {
        let mut im: *mut sys::ct_import = std::ptr::null_mut();
        check(unsafe { sys::ct_snapshot_import_begin(self.as_ptr(), &mut im) })?;
        let rc = unsafe { sys::ct_snapshot_import_feed(im, stream.as_ptr(), stream.len()) };
        if rc != 0 {
            unsafe { sys::ct_snapshot_import_end(im) };
            return Err(check(rc).unwrap_err());
        }
        let mut at = 0u64;
        let rc = unsafe { sys::ct_snapshot_import_finish(im, &mut at) };
        unsafe { sys::ct_snapshot_import_end(im) };
        check(rc)?;
        Ok(at)
    }
}
