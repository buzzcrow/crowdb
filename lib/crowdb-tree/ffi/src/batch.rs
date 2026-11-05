// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

mod allocation;
pub use allocation::{OwnedAllocation, OwnedOp};

use bytes::Bytes;

use crate::error::check;
use crate::sys;
use crate::tree::Crowdbtree;
use crate::CtError;

/// One record of a multi-key batch passed to [`Crowdbtree::apply_batch`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BatchOp<'a> {
    Put { key: &'a [u8], value: &'a [u8] },
    Delete { key: &'a [u8] },
}

/// One operation for [`Crowdbtree::apply_batch_external`]. The key and value
/// are copied during the call. A `Bytes` slice has unknown backing capacity;
/// its length is never treated as the retained allocation's capacity.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExtOp {
    Put { key: Bytes, value: Bytes },
    Delete { key: Bytes },
}

impl Crowdbtree {
    /// Apply `ops` atomically at `slot` (one call into the C++ engine, so a
    /// concurrent reader never observes a partially-applied batch -- unlike
    /// looping [`Self::apply_put`]/[`Self::apply_delete`] per key).
    pub fn apply_batch(&self, slot: u64, ops: &[BatchOp<'_>]) -> Result<(), CtError> {
        let refs: Vec<sys::ct_kv_ref> = ops
            .iter()
            .map(|op| match op {
                BatchOp::Put { key, value } => sys::ct_kv_ref {
                    key: key.as_ptr(),
                    key_len: key.len(),
                    value: value.as_ptr(),
                    value_len: value.len(),
                    kind: 0,
                },
                BatchOp::Delete { key } => sys::ct_kv_ref {
                    key: key.as_ptr(),
                    key_len: key.len(),
                    value: std::ptr::null(),
                    value_len: 0,
                    kind: 1,
                },
            })
            .collect();
        check(unsafe { sys::ct_apply_batch_slices(self.as_ptr(), slot, refs.as_ptr(), refs.len() as u64) })
    }

    /// Atomically apply operations consuming their `Bytes` owners. Backing
    /// capacity and identity are unknown for arbitrary `Bytes`, so values are
    /// copied into owned cells during this call. This prevents a small slice
    /// from retaining an unaccounted large allocation until a later flush.
    ///
    /// Rust retains the inputs until the synchronous copy returns and then
    /// drops every consumed `Bytes`, including on error.
    pub fn apply_batch_external(&self, slot: u64, ops: Vec<ExtOp>) -> Result<(), CtError> {
        let refs: Vec<sys::ct_kv_ref> = ops
            .iter()
            .map(|op| match op {
                ExtOp::Put { key, value } => sys::ct_kv_ref {
                    key: key.as_ptr(),
                    key_len: key.len(),
                    value: value.as_ptr(),
                    value_len: value.len(),
                    kind: 0,
                },
                ExtOp::Delete { key } => sys::ct_kv_ref {
                    key: key.as_ptr(),
                    key_len: key.len(),
                    value: std::ptr::null(),
                    value_len: 0,
                    kind: 1,
                },
            })
            .collect();
        check(unsafe { sys::ct_apply_batch_slices(self.as_ptr(), slot, refs.as_ptr(), refs.len() as u64) })
    }
}
