// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crate::{error::check, sys, Crowdbtree, CtError};
use std::ops::Range;
use std::ptr::NonNull;

/// Immutable slice of an allocation admitted at its complete `Vec` capacity.
/// Clones and slices share one payload charge, including spare capacity. The
/// native account survives the Tree without retaining its CPU executor.
#[derive(Debug)]
pub struct OwnedAllocation {
    owner: NonNull<sys::ct_owned_allocation>,
    data: NonNull<u8>,
    offset: usize,
    len: usize,
    capacity: usize,
}

impl OwnedAllocation {
    /// Complete backing allocation capacity, not this slice's length.
    pub fn capacity(&self) -> usize {
        self.capacity
    }
    pub fn len(&self) -> usize {
        self.len
    }
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Return an initialized subrange without allocating another control block.
    pub fn slice(&self, range: Range<usize>) -> Result<Self, CtError> {
        if range.start > range.end || range.end > self.len {
            return Err(CtError::InvalidArgument);
        }
        let mut result = self.clone();
        result.offset += range.start;
        result.len = range.end - range.start;
        Ok(result)
    }

    /// Tests identity even when the visible byte ranges differ.
    pub fn shares_allocation(&self, other: &Self) -> bool {
        self.owner == other.owner
    }
}

impl Clone for OwnedAllocation {
    fn clone(&self) -> Self {
        unsafe { sys::ct_owned_allocation_retain(self.owner.as_ptr()) };
        Self {
            owner: self.owner,
            data: self.data,
            offset: self.offset,
            len: self.len,
            capacity: self.capacity,
        }
    }
}
impl Drop for OwnedAllocation {
    fn drop(&mut self) {
        unsafe { sys::ct_owned_allocation_free(self.owner.as_ptr()) };
    }
}
impl AsRef<[u8]> for OwnedAllocation {
    fn as_ref(&self) -> &[u8] {
        // The complete immutable Vec and its initialized prefix remain alive
        // under this handle. Every slice is checked against that prefix.
        unsafe { std::slice::from_raw_parts(self.data.as_ptr().add(self.offset), self.len) }
    }
}
// Native reference counting is atomic, and the foreign Vec is never mutated.
unsafe impl Send for OwnedAllocation {}
unsafe impl Sync for OwnedAllocation {}

/// Borrowed keys and retained complete-allocation values for one atomic batch.
#[derive(Debug)]
pub enum OwnedOp<'a> {
    Put {
        key: &'a [u8],
        value: &'a OwnedAllocation,
    },
    Delete {
        key: &'a [u8],
    },
}

unsafe extern "C" fn drop_vec(context: *mut std::ffi::c_void) {
    drop(unsafe { Box::from_raw(context.cast::<Vec<u8>>()) });
}

impl Crowdbtree {
    /// Admit a complete `Vec`, charging its real capacity exactly once. Every
    /// failure consumes and drops the input.
    pub fn own_vec(&self, value: Vec<u8>) -> Result<OwnedAllocation, CtError> {
        let len = value.len();
        let capacity = value.capacity();
        let data = NonNull::new(value.as_ptr().cast_mut()).ok_or(CtError::Internal)?;
        let context = Box::into_raw(Box::new(value)).cast();
        let mut out = std::ptr::null_mut();
        // C consumes context on every return and also charges Box<Vec>'s size.
        check(unsafe {
            sys::ct_owned_allocation_create(
                self.as_ptr(),
                data.as_ptr(),
                len,
                capacity,
                drop_vec,
                context,
                std::mem::size_of::<Vec<u8>>(),
                &mut out,
            )
        })?;
        Ok(OwnedAllocation {
            owner: NonNull::new(out).ok_or(CtError::Internal)?,
            data,
            offset: 0,
            len,
            capacity,
        })
    }

    /// Borrow allocation owners for an atomic write. Values within one host
    /// retain their existing allocation; a different host performs a budgeted
    /// copy. The caller may retain, reuse, or cancel its owners independently.
    pub fn apply_batch_owned(&self, slot: u64, operations: &[OwnedOp<'_>]) -> Result<(), CtError> {
        check(unsafe {
            sys::ct_apply_batch_owned_reader(
                self.as_ptr(),
                slot,
                operations.len(),
                read_operation,
                std::ptr::from_ref(&operations).cast(),
            )
        })
    }
}

unsafe extern "C" fn read_operation(context: *const std::ffi::c_void, index: usize) -> sys::ct_owned_kv_ref {
    // The call is synchronous. Native code only visits indexes below the
    // supplied count, and this borrowed slice stays alive for the whole call.
    let operations = unsafe { &*context.cast::<&[OwnedOp<'_>]>() };
    match &operations[index] {
        OwnedOp::Put { key, value } => sys::ct_owned_kv_ref {
            key: key.as_ptr(),
            key_len: key.len(),
            owner: value.owner.as_ptr(),
            offset: value.offset,
            length: value.len,
            kind: 0,
        },
        OwnedOp::Delete { key } => sys::ct_owned_kv_ref {
            key: key.as_ptr(),
            key_len: key.len(),
            owner: std::ptr::null_mut(),
            offset: 0,
            length: 0,
            kind: 1,
        },
    }
}
