// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::Buffer;
use crate::sys;
use bytes::{Bytes, OwnerAllocationLease};
use std::alloc::Layout;
use std::ffi::c_void;
use std::mem::ManuallyDrop;

/// An immutable buffer could not admit its complete owner/control storage.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OwnedBytesError {
    PayloadTooLarge,
    BudgetExhausted,
    AllocationFailed,
}
impl std::fmt::Display for OwnedBytesError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::PayloadTooLarge => "owned RPC payload exceeds its wire length",
            Self::BudgetExhausted => "owned RPC control budget exhausted",
            Self::AllocationFailed => "owned RPC control allocation failed",
        })
    }
}
impl std::error::Error for OwnedBytesError {}

struct OwnedBytes {
    bytes: Bytes,
    allocation: ManuallyDrop<Option<OwnerAllocationLease>>,
}

impl Buffer {
    /// Wrap bytes and charge every new native/Rust control allocation to the
    /// immutable owner's budget, when present. Refund follows actual free,
    /// including the Rust owner Box. Ordinary input Bytes remain caller-owned.
    /// # Errors
    /// Returns a length, budget, or allocation error without retaining input.
    pub fn try_from_owned_bytes(data: Bytes) -> Result<Self, OwnedBytesError> {
        if data.is_empty() {
            return Ok(Self::from_raw(std::ptr::null_mut()));
        }
        let len = u32::try_from(data.len()).map_err(|_| OwnedBytesError::PayloadTooLarge)?;
        let allocation = data
            .try_owner_allocation(Layout::new::<OwnedBytes>())
            .map_err(|_| OwnedBytesError::BudgetExhausted)?;
        let accounted = allocation.is_some();
        let mut owner = Box::new(OwnedBytes {
            bytes: data,
            allocation: ManuallyDrop::new(allocation),
        });
        if let Some(allocation) = owner.allocation.as_mut() {
            allocation.allocated();
        }
        let data_ptr = owner.bytes.as_ptr();
        let context = Box::into_raw(owner).cast::<c_void>();
        let allocator = sys::crowdb_rpc_control_allocator_v1 {
            version: 1,
            context,
            claim: Some(claim),
            record: Some(record),
            release: Some(release),
        };
        let handle = unsafe {
            sys::crowdb_rpc_buffer_create_external_accounted(
                data_ptr,
                len,
                Some(free_owner),
                context,
                if accounted {
                    &raw const allocator
                } else {
                    std::ptr::null()
                },
            )
        };
        if handle.is_null() {
            unsafe { free_owner(context) };
            return Err(OwnedBytesError::AllocationFailed);
        }
        Ok(Self::from_raw(handle))
    }
}

unsafe extern "C" fn free_owner(context: *mut c_void) {
    let mut owner = unsafe { Box::from_raw(context.cast::<OwnedBytes>()) };
    let allocation = unsafe { ManuallyDrop::take(&mut owner.allocation) };
    // The lease itself retains Bytes: native budget/context survive both the
    // contained Bytes drop and the actual Box deallocation below.
    drop(owner);
    drop(allocation);
}
unsafe extern "C" fn claim(context: *mut c_void, size: usize, align: usize) -> i32 {
    let Ok(layout) = Layout::from_size_align(size, align) else {
        return 0;
    };
    let owner = unsafe { &*context.cast::<OwnedBytes>() };
    i32::from(
        owner
            .bytes
            .owner_allocation()
            .is_some_and(|budget| budget.claim(layout).is_ok()),
    )
}
unsafe extern "C" fn record(context: *mut c_void, size: usize, align: usize, acquired: i32) {
    let Ok(layout) = Layout::from_size_align(size, align) else {
        return;
    };
    let owner = unsafe { &*context.cast::<OwnedBytes>() };
    if let Some(budget) = owner.bytes.owner_allocation() {
        budget.record(layout, acquired != 0);
    }
}
unsafe extern "C" fn release(context: *mut c_void, size: usize, align: usize) {
    let Ok(layout) = Layout::from_size_align(size, align) else {
        return;
    };
    let owner = unsafe { &*context.cast::<OwnedBytes>() };
    if let Some(budget) = owner.bytes.owner_allocation() {
        budget.release(layout);
    }
}
