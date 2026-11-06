// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use bytes::Bytes;

use crate::error::{check, CtError};
use crate::reactor::{FutureGuard, RawOutcome};
use crate::sys;

struct ScanOwner {
    handle: *mut sys::ct_scan_result,
    data: *const u8,
    len: usize,
}

// SAFETY: the owner exposes immutable bytes and is destroyed once after all
// Bytes clones/slices drop; native allocation accounting is thread-safe.
unsafe impl Send for ScanOwner {}

impl AsRef<[u8]> for ScanOwner {
    fn as_ref(&self) -> &[u8] {
        if self.len == 0 {
            &[]
        } else {
            // SAFETY: ct_scan_result_data ties these bytes to the live handle.
            unsafe { std::slice::from_raw_parts(self.data, self.len) }
        }
    }
}

impl Drop for ScanOwner {
    fn drop(&mut self) {
        unsafe { sys::ct_scan_result_free(self.handle) };
    }
}

pub(crate) fn take(handle: *mut sys::ct_scan_result) -> Result<(Bytes, usize, bool), CtError> {
    if handle.is_null() {
        return Err(CtError::Internal);
    }
    let mut owner = ScanOwner {
        handle,
        data: std::ptr::null(),
        len: 0,
    };
    let mut count = 0;
    let mut truncated = 0;
    check(unsafe {
        sys::ct_scan_result_data(
            handle,
            &mut owner.data,
            &mut owner.len,
            &mut count,
            &mut truncated,
        )
    })?;
    let count = usize::try_from(count).map_err(|_| CtError::Corruption)?;
    let budget = crate::owner_budget::OwnerBudget::scan(handle)?;
    let bytes = budget.wrap(owner)?;
    Ok((bytes, count, truncated != 0))
}

pub(crate) fn poll(guard: &mut FutureGuard) -> Option<Result<RawOutcome, CtError>> {
    let mut done = 0;
    let mut result = std::ptr::null_mut();
    let status = unsafe { sys::ct_future_poll_scan_v2(guard.0, &mut done, &mut result) };
    if done == 0 {
        return None;
    }
    guard.0 = std::ptr::null_mut();
    Some(check(status).and_then(|()| {
        let (packed, count, truncated) = take(result)?;
        Ok(RawOutcome {
            found: truncated,
            slot: count as u64,
            value: Vec::new(),
            packed: Some(packed),
        })
    }))
}

pub(crate) fn scan(
    tree: *mut sys::ct_tree,
    options: &sys::ct_scan_options_v2,
) -> Result<(Vec<super::ScanEntry>, bool), CtError> {
    let mut result = std::ptr::null_mut();
    check(unsafe { sys::ct_scan_v2(tree, options, &mut result) })?;
    let (packed, count, truncated) = take(result)?;
    Ok((super::decode_scan_bytes(packed, count)?, truncated))
}

pub(crate) fn start(tree: *mut sys::ct_tree, options: &super::ScanOptions) -> Result<FutureGuard, CtError> {
    let start = options.start.as_deref().unwrap_or_default();
    let raw = sys::ct_scan_options_v2 {
        struct_size: std::mem::size_of::<sys::ct_scan_options_v2>(),
        prefix: options.prefix.as_ptr(),
        prefix_len: options.prefix.len(),
        start: start.as_ptr(),
        start_len: start.len(),
        end: options.end.as_ptr(),
        end_len: options.end.len(),
        begin: options.begin.as_ptr(),
        begin_len: options.begin.len(),
        limit: options.limit,
        byte_budget: options.byte_budget,
        deadline_ms: options.deadline_ms,
        reverse: i32::from(options.direction == super::ScanDirection::Reverse),
        keys_only: i32::from(options.keys_only),
        include_tombstones: i32::from(options.include_tombstones),
        has_start_bound: i32::from(options.start.is_some()),
        start_inclusive: i32::from(options.start_inclusive),
        strict_byte_budget: i32::from(options.strict_byte_budget),
    };
    let mut future = std::ptr::null_mut();
    check(unsafe { sys::ct_scan_async_v2(tree, &raw, &mut future) })?;
    if future.is_null() {
        return Err(CtError::Internal);
    }
    Ok(FutureGuard(future))
}
