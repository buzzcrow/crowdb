// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::stage::{
    decode_stage_list, encode_stage_list, StageId, StageRecord, MAX_STAGE_BYTES, MAX_STAGE_SCAN,
};
use super::{catalog_store_ref, check, error_code, sys, ChunkRootCatalog, CtError};
use std::ffi::c_void;

fn external(id: StageId) -> sys::ct_chunk_stage_id {
    sys::ct_chunk_stage_id {
        owner_epoch: id.owner_epoch,
        operation_high: id.operation_high,
        operation_low: id.operation_low,
    }
}
fn internal(id: sys::ct_chunk_stage_id) -> StageId {
    StageId {
        owner_epoch: id.owner_epoch,
        operation_high: id.operation_high,
        operation_low: id.operation_low,
    }
}
impl ChunkRootCatalog {
    /// Persistent stage-record schema support; not a complete writeback capability.
    pub fn supports_staging(&self) -> bool {
        unsafe { sys::ct_root_catalog_stage_capabilities(self.ptr.as_ptr()) & 1 != 0 }
    }
    pub fn load_stage(&self, tree_id: u64, id: StageId) -> Result<Option<StageRecord>, CtError> {
        let mut out = sys::ct_buf {
            data: std::ptr::null_mut(),
            len: 0,
        };
        let status =
            unsafe { sys::ct_root_catalog_stage_load(self.ptr.as_ptr(), tree_id, external(id), &mut out) };
        let bytes = take_blob(status, out)?;
        bytes.map(|bytes| StageRecord::decode(&bytes)).transpose()
    }
    pub fn compare_stage(
        &self,
        tree_id: u64,
        owner_epoch: u64,
        expected: Option<&StageRecord>,
        next: Option<&StageRecord>,
    ) -> Result<(), CtError> {
        let expected = expected.map(StageRecord::encode).transpose()?.unwrap_or_default();
        let next = next.map(StageRecord::encode).transpose()?.unwrap_or_default();
        check(unsafe {
            sys::ct_root_catalog_stage_compare(
                self.ptr.as_ptr(),
                tree_id,
                owner_epoch,
                expected.as_ptr(),
                expected.len(),
                next.as_ptr(),
                next.len(),
            )
        })
    }
    pub fn list_stages(
        &self,
        tree_id: u64,
        after: Option<StageId>,
        limit: u32,
    ) -> Result<Vec<StageRecord>, CtError> {
        let cursor = after.map(external);
        let mut out = sys::ct_buf {
            data: std::ptr::null_mut(),
            len: 0,
        };
        let status = unsafe {
            sys::ct_root_catalog_stage_list(
                self.ptr.as_ptr(),
                tree_id,
                cursor.as_ref().map_or(std::ptr::null(), |id| id),
                limit,
                &mut out,
            )
        };
        decode_stage_list(&take_blob(status, out)?.ok_or(CtError::Corruption)?)
    }
}
fn take_blob(status: i32, mut out: sys::ct_buf) -> Result<Option<Vec<u8>>, CtError> {
    let result = if status == -1 {
        Ok(None)
    } else {
        check(status).and_then(|()| {
            if out.data.is_null() || out.len > 4 + MAX_STAGE_SCAN as usize * (4 + MAX_STAGE_BYTES) {
                return Err(CtError::Corruption);
            }
            Ok(Some(
                unsafe { std::slice::from_raw_parts(out.data, out.len) }.to_vec(),
            ))
        })
    };
    unsafe { sys::ct_free_buf(&mut out) };
    result
}

pub(super) fn callbacks() -> sys::ct_root_catalog_staging_callbacks {
    sys::ct_root_catalog_staging_callbacks {
        version: 1,
        struct_bytes: std::mem::size_of::<sys::ct_root_catalog_staging_callbacks>() as u32,
        capabilities: 1,
        load: Some(load),
        compare: Some(compare),
        list: Some(list),
    }
}
unsafe fn output(bytes: Vec<u8>, out: *mut *const u8, length: *mut usize) -> i32 {
    let bytes = bytes.into_boxed_slice();
    *length = bytes.len();
    *out = Box::into_raw(bytes).cast::<u8>();
    0
}
unsafe fn optional(data: *const u8, length: usize) -> Result<Option<StageRecord>, CtError> {
    if length == 0 {
        return Ok(None);
    }
    if data.is_null() || length > MAX_STAGE_BYTES {
        return Err(CtError::InvalidArgument);
    }
    StageRecord::decode(std::slice::from_raw_parts(data, length)).map(Some)
}
unsafe extern "C" fn load(
    context: *mut c_void,
    tree_id: u64,
    id: sys::ct_chunk_stage_id,
    out: *mut *const u8,
    length: *mut usize,
) -> i32 {
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        if context.is_null() || out.is_null() || length.is_null() {
            return -2;
        }
        match catalog_store_ref(context).load_stage(tree_id, internal(id)) {
            Ok(Some(record)) => match record.encode() {
                Ok(bytes) => output(bytes, out, length),
                Err(error) => error_code(error),
            },
            Ok(None) => -1,
            Err(error) => error_code(error),
        }
    }))
    .unwrap_or(-6)
}
unsafe extern "C" fn compare(
    context: *mut c_void,
    tree_id: u64,
    owner_epoch: u64,
    expected: *const u8,
    expected_length: usize,
    next: *const u8,
    next_length: usize,
) -> i32 {
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        if context.is_null() {
            return -2;
        }
        let result = (|| {
            let expected = optional(expected, expected_length)?;
            let next = optional(next, next_length)?;
            catalog_store_ref(context).compare_stage(tree_id, owner_epoch, expected.as_ref(), next.as_ref())
        })();
        result.map_or_else(error_code, |()| 0)
    }))
    .unwrap_or(-6)
}
unsafe extern "C" fn list(
    context: *mut c_void,
    tree_id: u64,
    after: *const sys::ct_chunk_stage_id,
    limit: u32,
    out: *mut *const u8,
    length: *mut usize,
) -> i32 {
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        if context.is_null() || out.is_null() || length.is_null() || !(1..=MAX_STAGE_SCAN).contains(&limit) {
            return -2;
        }
        let result = catalog_store_ref(context)
            .list_stages(tree_id, after.as_ref().copied().map(internal), limit)
            .and_then(|records| encode_stage_list(&records));
        match result {
            Ok(bytes) => output(bytes, out, length),
            Err(error) => error_code(error),
        }
    }))
    .unwrap_or(-6)
}
