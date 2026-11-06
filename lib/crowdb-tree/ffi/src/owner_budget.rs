// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
use std::alloc::Layout;

use crate::error::{check, CtError};
use crate::sys;

pub(crate) struct OwnerBudget(*mut sys::ct_owned_budget);

// SAFETY: native budgets retain independent ownership and use atomic admission
// and accounting. The unique opaque handle is freed once after all byte clones.
unsafe impl Send for OwnerBudget {}
unsafe impl Sync for OwnerBudget {}

impl OwnerBudget {
    pub(crate) fn wrap<T: AsRef<[u8]> + Send + 'static>(self, owner: T) -> Result<bytes::Bytes, CtError> {
        if self.0.is_null() {
            Ok(bytes::Bytes::from_owner(owner))
        } else {
            bytes::Bytes::try_from_owner_accounted(owner, self)
        }
    }

    pub(crate) fn future(handle: *mut sys::ct_future) -> Result<Self, CtError> {
        let mut budget = std::ptr::null_mut();
        check(unsafe { sys::ct_future_owned_budget(handle, &mut budget) })?;
        Ok(Self(budget))
    }
    pub(crate) fn scan(handle: *mut sys::ct_scan_result) -> Result<Self, CtError> {
        let mut budget = std::ptr::null_mut();
        check(unsafe { sys::ct_scan_result_owned_budget(handle, &mut budget) })?;
        Ok(Self(budget))
    }
    pub(crate) fn snapshot(handle: *mut sys::ct_snapshot_chunk) -> Result<Self, CtError> {
        let mut budget = std::ptr::null_mut();
        check(unsafe { sys::ct_snapshot_chunk_owned_budget(handle, &mut budget) })?;
        Ok(Self(budget))
    }
}
impl bytes::OwnerAllocation for OwnerBudget {
    type Error = CtError;
    fn claim(&self, layout: Layout) -> Result<(), CtError> {
        check(unsafe { sys::ct_owned_budget_claim(self.0, layout.size()) })
    }
    fn record(&self, layout: Layout, acquired: bool) {
        unsafe { sys::ct_owned_budget_record(self.0, layout.size(), i32::from(acquired)) };
    }
    fn release(&self, layout: Layout) {
        unsafe { sys::ct_owned_budget_release(self.0, layout.size()) };
    }
}
impl Drop for OwnerBudget {
    fn drop(&mut self) {
        unsafe { sys::ct_owned_budget_free(self.0) };
    }
}
