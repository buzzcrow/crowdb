// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
use std::ptr::NonNull;
use std::sync::{Arc, Weak};

use crate::error::check;
use crate::reactor::{drive_ct_future, FutureGuard, FutureKind};
use crate::{sys, BatchOp, Crowdbtree, CtError, ExtOp};

/// An owned, charged copy of a complete batch. Preparation holds no source
/// writer, epoch guard, or Tree close ticket, and may be cancelled by dropping it.
/// Arm immediately before consensus may choose a slot for this proposal.
pub struct PreparedApply {
    raw: NonNull<sys::ct_apply_permit>,
    tree: Weak<Crowdbtree>,
}

// SAFETY: this opaque handle is exclusively owned. Native prepare/arm/submit
// permit movement between threads but require serialized access to one handle.
unsafe impl Send for PreparedApply {}

/// A generation-pinned proposal. Dropping it before submit cancels admission;
/// after submit, the accepted operation drains even if its future is dropped.
pub struct ArmedApply {
    // Release the native permit before dropping the last possible Tree owner.
    permit: PreparedApply,
    tree: Arc<Crowdbtree>,
}

impl Crowdbtree {
    /// Copy and admit a complete local proposal before choosing its slot.
    /// Resource pressure is reported before consensus accepts the proposal.
    pub fn prepare_proposal(self: &Arc<Self>, ops: &[BatchOp<'_>]) -> Result<PreparedApply, CtError> {
        self.prepare_in_partition(ops, false)
    }

    /// Copy a follower or replay batch using its independent progress partition.
    pub fn prepare_replay(self: &Arc<Self>, ops: &[BatchOp<'_>]) -> Result<PreparedApply, CtError> {
        self.prepare_in_partition(ops, true)
    }

    fn prepare_in_partition(
        self: &Arc<Self>,
        ops: &[BatchOp<'_>],
        gap_repair: bool,
    ) -> Result<PreparedApply, CtError> {
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
        let mut raw = std::ptr::null_mut();
        check(unsafe {
            sys::ct_apply_prepare(
                self.as_ptr(),
                refs.as_ptr(),
                refs.len(),
                i32::from(gap_repair),
                &mut raw,
            )
        })?;
        Ok(PreparedApply {
            raw: NonNull::new(raw).ok_or(CtError::Internal)?,
            tree: Arc::downgrade(self),
        })
    }
}

impl PreparedApply {
    /// Pin this prepared generation before a proposal can become chosen.
    /// Generation replacement or Tree closure makes an old permit unavailable.
    pub fn arm(self) -> Result<ArmedApply, CtError> {
        let tree = self.tree.upgrade().ok_or(CtError::Unavailable)?;
        check(unsafe { sys::ct_apply_arm(self.raw.as_ptr()) })?;
        Ok(ArmedApply { permit: self, tree })
    }
}

impl Drop for PreparedApply {
    fn drop(&mut self) {
        unsafe { sys::ct_apply_permit_free(self.raw.as_ptr()) };
    }
}

impl ArmedApply {
    /// Consume this admission for exactly one chosen slot. CPU and budget
    /// continuations run in the Tree runtime; this future only awaits completion.
    pub async fn apply(self, slot: u64) -> Result<(), CtError> {
        let guard = {
            let mut raw = std::ptr::null_mut();
            check(unsafe { sys::ct_apply_submit(self.permit.raw.as_ptr(), slot, &mut raw) })?;
            FutureGuard(raw)
        };
        drive_ct_future(guard, &self.tree, FutureKind::Flush).await?;
        Ok(())
    }
}

// Native retains this bridge while reading the immutable caller input. Its Box
// allocation is charged separately from the controlled native batch copy.
struct ReplayOwner {
    ops: Vec<ExtOp>,
}

unsafe extern "C" fn read_replay(context: *mut std::ffi::c_void, index: usize, out: *mut sys::ct_kv_ref) {
    // SAFETY: native retains this box, serializes reads, and uses index < count.
    let owner = unsafe { &*context.cast::<ReplayOwner>() };
    let value = match &owner.ops[index] {
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
    };
    // SAFETY: native supplies an initialized writable ct_kv_ref for this call.
    unsafe { out.write(value) };
}

unsafe extern "C" fn release_replay(context: *mut std::ffi::c_void) {
    // SAFETY: ct_apply_replay consumes one Box and invokes this callback once.
    drop(unsafe { Box::from_raw(context.cast::<ReplayOwner>()) });
}

impl Crowdbtree {
    /// Apply one already-chosen follower or WAL batch. High slots retain the
    /// immutable input while the missing contiguous slot owns the gap reserve.
    /// Native quota waits and staged writeback continue if this future drops.
    pub async fn apply_replay(self: &Arc<Self>, slot: u64, operations: Vec<ExtOp>) -> Result<(), CtError> {
        let guard = {
            let count = operations.len();
            let context = Box::into_raw(Box::new(ReplayOwner { ops: operations })).cast();
            let mut raw = std::ptr::null_mut();
            check(unsafe {
                sys::ct_apply_replay(
                    self.as_ptr(),
                    slot,
                    count,
                    read_replay,
                    release_replay,
                    context,
                    std::mem::size_of::<ReplayOwner>(),
                    &mut raw,
                )
            })?;
            let raw = NonNull::new(raw).ok_or(CtError::Internal)?;
            FutureGuard(raw.as_ptr())
        };
        drive_ct_future(guard, self, FutureKind::Flush).await?;
        Ok(())
    }
}
