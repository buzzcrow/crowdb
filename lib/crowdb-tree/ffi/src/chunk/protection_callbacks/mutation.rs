// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
use super::{external, internal, sys, with_catalog, CtError};
use crate::chunk::protection::MAX_REFERENCE_WINDOW;
use crate::chunk::stage::{StageId, MAX_STAGE_SCAN};
use std::ffi::c_void;

pub(super) unsafe extern "C" fn prepare(
    context: *mut c_void,
    tree: u64,
    id: sys::ct_chunk_stage_id,
    window: u64,
    references: *const sys::ct_chunk_stage_id,
    length: usize,
) -> i32 {
    with_catalog(context, |catalog| {
        if references.is_null() || !(1..=MAX_REFERENCE_WINDOW).contains(&length) {
            return Err(CtError::InvalidArgument);
        }
        let mut values = [StageId::default(); MAX_REFERENCE_WINDOW];
        for (slot, value) in values
            .iter_mut()
            .zip(std::slice::from_raw_parts(references, length))
        {
            *slot = internal(*value);
        }
        catalog.prepare_manifest_references(tree, internal(id), window, &values[..length])
    })
}
pub(super) unsafe extern "C" fn finish(
    context: *mut c_void,
    tree: u64,
    id: sys::ct_chunk_stage_id,
    action: u32,
) -> i32 {
    with_catalog(context, |catalog| match action {
        1 => catalog.commit_manifest_candidate(tree, internal(id)),
        2 => catalog.abort_manifest_candidate(tree, internal(id)),
        3 => catalog.recover_manifest_candidate(tree, internal(id)),
        _ => Err(CtError::InvalidArgument),
    })
}
pub(super) unsafe extern "C" fn release(
    context: *mut c_void,
    tree: u64,
    id: sys::ct_chunk_stage_id,
    window: u64,
) -> i32 {
    with_catalog(context, |catalog| {
        catalog.release_manifest_references(tree, internal(id), window)
    })
}
pub(super) unsafe extern "C" fn pin(
    context: *mut c_void,
    tree: u64,
    stage: sys::ct_chunk_stage_id,
    pin: sys::ct_chunk_stage_id,
    acquire: u8,
) -> i32 {
    with_catalog(context, |catalog| match acquire {
        0 => catalog.unpin_stage(tree, internal(stage), internal(pin)),
        1 => catalog.pin_stage(tree, internal(stage), internal(pin)),
        _ => Err(CtError::InvalidArgument),
    })
}
pub(super) unsafe extern "C" fn prune(
    context: *mut c_void,
    tree: u64,
    stage: sys::ct_chunk_stage_id,
    after: *const sys::ct_chunk_stage_id,
    limit: u32,
    out: *mut sys::ct_chunk_pin_progress,
) -> i32 {
    with_catalog(context, |catalog| {
        if out.is_null() || !(1..=MAX_STAGE_SCAN).contains(&limit) {
            return Err(CtError::InvalidArgument);
        }
        let result = catalog.prune_stage_pins(
            tree,
            internal(stage),
            after.as_ref().copied().map(internal),
            limit,
        )?;
        *out = sys::ct_chunk_pin_progress {
            after: external(result.after.unwrap_or_default()),
            examined: result.examined,
            released: result.released,
            has_after: u8::from(result.after.is_some()),
            more: u8::from(result.more),
        };
        Ok(())
    })
}
