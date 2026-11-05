// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
use super::{
    catalog_store_ref, error_code,
    protection::{CandidateStatus, ManifestCandidate, StageProtectionCatalog, MAX_CANDIDATE_MANIFEST_BYTES},
    stage::{StageId, MAX_STAGE_SCAN},
    sys, CtError,
};
use std::ffi::c_void;
mod mutation;

fn internal(id: sys::ct_chunk_stage_id) -> StageId {
    StageId {
        owner_epoch: id.owner_epoch,
        operation_high: id.operation_high,
        operation_low: id.operation_low,
    }
}
fn external(id: StageId) -> sys::ct_chunk_stage_id {
    sys::ct_chunk_stage_id {
        owner_epoch: id.owner_epoch,
        operation_high: id.operation_high,
        operation_low: id.operation_low,
    }
}
fn status(value: CandidateStatus) -> sys::ct_chunk_candidate_status {
    sys::ct_chunk_candidate_status {
        id: external(value.id),
        generation: value.generation,
        reference_count: value.reference_count,
        prepared_references: value.prepared_references,
        prepared_windows: value.prepared_windows,
        released_windows: value.released_windows,
        state: value.state as u32,
    }
}
unsafe fn with_catalog(
    context: *mut c_void,
    operation: impl FnOnce(&dyn StageProtectionCatalog) -> Result<(), CtError>,
) -> i32 {
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        if context.is_null() {
            return Err(CtError::InvalidArgument);
        }
        let catalog = catalog_store_ref(context)
            .stage_protection()
            .ok_or(CtError::NotSupported)?;
        operation(catalog)
    }))
    .unwrap_or(Err(CtError::Internal))
    .map_or_else(error_code, |()| 0)
}
pub(super) fn callbacks() -> sys::ct_root_catalog_protection_callbacks {
    sys::ct_root_catalog_protection_callbacks {
        version: 1,
        struct_bytes: std::mem::size_of::<sys::ct_root_catalog_protection_callbacks>()
            .try_into()
            .expect("callback layout fits u32"),
        capabilities: 1,
        check_reader: Some(check_reader),
        writer_quiesced: Some(writer_quiesced),
        begin: Some(begin),
        load: Some(load),
        list: Some(list),
        prepare: Some(mutation::prepare),
        finish: Some(mutation::finish),
        release: Some(mutation::release),
        pin: Some(mutation::pin),
        prune: Some(mutation::prune),
    }
}
unsafe extern "C" fn check_reader(context: *mut c_void) -> i32 {
    with_catalog(context, |catalog| catalog.check_read_owner())
}
unsafe extern "C" fn writer_quiesced(context: *mut c_void, tree: u64, epoch: u64) -> i32 {
    with_catalog(context, |catalog| catalog.writer_quiesced(tree, epoch))
}
unsafe extern "C" fn begin(
    context: *mut c_void,
    tree: u64,
    id: sys::ct_chunk_stage_id,
    expected_generation: u64,
    reference_count: u64,
    reference_checksum: u32,
    manifest: *const u8,
    length: usize,
) -> i32 {
    with_catalog(context, |catalog| {
        if manifest.is_null() || length == 0 || length > MAX_CANDIDATE_MANIFEST_BYTES {
            return Err(CtError::InvalidArgument);
        }
        let candidate = ManifestCandidate {
            id: internal(id),
            expected_generation,
            reference_count,
            reference_checksum,
            manifest: std::slice::from_raw_parts(manifest, length).to_vec(),
        };
        candidate.validate()?;
        catalog.begin_manifest_candidate(tree, &candidate)
    })
}
unsafe extern "C" fn load(
    context: *mut c_void,
    tree: u64,
    id: sys::ct_chunk_stage_id,
    out: *mut sys::ct_chunk_candidate_status,
) -> i32 {
    with_catalog(context, |catalog| {
        if out.is_null() {
            return Err(CtError::InvalidArgument);
        }
        *out = status(
            catalog
                .load_manifest_candidate(tree, internal(id))?
                .ok_or(CtError::NotFound)?,
        );
        Ok(())
    })
}
unsafe extern "C" fn list(
    context: *mut c_void,
    tree: u64,
    after: *const sys::ct_chunk_stage_id,
    limit: u32,
    out: *mut sys::ct_chunk_candidate_status,
    length: *mut usize,
) -> i32 {
    with_catalog(context, |catalog| {
        if out.is_null()
            || length.is_null()
            || !(1..=MAX_STAGE_SCAN).contains(&limit)
            || *length < limit as usize
        {
            return Err(CtError::InvalidArgument);
        }
        let records = catalog.list_manifest_candidates(tree, after.as_ref().copied().map(internal), limit)?;
        if records.len() > limit as usize {
            return Err(CtError::Corruption);
        }
        for (index, value) in records.iter().copied().enumerate() {
            *out.add(index) = status(value);
        }
        *length = records.len();
        Ok(())
    })
}
