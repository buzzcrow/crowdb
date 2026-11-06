// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
//! Shared host runtime storage with exact immutable pack identities.

use super::{ChunkRootCatalog, ChunkTransport};
use crate::{error::check, sys, CtError, PageStore, TreeRuntime};
use std::ptr::NonNull;
use std::sync::Arc;

#[derive(Debug, Clone, Copy)]
pub struct ChunkRuntimeOptions {
    pub tree_id: u64,
    pub owner_epoch: u64,
    /// Zero opens current; a nonzero value opens only that retained root.
    pub open_generation: u64,
    pub iu_size: u32,
    pub mirror_copies: u32,
    pub max_chunk_bytes: u64,
}

impl Default for ChunkRuntimeOptions {
    fn default() -> Self {
        Self {
            tree_id: 0,
            owner_epoch: 0,
            open_generation: 0,
            iu_size: 64 * 1024,
            mirror_copies: 2,
            max_chunk_bytes: 256 * 1024 * 1024,
        }
    }
}

impl PageStore {
    /// Create a paid facade without opening storage or starting a private pool.
    /// Inject it into a `Config` with the same shared `TreeRuntime`. The catalog
    /// must already be bound to the host `ChunkPinReleaseService`. Each catalog
    /// claim is one writer incarnation and cannot be reopened after close.
    ///
    /// # Errors
    /// Rejects unsupported fixed-pack/protection capabilities, invalid geometry,
    /// or insufficient host credit. Actual root validation happens at Tree open.
    pub fn open_chunk_runtime(
        runtime: &TreeRuntime,
        options: ChunkRuntimeOptions,
        catalog: Arc<ChunkRootCatalog>,
        transport: &ChunkTransport,
    ) -> Result<Self, CtError> {
        let raw = sys::ct_chunk_runtime_options {
            version: 1,
            struct_bytes: u32::try_from(std::mem::size_of::<sys::ct_chunk_runtime_options>())
                .map_err(|_| CtError::InvalidArgument)?,
            tree_id: options.tree_id,
            owner_epoch: options.owner_epoch,
            open_generation: options.open_generation,
            iu_size: options.iu_size,
            mirror_copies: options.mirror_copies,
            max_chunk_bytes: options.max_chunk_bytes,
        };
        let mut out = std::ptr::null_mut();
        check(unsafe {
            sys::ct_chunk_runtime_page_store_open(
                runtime.as_ptr(),
                &raw,
                catalog.ptr.as_ptr(),
                transport.ptr.as_ptr(),
                &mut out,
            )
        })?;
        Ok(Self {
            ptr: NonNull::new(out).ok_or(CtError::Internal)?,
            chunk_catalog: Some(catalog),
        })
    }
}
