// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::ffi::c_void;
use std::ptr::NonNull;

use crate::error::{check, CtError};
use crate::{sys, PageStore};

#[derive(Debug, Clone, Copy)]
pub struct ChunkPageStoreOptions {
    pub tree_id: u64,
    pub owner_epoch: u64,
    pub pack_bytes: usize,
    pub iu_size: u32,
    pub max_concurrent_packs: usize,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ChunkPageStoreStats {
    pub generations_published: u64,
    pub packs_written: u64,
    pub pack_bytes_written: u64,
    pub pack_reads: u64,
    pub cache_hits: u64,
    pub layout_queries: u64,
    pub mirror_write_attempts: u64,
    pub mirror_write_failures: u64,
    pub retained_manifests: u64,
    pub pinned_bytes: u64,
    pub oldest_pin_age_ms: u64,
    pub orphan_bytes: u64,
}

pub struct ChunkRootCatalog {
    ptr: NonNull<sys::ct_root_catalog>,
}

impl ChunkRootCatalog {
    pub fn open_memory(owner_epoch: u64) -> Result<Self, CtError> {
        let mut out = std::ptr::null_mut();
        check(unsafe { sys::ct_memory_root_catalog_open(owner_epoch, &mut out) })?;
        Ok(Self {
            ptr: NonNull::new(out).ok_or(CtError::Internal)?,
        })
    }

    pub fn reclaim_before(&self, tree_id: u64, generation: u64) -> u64 {
        unsafe { sys::ct_root_catalog_reclaim_before(self.ptr.as_ptr(), tree_id, generation) }
    }
}

impl std::fmt::Debug for ChunkRootCatalog {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.debug_struct("ChunkRootCatalog").finish_non_exhaustive()
    }
}

unsafe impl Send for ChunkRootCatalog {}
unsafe impl Sync for ChunkRootCatalog {}

impl Drop for ChunkRootCatalog {
    fn drop(&mut self) {
        unsafe { sys::ct_root_catalog_free(self.ptr.as_ptr()) };
    }
}

#[derive(Debug, Clone, Copy)]
pub struct ChunkRpcRoute {
    pub client: *mut c_void,
    pub server: *mut c_void,
    pub connection: *mut c_void,
}

#[derive(Debug, Clone, Copy)]
pub struct ChunkRpcDiskRoute {
    pub disk_id_high: u64,
    pub disk_id_low: u64,
    pub route: ChunkRpcRoute,
}

pub struct ChunkRpcTransportOptions<'a> {
    pub chunkdb: ChunkRpcRoute,
    pub disk_routes: &'a [ChunkRpcDiskRoute],
    pub writer_lease_ms: u64,
    pub rpc_timeout_ms: u64,
    pub completion_capacity: u32,
}

pub struct ChunkTransport {
    ptr: NonNull<sys::ct_chunk_transport>,
}

impl ChunkTransport {
    /// Create a direct C++ ChunkDB/DiskIO transport.
    ///
    /// # Safety
    ///
    /// Every route must contain matching live `crowdb-rpc` client, server,
    /// and connection handles. Those objects must outlive every page store
    /// opened from this transport.
    pub unsafe fn open_rpc(options: &ChunkRpcTransportOptions<'_>) -> Result<Self, CtError> {
        let disk_routes: Vec<_> = options
            .disk_routes
            .iter()
            .map(|route| sys::ct_chunk_rpc_disk_route {
                disk_id_high: route.disk_id_high,
                disk_id_low: route.disk_id_low,
                route: raw_route(route.route),
            })
            .collect();
        let raw = sys::ct_chunk_rpc_transport_options {
            chunkdb: raw_route(options.chunkdb),
            disk_routes: disk_routes.as_ptr(),
            disk_route_count: disk_routes.len(),
            writer_lease_ms: options.writer_lease_ms,
            rpc_timeout_ms: options.rpc_timeout_ms,
            completion_capacity: options.completion_capacity,
        };
        let mut out = std::ptr::null_mut();
        check(unsafe { sys::ct_rpc_chunk_transport_open(&raw, &mut out) })?;
        Ok(Self {
            ptr: NonNull::new(out).ok_or(CtError::Internal)?,
        })
    }
}

const fn raw_route(route: ChunkRpcRoute) -> sys::ct_chunk_rpc_route {
    sys::ct_chunk_rpc_route {
        client: route.client,
        server: route.server,
        connection: route.connection,
    }
}

impl std::fmt::Debug for ChunkTransport {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.debug_struct("ChunkTransport").finish_non_exhaustive()
    }
}

unsafe impl Send for ChunkTransport {}
unsafe impl Sync for ChunkTransport {}

impl Drop for ChunkTransport {
    fn drop(&mut self) {
        unsafe { sys::ct_chunk_transport_free(self.ptr.as_ptr()) };
    }
}

impl PageStore {
    pub fn open_chunk(
        options: ChunkPageStoreOptions,
        catalog: &ChunkRootCatalog,
        transport: Option<&ChunkTransport>,
    ) -> Result<Self, CtError> {
        let raw = sys::ct_chunk_page_store_options {
            tree_id: options.tree_id,
            owner_epoch: options.owner_epoch,
            pack_bytes: options.pack_bytes,
            iu_size: options.iu_size,
            max_concurrent_packs: options.max_concurrent_packs,
        };
        let mut out = std::ptr::null_mut();
        let status = match transport {
            Some(transport) => unsafe {
                sys::ct_chunk_page_store_open_with_transport(
                    &raw,
                    catalog.ptr.as_ptr(),
                    transport.ptr.as_ptr(),
                    &mut out,
                )
            },
            None => unsafe { sys::ct_chunk_page_store_open(&raw, catalog.ptr.as_ptr(), &mut out) },
        };
        check(status)?;
        Ok(Self {
            ptr: NonNull::new(out).ok_or(CtError::Internal)?,
        })
    }

    pub fn chunk_stats(&self) -> Result<ChunkPageStoreStats, CtError> {
        let mut raw = sys::ct_chunk_page_store_stats::default();
        check(unsafe { sys::ct_chunk_page_store_get_stats(self.ptr.as_ptr(), &mut raw) })?;
        Ok(ChunkPageStoreStats {
            generations_published: raw.generations_published,
            packs_written: raw.packs_written,
            pack_bytes_written: raw.pack_bytes_written,
            pack_reads: raw.pack_reads,
            cache_hits: raw.cache_hits,
            layout_queries: raw.layout_queries,
            mirror_write_attempts: raw.mirror_write_attempts,
            mirror_write_failures: raw.mirror_write_failures,
            retained_manifests: raw.retained_manifests,
            pinned_bytes: raw.pinned_bytes,
            oldest_pin_age_ms: raw.oldest_pin_age_ms,
            orphan_bytes: raw.orphan_bytes,
        })
    }

    pub fn reclaim_chunk_orphans(&self) -> u64 {
        unsafe { sys::ct_chunk_page_store_reclaim_orphans(self.ptr.as_ptr()) }
    }
}
