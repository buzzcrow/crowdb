// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::ffi::c_void;
use std::ptr::NonNull;
use std::sync::Arc;

use crate::error::{check, CtError};
use crate::{sys, PageStore};

#[cfg(feature = "chunk-rpc")]
use crowdb_rpc_ffi::OwnedClientRoute;

#[derive(Debug, Clone, Copy)]
pub struct ChunkPageStoreOptions {
    pub tree_id: u64,
    pub owner_epoch: u64,
    pub pack_bytes: usize,
    pub iu_size: u32,
    pub max_concurrent_packs: usize,
    pub materialization_bytes_per_pass: u64,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ChunkPageStoreStats {
    pub generations_published: u64,
    pub packs_written: u64,
    pub pack_bytes_written: u64,
    pub packs_reused: u64,
    pub pack_bytes_reused: u64,
    pub pack_reads: u64,
    pub cache_hits: u64,
    pub layout_queries: u64,
    pub mirror_write_attempts: u64,
    pub mirror_write_failures: u64,
    pub retained_manifests: u64,
    pub pinned_bytes: u64,
    pub oldest_pin_age_ms: u64,
    pub orphan_bytes: u64,
    pub materialization_passes: u64,
    pub materialization_failures: u64,
    pub materialization_packs_written: u64,
    pub materialization_bytes_written: u64,
    pub shared_packs: u64,
    pub rpc_operations: u64,
    pub rpc_latency_ns: u64,
    pub diskio_operations: u64,
    pub diskio_latency_ns: u64,
    pub coalesced_reads: u64,
    pub coalesced_read_bytes: u64,
    pub completion_wakeups: u64,
    pub materialization_scan_bytes: u64,
    pub shared_metadata_segments: u64,
    pub materialized_metadata_segments: u64,
    pub manifest_publication_latency_ns: u64,
    pub recovery_latency_ns: u64,
}

pub struct ChunkRootCatalog {
    ptr: NonNull<sys::ct_root_catalog>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RootCatalogObject {
    CurrentManifest,
    Manifest(u64),
    ReferenceSegment(u64),
}

/// Synchronous persistence contract used only by tree checkpoint and
/// maintenance paths. Implementations may bridge to an asynchronous service
/// on a dedicated worker; tree point reads never call this interface.
pub trait RootCatalogStore: Send + Sync + 'static {
    fn load(&self, tree_id: u64, object: RootCatalogObject) -> Result<Option<Vec<u8>>, CtError>;
    fn store(&self, tree_id: u64, object: RootCatalogObject, data: &[u8]) -> Result<(), CtError>;
    fn publish(
        &self,
        tree_id: u64,
        expected_generation: u64,
        owner_epoch: u64,
        generation: u64,
        manifest: &[u8],
    ) -> Result<(), CtError>;
    fn allocate_reference_segment_id(&self, tree_id: u64) -> Result<u64, CtError>;
    fn discard_reference_segments(&self, _tree_id: u64, _object_ids: &[u64]) -> u64 {
        0
    }
    fn reclaim_before(&self, _tree_id: u64, _generation: u64) -> u64 {
        0
    }
}

impl ChunkRootCatalog {
    pub fn open_memory(owner_epoch: u64) -> Result<Self, CtError> {
        let mut out = std::ptr::null_mut();
        check(unsafe { sys::ct_memory_root_catalog_open(owner_epoch, &mut out) })?;
        Ok(Self {
            ptr: NonNull::new(out).ok_or(CtError::Internal)?,
        })
    }

    /// Opens a catalog whose opaque manifests are persisted by `store`.
    ///
    /// # Errors
    ///
    /// Returns an invalid callback configuration error.
    pub fn open_callback(store: Arc<dyn RootCatalogStore>) -> Result<Self, CtError> {
        let context = Box::into_raw(Box::new(store)).cast::<c_void>();
        let callbacks = sys::ct_root_catalog_callbacks {
            load: Some(catalog_load),
            free_blob: Some(catalog_free_blob),
            store: Some(catalog_store),
            publish: Some(catalog_publish),
            allocate_reference_segment_id: Some(catalog_allocate_reference_segment_id),
            discard_reference_segments: Some(catalog_discard_reference_segments),
            reclaim_before: Some(catalog_reclaim_before),
            drop_context: Some(catalog_drop_context),
        };
        let mut out = std::ptr::null_mut();
        if let Err(error) =
            check(unsafe { sys::ct_callback_root_catalog_open(&callbacks, context, &mut out) })
        {
            unsafe { drop(Box::from_raw(context.cast::<Arc<dyn RootCatalogStore>>())) };
            return Err(error);
        }
        Ok(Self {
            ptr: NonNull::new(out).ok_or(CtError::Internal)?,
        })
    }

    pub fn reclaim_before(&self, tree_id: u64, generation: u64) -> u64 {
        unsafe { sys::ct_root_catalog_reclaim_before(self.ptr.as_ptr(), tree_id, generation) }
    }
}

fn catalog_object(kind: i32, object_id: u64) -> Result<RootCatalogObject, CtError> {
    match kind {
        1 => Ok(RootCatalogObject::CurrentManifest),
        2 => Ok(RootCatalogObject::Manifest(object_id)),
        3 => Ok(RootCatalogObject::ReferenceSegment(object_id)),
        _ => Err(CtError::InvalidArgument),
    }
}

fn error_code(error: CtError) -> i32 {
    match error {
        CtError::NotFound => -1,
        CtError::InvalidArgument => -2,
        CtError::Corruption => -3,
        CtError::IoError => -4,
        CtError::NotSupported => -5,
        CtError::Internal => -6,
        CtError::ResourceExhausted => -7,
        CtError::Unavailable | CtError::Unknown(_) => -8,
    }
}

unsafe fn catalog_store_ref<'a>(context: *mut c_void) -> &'a Arc<dyn RootCatalogStore> {
    &*context.cast::<Arc<dyn RootCatalogStore>>()
}

unsafe extern "C" fn catalog_load(
    context: *mut c_void,
    kind: i32,
    tree_id: u64,
    object_id: u64,
    out: *mut *const u8,
    len: *mut usize,
) -> i32 {
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        if context.is_null() || out.is_null() || len.is_null() {
            return -2;
        }
        let object = match catalog_object(kind, object_id) {
            Ok(object) => object,
            Err(error) => return error_code(error),
        };
        match catalog_store_ref(context).load(tree_id, object) {
            Ok(Some(bytes)) if !bytes.is_empty() => {
                let bytes = bytes.into_boxed_slice();
                *len = bytes.len();
                *out = Box::into_raw(bytes).cast::<u8>();
                0
            }
            Ok(_) => -1,
            Err(error) => error_code(error),
        }
    }))
    .unwrap_or(-6)
}

unsafe extern "C" fn catalog_free_blob(_context: *mut c_void, data: *const u8, len: usize) {
    if !data.is_null() {
        drop(Box::from_raw(std::ptr::slice_from_raw_parts_mut(
            data.cast_mut(),
            len,
        )));
    }
}

unsafe extern "C" fn catalog_store(
    context: *mut c_void,
    kind: i32,
    tree_id: u64,
    object_id: u64,
    data: *const u8,
    len: usize,
) -> i32 {
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        if context.is_null() || (data.is_null() && len != 0) {
            return -2;
        }
        let object = match catalog_object(kind, object_id) {
            Ok(object) => object,
            Err(error) => return error_code(error),
        };
        let bytes = if len == 0 {
            &[]
        } else {
            std::slice::from_raw_parts(data, len)
        };
        catalog_store_ref(context)
            .store(tree_id, object, bytes)
            .map_or_else(error_code, |()| 0)
    }))
    .unwrap_or(-6)
}

unsafe extern "C" fn catalog_publish(
    context: *mut c_void,
    tree_id: u64,
    expected_generation: u64,
    owner_epoch: u64,
    generation: u64,
    data: *const u8,
    len: usize,
) -> i32 {
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        if context.is_null() || (data.is_null() && len != 0) {
            return -2;
        }
        catalog_store_ref(context)
            .publish(
                tree_id,
                expected_generation,
                owner_epoch,
                generation,
                if len == 0 {
                    &[]
                } else {
                    std::slice::from_raw_parts(data, len)
                },
            )
            .map_or_else(error_code, |()| 0)
    }))
    .unwrap_or(-6)
}

unsafe extern "C" fn catalog_allocate_reference_segment_id(
    context: *mut c_void,
    tree_id: u64,
    out: *mut u64,
) -> i32 {
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        if context.is_null() || out.is_null() {
            return -2;
        }
        match catalog_store_ref(context).allocate_reference_segment_id(tree_id) {
            Ok(value) => {
                *out = value;
                0
            }
            Err(error) => error_code(error),
        }
    }))
    .unwrap_or(-6)
}

unsafe extern "C" fn catalog_discard_reference_segments(
    context: *mut c_void,
    tree_id: u64,
    object_ids: *const u64,
    object_count: usize,
) -> u64 {
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        if context.is_null() || (object_ids.is_null() && object_count != 0) {
            return 0;
        }
        let ids = if object_count == 0 {
            &[]
        } else {
            std::slice::from_raw_parts(object_ids, object_count)
        };
        catalog_store_ref(context).discard_reference_segments(tree_id, ids)
    }))
    .unwrap_or(0)
}

unsafe extern "C" fn catalog_reclaim_before(context: *mut c_void, tree_id: u64, generation: u64) -> u64 {
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        if context.is_null() {
            return 0;
        }
        catalog_store_ref(context).reclaim_before(tree_id, generation)
    }))
    .unwrap_or(0)
}

unsafe extern "C" fn catalog_drop_context(context: *mut c_void) {
    if !context.is_null() {
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            drop(Box::from_raw(context.cast::<Arc<dyn RootCatalogStore>>()));
        }));
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

#[cfg(feature = "chunk-rpc")]
#[derive(Debug, Clone)]
pub struct OwnedChunkRpcDiskRoute {
    pub disk_id_high: u64,
    pub disk_id_low: u64,
    pub route: OwnedClientRoute,
}

#[cfg(feature = "chunk-rpc")]
#[derive(Debug)]
pub struct OwnedChunkRpcTransportOptions {
    pub chunkdb: OwnedClientRoute,
    pub disk_routes: Vec<OwnedChunkRpcDiskRoute>,
    pub writer_lease_ms: u64,
    pub rpc_timeout_ms: u64,
    pub completion_capacity: u32,
}

#[cfg(feature = "chunk-rpc")]
#[derive(Debug)]
struct OwnedTransportRoutes {
    _chunkdb: OwnedClientRoute,
    _disk_routes: Vec<OwnedChunkRpcDiskRoute>,
}

pub struct ChunkTransport {
    ptr: NonNull<sys::ct_chunk_transport>,
    #[cfg(feature = "chunk-rpc")]
    _routes: Option<OwnedTransportRoutes>,
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
            #[cfg(feature = "chunk-rpc")]
            _routes: None,
        })
    }

    /// Create a direct C++ transport while retaining all crowdb-rpc owners.
    #[cfg(feature = "chunk-rpc")]
    pub fn open_owned_rpc(options: OwnedChunkRpcTransportOptions) -> Result<Self, CtError> {
        let chunkdb = owned_raw_route(&options.chunkdb);
        let disk_routes: Vec<_> = options
            .disk_routes
            .iter()
            .map(|route| ChunkRpcDiskRoute {
                disk_id_high: route.disk_id_high,
                disk_id_low: route.disk_id_low,
                route: owned_raw_route(&route.route),
            })
            .collect();
        let raw_options = ChunkRpcTransportOptions {
            chunkdb,
            disk_routes: &disk_routes,
            writer_lease_ms: options.writer_lease_ms,
            rpc_timeout_ms: options.rpc_timeout_ms,
            completion_capacity: options.completion_capacity,
        };
        let mut transport = unsafe { Self::open_rpc(&raw_options) }?;
        transport._routes = Some(OwnedTransportRoutes {
            _chunkdb: options.chunkdb,
            _disk_routes: options.disk_routes,
        });
        Ok(transport)
    }
}

#[cfg(feature = "chunk-rpc")]
fn owned_raw_route(route: &OwnedClientRoute) -> ChunkRpcRoute {
    let (client, server, connection) = route.raw_handles();
    ChunkRpcRoute {
        client,
        server,
        connection,
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
        catalog: Arc<ChunkRootCatalog>,
        transport: Option<&ChunkTransport>,
    ) -> Result<Self, CtError> {
        let raw = sys::ct_chunk_page_store_options {
            tree_id: options.tree_id,
            owner_epoch: options.owner_epoch,
            pack_bytes: options.pack_bytes,
            iu_size: options.iu_size,
            max_concurrent_packs: options.max_concurrent_packs,
            materialization_bytes_per_pass: options.materialization_bytes_per_pass,
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
            chunk_catalog: Some(catalog),
        })
    }

    pub fn chunk_stats(&self) -> Result<ChunkPageStoreStats, CtError> {
        let mut raw = sys::ct_chunk_page_store_stats::default();
        check(unsafe { sys::ct_chunk_page_store_get_stats(self.ptr.as_ptr(), &mut raw) })?;
        Ok(ChunkPageStoreStats {
            generations_published: raw.generations_published,
            packs_written: raw.packs_written,
            pack_bytes_written: raw.pack_bytes_written,
            packs_reused: raw.packs_reused,
            pack_bytes_reused: raw.pack_bytes_reused,
            pack_reads: raw.pack_reads,
            cache_hits: raw.cache_hits,
            layout_queries: raw.layout_queries,
            mirror_write_attempts: raw.mirror_write_attempts,
            mirror_write_failures: raw.mirror_write_failures,
            retained_manifests: raw.retained_manifests,
            pinned_bytes: raw.pinned_bytes,
            oldest_pin_age_ms: raw.oldest_pin_age_ms,
            orphan_bytes: raw.orphan_bytes,
            materialization_passes: raw.materialization_passes,
            materialization_failures: raw.materialization_failures,
            materialization_packs_written: raw.materialization_packs_written,
            materialization_bytes_written: raw.materialization_bytes_written,
            shared_packs: raw.shared_packs,
            rpc_operations: raw.rpc_operations,
            rpc_latency_ns: raw.rpc_latency_ns,
            diskio_operations: raw.diskio_operations,
            diskio_latency_ns: raw.diskio_latency_ns,
            coalesced_reads: raw.coalesced_reads,
            coalesced_read_bytes: raw.coalesced_read_bytes,
            completion_wakeups: raw.completion_wakeups,
            materialization_scan_bytes: raw.materialization_scan_bytes,
            shared_metadata_segments: raw.shared_metadata_segments,
            materialized_metadata_segments: raw.materialized_metadata_segments,
            manifest_publication_latency_ns: raw.manifest_publication_latency_ns,
            recovery_latency_ns: raw.recovery_latency_ns,
        })
    }

    /// Sets the WAL byte offset that the next chunk-root publication makes
    /// recoverable. Non-chunk stores accept the hint without persisting it.
    ///
    /// # Errors
    ///
    /// Returns an invalid-argument, availability, or corruption error.
    pub fn set_wal_replay_offset(&self, offset: u64) -> Result<(), CtError> {
        check(unsafe { sys::ct_chunk_page_store_set_wal_replay_offset(self.ptr.as_ptr(), offset) })
    }

    /// Returns the WAL replay offset persisted with the current chunk root.
    /// Legacy and non-chunk stores return zero.
    ///
    /// # Errors
    ///
    /// Returns an availability or corruption error when the current root
    /// cannot be validated.
    pub fn wal_replay_offset(&self) -> Result<u64, CtError> {
        let mut offset = 0;
        check(unsafe { sys::ct_chunk_page_store_get_wal_replay_offset(self.ptr.as_ptr(), &mut offset) })?;
        Ok(offset)
    }

    pub fn reclaim_chunk_orphans(&self) -> u64 {
        unsafe { sys::ct_chunk_page_store_reclaim_orphans(self.ptr.as_ptr()) }
    }

    /// Reclaim catalog generations older than the supplied published
    /// retention watermark. Non-chunk stores have no catalog generations.
    #[must_use]
    pub fn reclaim_chunk_generations_before(&self, tree_id: u64, generation: u64) -> u64 {
        self.chunk_catalog
            .as_ref()
            .map_or(0, |catalog| catalog.reclaim_before(tree_id, generation))
    }
}
