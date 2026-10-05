// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Native transport ABI and retained service resolver ownership.

use crate::error::{check, CtError};
use crate::sys;
#[cfg(feature = "chunk-rpc")]
use crowdb_rpc_ffi::OwnedClientRoute;
use std::ffi::c_void;
use std::ptr::NonNull;
#[cfg(feature = "chunk-rpc")]
use std::sync::Arc;

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
    pub mirror_copies: u32,
}

#[cfg(feature = "chunk-rpc")]
#[derive(Debug, Clone)]
pub struct OwnedChunkRpcDiskRoute {
    pub disk_id_high: u64,
    pub disk_id_low: u64,
    pub route: OwnedClientRoute,
}

#[cfg(feature = "chunk-rpc")]
pub struct OwnedChunkRpcTransportOptions {
    pub chunkdb: Arc<ChunkRpcRouteResolver>,
    pub disks: Option<Arc<ChunkRpcRouteResolver>>,
    pub disk_routes: Vec<OwnedChunkRpcDiskRoute>,
    pub writer_lease_ms: u64,
    pub rpc_timeout_ms: u64,
    pub completion_capacity: u32,
    pub mirror_copies: u32,
}

#[cfg(feature = "chunk-rpc")]
pub type ChunkRpcRouteResolver = dyn Fn(Option<(u64, u64)>, bool) -> Option<OwnedClientRoute> + Send + Sync;

#[cfg(feature = "chunk-rpc")]
struct OwnedTransportRoutes {
    chunkdb: Arc<ChunkRpcRouteResolver>,
    disks: Option<Arc<ChunkRpcRouteResolver>>,
    _disk_routes: Vec<OwnedChunkRpcDiskRoute>,
}

pub struct ChunkTransport {
    pub(super) ptr: NonNull<sys::ct_chunk_transport>,
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
            mirror_copies: options.mirror_copies,
            chunkdb_resolver: sys::ct_chunk_rpc_resolver::default(),
            disk_resolver: sys::ct_chunk_rpc_resolver::default(),
        };
        let mut out = std::ptr::null_mut();
        check(unsafe { sys::ct_rpc_chunk_transport_open(&raw, &mut out) })?;
        Ok(Self {
            ptr: NonNull::new(out).ok_or(CtError::Internal)?,
        })
    }

    /// Create a direct C++ transport while retaining all crowdb-rpc owners.
    #[cfg(feature = "chunk-rpc")]
    pub fn open_owned_rpc(options: OwnedChunkRpcTransportOptions) -> Result<Self, CtError> {
        let disk_routes: Vec<_> = options
            .disk_routes
            .iter()
            .map(|route| sys::ct_chunk_rpc_disk_route {
                disk_id_high: route.disk_id_high,
                disk_id_low: route.disk_id_low,
                route: raw_route(owned_raw_route(&route.route)),
            })
            .collect();
        let owners = Arc::new(OwnedTransportRoutes {
            chunkdb: options.chunkdb,
            disks: options.disks,
            _disk_routes: options.disk_routes,
        });
        let raw = sys::ct_chunk_rpc_transport_options {
            chunkdb: sys::ct_chunk_rpc_route {
                client: std::ptr::null_mut(),
                server: std::ptr::null_mut(),
                connection: std::ptr::null_mut(),
            },
            disk_routes: disk_routes.as_ptr(),
            disk_route_count: disk_routes.len(),
            writer_lease_ms: options.writer_lease_ms,
            rpc_timeout_ms: options.rpc_timeout_ms,
            completion_capacity: options.completion_capacity,
            mirror_copies: options.mirror_copies,
            chunkdb_resolver: sys::ct_chunk_rpc_resolver {
                context: Arc::as_ptr(&owners).cast_mut().cast(),
                resolve: Some(resolve_route),
                release_route: Some(release_route),
                retain_context: Some(retain_context),
                release_context: Some(release_context),
            },
            disk_resolver: if owners.disks.is_some() {
                sys::ct_chunk_rpc_resolver {
                    context: Arc::as_ptr(&owners).cast_mut().cast(),
                    resolve: Some(resolve_disk_route),
                    release_route: Some(release_route),
                    retain_context: Some(retain_context),
                    release_context: Some(release_context),
                }
            } else {
                sys::ct_chunk_rpc_resolver::default()
            },
        };
        let mut out = std::ptr::null_mut();
        // C++ takes its own Arc reference in its RAII resolver, including
        // construction-error paths, and retains it through the last page store.
        check(unsafe { sys::ct_rpc_chunk_transport_open(&raw, &mut out) })?;
        Ok(Self {
            ptr: NonNull::new(out).ok_or(CtError::Internal)?,
        })
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

#[cfg(feature = "chunk-rpc")]
unsafe extern "C" fn resolve_route(
    context: *mut c_void,
    high: u64,
    low: u64,
    refresh: bool,
    route: *mut sys::ct_chunk_rpc_route,
    lease: *mut *mut c_void,
) -> i32 {
    unsafe { resolve_owned_route(context, high, low, refresh, route, lease, false) }
}

#[cfg(feature = "chunk-rpc")]
unsafe extern "C" fn resolve_disk_route(
    context: *mut c_void,
    high: u64,
    low: u64,
    refresh: bool,
    route: *mut sys::ct_chunk_rpc_route,
    lease: *mut *mut c_void,
) -> i32 {
    unsafe { resolve_owned_route(context, high, low, refresh, route, lease, true) }
}

#[cfg(feature = "chunk-rpc")]
unsafe fn resolve_owned_route(
    context: *mut c_void,
    high: u64,
    low: u64,
    refresh: bool,
    route: *mut sys::ct_chunk_rpc_route,
    lease: *mut *mut c_void,
    disk: bool,
) -> i32 {
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        if context.is_null() || route.is_null() || lease.is_null() {
            return -2;
        }
        let owners = unsafe { &*context.cast::<OwnedTransportRoutes>() };
        let id = if disk {
            Some((high, low))
        } else {
            (high != 0 || low != 0).then_some((high, low))
        };
        let resolver = if disk {
            owners.disks.as_ref()
        } else {
            Some(&owners.chunkdb)
        };
        let Some(resolved) = resolver.and_then(|resolver| resolver(id, refresh)) else {
            return -8;
        };
        unsafe {
            *route = raw_route(owned_raw_route(&resolved));
            *lease = Box::into_raw(Box::new(resolved)).cast();
        }
        0
    }))
    .unwrap_or(-6)
}

#[cfg(feature = "chunk-rpc")]
unsafe extern "C" fn release_route(lease: *mut c_void) {
    if !lease.is_null() {
        unsafe {
            drop(Box::from_raw(lease.cast::<OwnedClientRoute>()));
        }
    }
}

#[cfg(feature = "chunk-rpc")]
unsafe extern "C" fn retain_context(context: *mut c_void) {
    unsafe {
        Arc::increment_strong_count(context.cast::<OwnedTransportRoutes>());
    }
}

#[cfg(feature = "chunk-rpc")]
unsafe extern "C" fn release_context(context: *mut c_void) {
    unsafe {
        Arc::decrement_strong_count(context.cast::<OwnedTransportRoutes>());
    }
}
