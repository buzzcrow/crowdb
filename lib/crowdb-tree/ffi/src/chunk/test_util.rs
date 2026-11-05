// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
//! In-memory services exercising the ordinary fixed-pack runtime factory.
use super::{ChunkRootCatalog, ChunkRuntimeOptions, ChunkTransport};
use crate::{error::check, sys, Config, CtError, PageStore, TreeRuntime, TreeRuntimeOptions};
use std::ptr::NonNull;
use std::sync::Arc;

extern "C" {
    fn ct_test_chunk_runtime_create(
        runtime: *mut sys::ct_runtime,
        catalog: *mut *mut sys::ct_root_catalog,
        transport: *mut *mut sys::ct_chunk_transport,
    ) -> std::os::raw::c_int;
    fn ct_test_chunk_runtime_next(
        old: *mut sys::ct_root_catalog,
        out: *mut *mut sys::ct_root_catalog,
    ) -> std::os::raw::c_int;
    fn ct_test_chunk_runtime_finish(catalog: *mut sys::ct_root_catalog) -> std::os::raw::c_int;
    fn ct_test_chunk_runtime_root_load(
        catalog: *mut sys::ct_root_catalog,
        callback: extern "C" fn(*mut std::ffi::c_void) -> bool,
        drop_context: extern "C" fn(*mut std::ffi::c_void),
        context: *mut std::ffi::c_void,
    ) -> std::os::raw::c_int;
}

struct TestRootLoad(Box<dyn Fn() + Send + Sync>);

extern "C" fn root_load(context: *mut std::ffi::c_void) -> bool {
    // The host retains this stable box until its catalog and accepted work drain.
    let hook = unsafe { &*context.cast::<TestRootLoad>() };
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| (hook.0)())).is_ok()
}

extern "C" fn drop_root_load(context: *mut std::ffi::c_void) {
    // Native catalog owners retain the context across writer replacement.
    unsafe { drop(Box::from_raw(context.cast::<TestRootLoad>())) };
}

struct TestReleaseOwner(Arc<ChunkRootCatalog>);
impl Drop for TestReleaseOwner {
    fn drop(&mut self) {
        let result = check(unsafe { ct_test_chunk_runtime_finish(self.0.ptr.as_ptr()) });
        assert!(
            result.is_ok(),
            "test retained a Tree or snapshot past its Chunk host: {result:?}"
        );
    }
}

/// One persistent test tree with a shared CPU host and real candidate/pin rules.
/// Drop every Tree and snapshot before this service. Reopen through `next_writer`
/// only after the previous Tree closes; the catalog incarnation cannot be reused.
pub struct TestChunkBackend {
    _release: TestReleaseOwner,
    pub catalog: Arc<ChunkRootCatalog>,
    transport: ChunkTransport,
    options: ChunkRuntimeOptions,
    pub runtime: Arc<TreeRuntime>,
}
impl TestChunkBackend {
    pub fn new(tree: u64, epoch: u64) -> Result<Self, CtError> {
        let runtime = Arc::new(TreeRuntime::new(TreeRuntimeOptions {
            workers: 1,
            ..TreeRuntimeOptions::default()
        })?);
        let mut catalog = std::ptr::null_mut();
        let mut transport = std::ptr::null_mut();
        check(unsafe { ct_test_chunk_runtime_create(runtime.as_ptr(), &mut catalog, &mut transport) })?;
        let catalog = Arc::new(ChunkRootCatalog {
            ptr: NonNull::new(catalog).ok_or(CtError::Internal)?,
        });
        Ok(Self {
            runtime,
            catalog: catalog.clone(),
            transport: ChunkTransport {
                ptr: NonNull::new(transport).ok_or(CtError::Internal)?,
            },
            options: ChunkRuntimeOptions {
                tree_id: tree,
                owner_epoch: epoch,
                iu_size: 1,
                ..ChunkRuntimeOptions::default()
            },
            _release: TestReleaseOwner(catalog),
        })
    }
    pub fn page_store(&self, generation: u64) -> Result<Arc<PageStore>, CtError> {
        PageStore::open_chunk_runtime(
            &self.runtime,
            ChunkRuntimeOptions {
                open_generation: generation,
                ..self.options
            },
            self.catalog.clone(),
            &self.transport,
        )
        .map(Arc::new)
    }
    pub fn config(&self) -> Config {
        Config {
            runtime: Some(self.runtime.clone()),
            frame_bytes: 4096,
            ..Config::default()
        }
    }
    /// Install while no Tree operation is running; the callback must not panic.
    /// # Errors
    /// Returns an invalid or unavailable native test catalog error.
    pub fn on_root_load_for_tests(
        &mut self,
        callback: impl Fn() + Send + Sync + 'static,
    ) -> Result<(), CtError> {
        let hook = Box::into_raw(Box::new(TestRootLoad(Box::new(callback))));
        check(unsafe {
            ct_test_chunk_runtime_root_load(self.catalog.ptr.as_ptr(), root_load, drop_root_load, hook.cast())
        })?;
        Ok(())
    }
    pub fn next_writer(&mut self) -> Result<(), CtError> {
        let mut out = std::ptr::null_mut();
        check(unsafe { ct_test_chunk_runtime_next(self.catalog.ptr.as_ptr(), &mut out) })?;
        self.catalog = Arc::new(ChunkRootCatalog {
            ptr: NonNull::new(out).ok_or(CtError::Internal)?,
        });
        Ok(())
    }
}
