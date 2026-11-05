// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Host-owned final pin release. No Tree or native CPU executor is retained.
use super::ChunkRootCatalog;
use crate::{error::check, sys, CtError, TreeRuntime};
use std::ffi::c_void;
use std::ptr::NonNull;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::sync::Notify;

struct ServiceState {
    native: NonNull<sys::ct_chunk_pin_service>,
    wake: Arc<Notify>,
    done: AtomicBool,
    completion: Notify,
}
// The native queue has atomic producers and one serialized backend consumer.
// The task owns this handle through the last metadata callback and queue tail.
unsafe impl Send for ServiceState {}
unsafe impl Sync for ServiceState {}
impl Drop for ServiceState {
    fn drop(&mut self) {
        unsafe { sys::ct_chunk_pin_service_free(self.native.as_ptr()) };
    }
}

/// Keep this service at the host boundary, independently of every Tree.
/// Tree close leaves retained snapshots readable. Host shutdown stops new pins,
/// then waits for those snapshots to release their pins and real unpin replies.
/// The surrounding Tokio runtime must stay alive until `shutdown` completes.
pub struct ChunkPinReleaseService {
    state: Arc<ServiceState>,
}
impl ChunkPinReleaseService {
    /// Reserve the native queue in the host ledger and start one bounded pump
    /// on the existing Tokio blocking pool; this creates no private thread.
    /// # Errors
    /// Rejects missing Tokio context, zero capacity, or insufficient host credit.
    pub fn new(runtime: &TreeRuntime, capacity: usize) -> Result<Self, CtError> {
        let executor = tokio::runtime::Handle::try_current().map_err(|_| CtError::InvalidArgument)?;
        let wake = Arc::new(Notify::new());
        let context = Arc::into_raw(wake.clone()).cast_mut().cast::<c_void>();
        let mut native = std::ptr::null_mut();
        let status = unsafe {
            sys::ct_chunk_pin_service_create(
                runtime.as_ptr(),
                capacity,
                wake_queue,
                drop_wake,
                context,
                &mut native,
            )
        };
        if let Err(error) = check(status) {
            unsafe { drop_wake(context) };
            return Err(error);
        }
        let state = Arc::new(ServiceState {
            native: NonNull::new(native).ok_or(CtError::Internal)?,
            wake,
            done: AtomicBool::new(false),
            completion: Notify::new(),
        });
        executor.spawn(pump(state.clone()));
        Ok(Self { state })
    }
    /// Bind only during catalog initialization, before opening its first Tree.
    /// # Errors
    /// Rejects catalogs without staged protection or a different prior service.
    pub fn bind(&self, catalog: &ChunkRootCatalog) -> Result<(), CtError> {
        check(unsafe {
            sys::ct_root_catalog_bind_pin_service(catalog.ptr.as_ptr(), self.state.native.as_ptr())
        })
    }
    /// Sample registered slots and pending releases without pumping the queue.
    /// `denied_host` counts capacity rejections; `waiters` counts pending releases.
    /// # Errors
    /// Returns a native argument error if the service handle is invalid.
    pub fn resource_usage(&self) -> Result<crate::runtime::ResourceUsage, CtError> {
        let mut raw = sys::ct_resource_usage_v1::default();
        check(unsafe { sys::ct_chunk_pin_service_resource_usage(self.state.native.as_ptr(), &mut raw) })?;
        Ok(raw.into())
    }
    /// Stop admission, preserve existing readers, and join real unpin work.
    pub async fn shutdown(&self) {
        unsafe { sys::ct_chunk_pin_service_stop(self.state.native.as_ptr()) };
        loop {
            let done = self.state.completion.notified();
            tokio::pin!(done);
            done.as_mut().enable();
            if self.state.done.load(Ordering::Acquire) {
                return;
            }
            done.await;
        }
    }
}
impl Drop for ChunkPinReleaseService {
    fn drop(&mut self) {
        // The existing host task owns accepted late releases. Explicit shutdown
        // is required before dropping the surrounding Tokio runtime.
        unsafe { sys::ct_chunk_pin_service_stop(self.state.native.as_ptr()) };
    }
}
async fn pump(state: Arc<ServiceState>) {
    loop {
        let wake = state.wake.notified();
        tokio::pin!(wake);
        wake.as_mut().enable();
        let owner = state.clone();
        let step = tokio::task::spawn_blocking(move || unsafe {
            sys::ct_chunk_pin_service_pump(owner.native.as_ptr(), 8)
        })
        .await;
        let Ok(step) = step else {
            // No native step throws. A host runtime cancelling its blocking
            // pool is an incomplete shutdown; never report pins as released.
            return;
        };
        if step.drained != 0 {
            state.done.store(true, Ordering::Release);
            state.completion.notify_waiters();
            return;
        }
        if step.failed != 0 {
            // Retry the same durable pin identity after an unknown outcome.
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        } else if step.more == 0 {
            wake.await;
        }
    }
}
unsafe extern "C" fn wake_queue(context: *mut c_void) {
    unsafe { &*context.cast::<Notify>() }.notify_one();
}
unsafe extern "C" fn drop_wake(context: *mut c_void) {
    unsafe { drop(Arc::from_raw(context.cast::<Notify>())) };
}
