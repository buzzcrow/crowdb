// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
//! Explicit host ownership for real backend close work after the last reader.
use crate::{error::check, sys, CtError};
use std::ffi::c_void;
use std::ptr::NonNull;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::sync::Notify;

struct ServiceState {
    native: NonNull<sys::ct_backend_release_service>,
    wake: Arc<Notify>,
    done: AtomicBool,
    completion: Notify,
}
// Native final owners are lock-free producers. The host pump is serialized by
// the native queue and owns this handle through actual descriptor disposal.
unsafe impl Send for ServiceState {}
unsafe impl Sync for ServiceState {}
impl Drop for ServiceState {
    fn drop(&mut self) {
        unsafe { sys::ct_backend_release_service_free(self.native.as_ptr()) };
    }
}

/// An independent host handle, also retained by the bounded background pump.
/// Keep the Tokio host alive until `shutdown` has completed. Dropping a Tree or
/// this handle never closes admission while retained snapshots can still read.
#[derive(Clone)]
pub struct BackendReleaseService {
    state: Arc<ServiceState>,
}
impl BackendReleaseService {
    pub(crate) fn attach(tree: NonNull<sys::ct_tree>) -> Result<Option<Self>, CtError> {
        let Ok(executor) = tokio::runtime::Handle::try_current() else {
            return Ok(None);
        };
        let wake = Arc::new(Notify::new());
        let context = Arc::into_raw(wake.clone()).cast_mut().cast::<c_void>();
        let mut native = std::ptr::null_mut();
        let status = unsafe {
            sys::ct_tree_backend_release_service(tree.as_ptr(), wake_queue, drop_wake, context, &mut native)
        };
        if let Err(error) = check(status) {
            unsafe { drop_wake(context) };
            return if matches!(error, CtError::NotFound) {
                Ok(None)
            } else {
                Err(error)
            };
        }
        let state = Arc::new(ServiceState {
            native: NonNull::new(native).ok_or(CtError::Internal)?,
            wake,
            done: AtomicBool::new(false),
            completion: Notify::new(),
        });
        executor.spawn(pump(state.clone()));
        Ok(Some(Self { state }))
    }
    /// Call only after closing host ingress to all users of the backend. This
    /// waits for retained readers and actual close jobs, without retaining a
    /// Tree or its native CPU runtime. The native backend also stops admission
    /// automatically when its final owner disappears.
    pub async fn shutdown(&self) {
        unsafe { sys::ct_backend_release_service_stop(self.state.native.as_ptr()) };
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
async fn pump(state: Arc<ServiceState>) {
    loop {
        let wake = state.wake.notified();
        tokio::pin!(wake);
        wake.as_mut().enable();
        let owner = state.clone();
        let step = tokio::task::spawn_blocking(move || unsafe {
            sys::ct_backend_release_service_pump(owner.native.as_ptr(), 8)
        })
        .await;
        let Ok(step) = step else {
            return;
        };
        if step.drained != 0 {
            state.done.store(true, Ordering::Release);
            state.completion.notify_waiters();
            return;
        }
        if step.more == 0 {
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
