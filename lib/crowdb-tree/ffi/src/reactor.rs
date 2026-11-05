// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::os::fd::{BorrowedFd, OwnedFd, RawFd};
use std::os::raw::c_int;
use std::sync::Arc;

use crate::error::{check, copy_buf, CtError};
use crate::sys;
use crate::tree::Crowdbtree;

// Reactor-driven async futures.
//
// Native reads, scans, admission and maintenance operations use completion
// polling with eventfd notifications. CPU work runs on the shared native runtime;
// even resident reads may be pending on their first poll. An already completed
// operation returns immediately. Pending CPU work and backend I/O both wake the
// eventfd pump when completion becomes observable.
//
// Fan-out note: this deliberately does *not* have every pending future call
// `AsyncFd::poll_read_ready` on a shared registration -- that method's
// single reserved waker slot only keeps the *most recently polling* task's
// waker (tokio's own doc comment on `poll_read_ready`), so N concurrently
// pending gets would silently starve all but the last one to (re)poll,
// hanging forever. Only one task -- a lazily-spawned pump owned by
// `Crowdbtree` -- ever touches the eventfd's `AsyncFd`; every other future
// waits on a `tokio::sync::Notify` the pump fans out to instead, which does
// support any number of concurrent waiters.

/// Give the pump an independent descriptor so native Tree closure cannot
/// recycle a descriptor that Tokio has yet to deregister after task abort.
///
/// # Safety
/// `fd` is a live native eventfd and its owner outlives this call.
pub(crate) unsafe fn duplicate_eventfd(fd: RawFd) -> std::io::Result<OwnedFd> {
    unsafe { BorrowedFd::borrow_raw(fd) }.try_clone_to_owned()
}

extern "C" {
    // libc read(2) -- used only to drain the reactor's eventfd counter
    // below, nothing to do with the ct_* C ABI.
    fn read(fd: c_int, buf: *mut u8, count: usize) -> isize;
}

/// Best-effort drain of the eventfd's accumulated counter back to 0. The
/// Reactor (`reactor.cpp`) never reads it -- draining is left entirely to
/// this side, deliberately, so that a later write (a 0 -> nonzero
/// transition) reliably produces a fresh edge for Tokio's I/O driver to
/// wake on; without draining, a still-nonzero counter can leave a second
/// completion's wakeup silently lost.
pub(crate) fn drain_eventfd(fd: RawFd) {
    let mut buf = [0u8; 8];
    unsafe {
        read(fd, buf.as_mut_ptr(), buf.len());
    }
}

/// The tree's lazily-spawned eventfd pump (see the module-level fan-out
/// note above): `notify` is fanned out to every waiting `drive_ct_future`
/// call each time the pump observes the eventfd fire; `task` is aborted by
/// `Crowdbtree`'s `Drop` before `ct_close` runs. The pump owns a duplicate fd
/// until Tokio deregisters it and drops the cancelled task.
pub(crate) struct EventfdPump {
    pub(crate) notify: Arc<tokio::sync::Notify>,
    pub(crate) task: tokio::task::AbortHandle,
}

/// Decoded (but not yet interpreted) result of one completed `ct_future`.
pub(crate) struct RawOutcome {
    pub(crate) found: bool,
    pub(crate) slot: u64,
    pub(crate) value: Vec<u8>,
    pub(crate) packed: Option<bytes::Bytes>,
}

/// Native future ownership kind. Get results borrow their bytes until the
/// handle is freed; admission completions free themselves on polling. Owned
/// scans use the versioned owner protocol and retain their result separately.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum FutureKind {
    Get,
    Flush,
    ScanOwned,
}

/// RAII guard for one in-flight `ct_future`: frees it via `ct_future_free`
/// if dropped before completion (task cancellation while `.await`ing
/// `drive_ct_future` below). Runs correctly even mid-`.await`: async fn
/// locals still in scope at a suspension point are dropped normally when
/// the generated future itself is dropped.
pub(crate) struct FutureGuard(pub(crate) *mut sys::ct_future);

impl Drop for FutureGuard {
    fn drop(&mut self) {
        if !self.0.is_null() {
            unsafe { sys::ct_future_free(self.0) };
        }
    }
}

// SAFETY: *mut ct_future is an opaque handle the C++ side documents as
// freely movable/pollable from any thread; this narrow impl is
// what lets drive_ct_future's generated future (which holds a FutureGuard
// across its `.await` points) be Send, without having to bless the
// non-Send raw ct_buf pointer that only ever lives inside the fully
// synchronous try_poll_ct_future below (never held across a suspension
// point).
unsafe impl Send for FutureGuard {}

/// One synchronous `ct_future_poll` attempt. `None` if still pending;
/// `Some` if done, in which case `guard` has already been nulled out --
/// either `ct_future_poll` itself freed the underlying `ct_future`
/// (admission completion), or, for a `Get`, this function did so explicitly via
/// `ct_future_free` right after copying `out_value`'s bytes out (which may
/// be a *borrowed* pointer into a still-live frame -- see `copy_buf`).
///
/// Deliberately synchronous and free of any `.await`: `sys::ct_buf` holds a
/// raw `*mut u8` which is not `Send`, so it must never be a value held
/// across a suspension point in `drive_ct_future`'s generated future.
pub(crate) fn try_poll_ct_future(
    guard: &mut FutureGuard,
    kind: FutureKind,
) -> Option<Result<RawOutcome, CtError>> {
    if guard.0.is_null() {
        return Some(Err(CtError::ResourceExhausted));
    }
    if kind == FutureKind::ScanOwned {
        return crate::scan::owner::poll(guard);
    }
    let mut done: c_int = 0;
    let mut found: c_int = 0;
    let mut slot: u64 = 0;
    let mut value = sys::ct_buf {
        data: std::ptr::null_mut(),
        len: 0,
    };
    let rc = unsafe { sys::ct_future_poll(guard.0, &mut done, &mut found, &mut slot, &mut value) };
    if done == 0 {
        return None;
    }
    // The get handle retains the borrowed result until after this copy.
    // Admission completions return an empty buffer.
    let value_bytes = copy_buf(value);
    let result = match check(rc) {
        Ok(()) => Ok(RawOutcome {
            found: found != 0,
            slot,
            value: value_bytes,
            packed: None,
        }),
        Err(e) => Err(e),
    };
    if kind == FutureKind::Get {
        // ct_future_poll deliberately does *not* free a kGet future (see
        // its doc comment in c_api.h). The owned read result must outlive
        // the copy_buf call above.
        unsafe { sys::ct_future_free(guard.0) };
    }
    // Admission completions are freed by ct_future_poll itself. Either way,
    // the underlying ct_future is gone now -- don't let FutureGuard's Drop
    // free it again.
    guard.0 = std::ptr::null_mut();
    Some(result)
}

/// Zero-copy borrowed value from a `ct_get_async` completion. Holds the
/// `ct_future` handle so the C++ page refcount (R6) keeping the frame
/// resident stays alive until this value is dropped. `Send` because the
/// per-page refcount is thread-independent (R6: pin/unpin from any thread).
pub struct PinnedValue {
    handle: *mut sys::ct_future,
    data: *const u8,
    len: usize,
}

// R6: PinnedValue is Send — the C++ page refcount (pin_state_ on PageBase)
// is a thread-independent atomic. ct_future_free unpins from the dropping
// thread. SAFETY: the handle is a unique pointer to a heap-allocated
// ct_future_impl; no shared mutable state across threads except the
// refcount atomics, which are designed for cross-thread access.
unsafe impl Send for PinnedValue {}

impl PinnedValue {
    /// Borrow the value bytes directly from the C++ engine's internal
    /// buffer. Valid until `self` is dropped.
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        if self.data.is_null() || self.len == 0 {
            &[]
        } else {
            unsafe { std::slice::from_raw_parts(self.data, self.len) }
        }
    }

    /// Convert into a `Bytes` that borrows directly from the C++ frame —
    /// zero-copy. The `ct_future` handle (and its page refcount pins) is
    /// kept alive until the `Bytes` is dropped, on any thread (R6: the
    /// per-page refcount is thread-independent, so `PinnedValue` is `Send`).
    /// When the last `Bytes` ref clone is dropped, the owner's `Drop` runs,
    /// which drops the `PinnedValue`, which calls `ct_future_free` to unpin.
    ///
    /// # Panics
    /// Panics if the pre-admitted control window cannot satisfy its fixed owner layout.
    #[must_use]
    pub fn into_bytes(self) -> bytes::Bytes {
        self.try_into_bytes()
            .expect("admitted immutable result control capacity")
    }

    /// Preserve the native allocation account through the Rust control block.
    /// The exact box layout is claimed before allocation and refunded after
    /// the last byte clone is destroyed and that box has been deallocated.
    ///
    /// # Errors
    /// Returns a native admission error if the fixed owner control cannot be claimed.
    pub fn try_into_bytes(self) -> Result<bytes::Bytes, CtError> {
        let budget = crate::owner_budget::OwnerBudget::future(self.handle)?;
        budget.wrap(PinnedBytesOwner { pv: self })
    }
}

/// Owner backing a `Bytes` created from `PinnedValue::into_bytes`. Holds the
/// `PinnedValue` so its `Drop` (calling `ct_future_free`) runs when the
/// `Bytes`'s refcount hits zero. `Send` because `PinnedValue` is `Send` (R6).
struct PinnedBytesOwner {
    pv: PinnedValue,
}

impl AsRef<[u8]> for PinnedBytesOwner {
    fn as_ref(&self) -> &[u8] {
        self.pv.as_bytes()
    }
}

impl Drop for PinnedValue {
    fn drop(&mut self) {
        if !self.handle.is_null() {
            unsafe { sys::ct_future_free(self.handle) };
        }
    }
}

/// Like [`try_poll_ct_future`] for `FutureKind::Get`, but instead of
/// copying the value bytes out and freeing the future, returns a
/// [`PinnedValue`] that borrows directly from the C++ frame. The
/// `ct_future` handle is transferred into the `PinnedValue` (its `Drop`
/// calls `ct_future_free`), so `guard.0` is nulled to prevent
/// `FutureGuard`'s `Drop` from double-freeing.
pub(crate) fn try_poll_ct_future_pinned(
    guard: &mut FutureGuard,
) -> Option<Result<Option<(u64, PinnedValue)>, CtError>> {
    if guard.0.is_null() {
        return Some(Err(CtError::ResourceExhausted));
    }
    let mut done: c_int = 0;
    let mut found: c_int = 0;
    let mut slot: u64 = 0;
    let mut value = sys::ct_buf {
        data: std::ptr::null_mut(),
        len: 0,
    };
    let rc = unsafe { sys::ct_future_poll(guard.0, &mut done, &mut found, &mut slot, &mut value) };
    if done == 0 {
        return None;
    }
    let result = match check(rc) {
        Ok(()) => {
            if found != 0 {
                let pv = PinnedValue {
                    handle: guard.0,
                    data: value.data,
                    len: value.len,
                };
                Ok(Some((slot, pv)))
            } else {
                // Not found: still need to free the future.
                unsafe { sys::ct_future_free(guard.0) };
                Ok(None)
            }
        }
        Err(e) => {
            unsafe { sys::ct_future_free(guard.0) };
            Err(e)
        }
    };
    guard.0 = std::ptr::null_mut();
    Some(result)
}

/// Drives a get, admission or owned-scan handle to completion: polls it and,
/// if pending, waits for the tree's eventfd pump to fan out a notification.
/// An operation already complete on the first poll needs no notification wait.
pub(crate) async fn drive_ct_future(
    guard: FutureGuard,
    tree: &Arc<Crowdbtree>,
    kind: FutureKind,
) -> Result<RawOutcome, CtError> {
    drive_completion(guard, tree, |guard| try_poll_ct_future(guard, kind)).await
}

pub(crate) async fn drive_completion<T>(
    mut guard: FutureGuard,
    tree: &Arc<Crowdbtree>,
    mut poll: impl FnMut(&mut FutureGuard) -> Option<Result<T, CtError>>,
) -> Result<T, CtError> {
    loop {
        // Construct (but do not yet await) the notification future *before*
        // checking ct_future_poll below, not after: Notify::notified
        // captures the pump's current notify_waiters call count at
        // construction time and is guaranteed to fire for any
        // notify_waiters after that point even before this is polled --
        // constructing it only *after* seeing done=0 would leave a window
        // where a completion + notify racing in right there is silently
        // missed, hanging until some unrelated later notification (or
        // forever, if none ever comes).
        let notify_arc = tree.eventfd_notify();
        let notified = notify_arc.as_ref().map(|n| n.notified());

        if let Some(result) = poll(&mut guard) {
            return result;
        }

        match notified {
            Some(n) => n.await,
            None => {
                // Without an available notification pump, yield before
                // polling again. Native CPU work can still be pending even
                // when no backend I/O is needed.
                tokio::task::yield_now().await;
            }
        }
    }
}
