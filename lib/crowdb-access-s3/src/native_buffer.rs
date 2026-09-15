// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Bounded owner-backed native receive buffers for the Hyper HTTP/1 path.

#![allow(unsafe_code)]

use std::io;
use std::mem::MaybeUninit;
use std::ptr::NonNull;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};

use atomic_waker::AtomicWaker;
use hyper::body::{Bytes, Http1BodyAllocator, Http1BodyBuffer};

#[derive(Clone)]
pub struct NativeBodyAllocator {
    state: Arc<AllocatorState>,
}

struct AllocatorState {
    budget_bytes: usize,
    max_frame_bytes: usize,
    retained_bytes: AtomicUsize,
    allocations: AtomicUsize,
    credit_waker: AtomicWaker,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum NativeBufferConfigError {
    #[error("native body buffer budget must be nonzero")]
    ZeroBudget,
    #[error("native body frame size must be nonzero and no larger than the budget")]
    InvalidFrameSize,
}

impl NativeBodyAllocator {
    /// Constructs a lock-free native allocator with one aggregate byte budget.
    ///
    /// # Errors
    ///
    /// Rejects zero or unreachable frame limits.
    pub fn new(budget_bytes: usize, max_frame_bytes: usize) -> Result<Self, NativeBufferConfigError> {
        if budget_bytes == 0 {
            return Err(NativeBufferConfigError::ZeroBudget);
        }
        if max_frame_bytes == 0 || max_frame_bytes > budget_bytes {
            return Err(NativeBufferConfigError::InvalidFrameSize);
        }
        Ok(Self {
            state: Arc::new(AllocatorState {
                budget_bytes,
                max_frame_bytes,
                retained_bytes: AtomicUsize::new(0),
                allocations: AtomicUsize::new(0),
                credit_waker: AtomicWaker::new(),
            }),
        })
    }

    #[must_use]
    pub fn retained_bytes(&self) -> usize {
        self.state.retained_bytes.load(Ordering::Acquire)
    }

    #[must_use]
    pub fn allocation_count(&self) -> usize {
        self.state.allocations.load(Ordering::Relaxed)
    }

    fn try_reserve(&self, bytes: usize) -> bool {
        self.state
            .retained_bytes
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                current
                    .checked_add(bytes)
                    .filter(|next| *next <= self.state.budget_bytes)
            })
            .is_ok()
    }
}

impl Http1BodyAllocator for NativeBodyAllocator {
    fn poll_allocate(
        &self,
        cx: &mut Context<'_>,
        requested: usize,
    ) -> Poll<io::Result<Box<dyn Http1BodyBuffer>>> {
        if requested == 0 {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "cannot allocate an empty HTTP body frame",
            )));
        }
        let capacity = requested.min(self.state.max_frame_bytes);
        if !self.try_reserve(capacity) {
            self.state.credit_waker.register(cx.waker());
            if !self.try_reserve(capacity) {
                return Poll::Pending;
            }
        }
        match NativeAllocation::new(capacity, Arc::clone(&self.state)) {
            Ok(allocation) => {
                self.state.allocations.fetch_add(1, Ordering::Relaxed);
                Poll::Ready(Ok(Box::new(allocation)))
            }
            Err(error) => {
                self.state.retained_bytes.fetch_sub(capacity, Ordering::AcqRel);
                self.state.credit_waker.wake();
                Poll::Ready(Err(error))
            }
        }
    }
}

struct NativeAllocation {
    pointer: NonNull<u8>,
    capacity: usize,
    initialized: usize,
    allocator: Arc<AllocatorState>,
}

// SAFETY: the allocation is uniquely mutable until freeze and immutable
// afterwards; the pointer is freed only when its final Bytes owner drops.
unsafe impl Send for NativeAllocation {}
// SAFETY: `AsRef` exposes only the initialized immutable prefix after freeze.
unsafe impl Sync for NativeAllocation {}

impl NativeAllocation {
    fn new(capacity: usize, allocator: Arc<AllocatorState>) -> io::Result<Self> {
        // SAFETY: malloc returns either a suitably aligned allocation of at
        // least `capacity` bytes or null; ownership is immediately wrapped.
        let pointer = NonNull::new(unsafe { libc::malloc(capacity).cast::<u8>() })
            .ok_or_else(|| io::Error::new(io::ErrorKind::OutOfMemory, "native body allocation failed"))?;
        Ok(Self {
            pointer,
            capacity,
            initialized: 0,
            allocator,
        })
    }
}

impl Http1BodyBuffer for NativeAllocation {
    fn spare_capacity_mut(&mut self) -> &mut [MaybeUninit<u8>] {
        let remaining = self.capacity - self.initialized;
        // SAFETY: `initialized <= capacity`; this allocation is uniquely
        // borrowed and the returned slice covers only its uninitialized tail.
        unsafe {
            std::slice::from_raw_parts_mut(
                self.pointer
                    .as_ptr()
                    .add(self.initialized)
                    .cast::<MaybeUninit<u8>>(),
                remaining,
            )
        }
    }

    fn advance(&mut self, count: usize) -> io::Result<()> {
        if count > self.capacity - self.initialized {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "socket initialized beyond native body allocation",
            ));
        }
        self.initialized += count;
        Ok(())
    }

    fn freeze(self: Box<Self>) -> Bytes {
        Bytes::from_owner(*self)
    }
}

impl AsRef<[u8]> for NativeAllocation {
    fn as_ref(&self) -> &[u8] {
        // SAFETY: Hyper calls `advance` only for bytes reported initialized by
        // the socket, and the immutable slice never exceeds that prefix.
        unsafe { std::slice::from_raw_parts(self.pointer.as_ptr(), self.initialized) }
    }
}

impl Drop for NativeAllocation {
    fn drop(&mut self) {
        // SAFETY: `pointer` came from malloc and this owner frees it once.
        unsafe { libc::free(self.pointer.as_ptr().cast()) };
        self.allocator
            .retained_bytes
            .fetch_sub(self.capacity, Ordering::AcqRel);
        self.allocator.credit_waker.wake();
    }
}
