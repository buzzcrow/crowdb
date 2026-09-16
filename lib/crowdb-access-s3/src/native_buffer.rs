// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Bounded owner-backed native receive buffers for the Hyper HTTP/1 path.

#![allow(unsafe_code)]

use std::cell::UnsafeCell;
use std::io;
use std::mem::MaybeUninit;
use std::ptr::NonNull;
use std::sync::atomic::{AtomicBool, AtomicPtr, AtomicUsize, Ordering};
use std::sync::{Arc, Weak};
use std::task::{Context, Poll};

use atomic_waker::AtomicWaker;
use crowdb_protocol::common::ChunkId;
use crowdb_protocol::frame::{
    encode_frame_regions, FrameError, FrameMagic, FRAME_FOOTER_BYTES, FRAME_HEADER_PREFIX_BYTES,
    MAX_FRAME_BYTES, MAX_FRAME_PAYLOAD_BYTES,
};
use hyper::body::{Bytes, Http1BodyReceiveBuffer, Http1BodyReceiveProvider};

#[derive(Clone)]
pub struct NativeBodyAllocator {
    state: Arc<AllocatorState>,
}

pub struct NativeBodyReceiver {
    allocator: NativeBodyAllocator,
    state: UnsafeCell<ReceiverState>,
    state_in_use: AtomicBool,
    capture_frames: AtomicBool,
    ready_frame: AtomicPtr<NativeFrameSlot>,
}

/// One socket-filled payload slot whose reserved framing bytes are still
/// private to the CROWDB owner.
pub struct NativeFrameSlot {
    owner: Arc<NativeOwner>,
    payload_offset: usize,
    payload_len: usize,
}

// SAFETY: Hyper invokes one provider serially for one Incoming body. The
// atomic guard rejects accidental concurrent entry before accessing state.
unsafe impl Sync for NativeBodyReceiver {}

struct ReceiverState {
    owner: Option<Arc<NativeOwner>>,
    next_slot: usize,
    issued: Option<IssuedSlot>,
}

struct IssuedSlot {
    owner: Weak<NativeOwner>,
    payload_offset: usize,
    capacity: usize,
}

struct AllocatorState {
    budget_bytes: usize,
    owner_bytes: usize,
    retained_bytes: AtomicUsize,
    allocations: AtomicUsize,
    direct_bytes: AtomicUsize,
    prefetched_bytes: AtomicUsize,
    credit_waker: AtomicWaker,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum NativeBufferConfigError {
    #[error("native body buffer budget must be nonzero")]
    ZeroBudget,
    #[error("native body owner size must be a 64 KiB multiple and no larger than the budget")]
    InvalidOwnerSize,
}

impl NativeBodyAllocator {
    /// Constructs a lock-free native allocator with one aggregate byte budget.
    ///
    /// # Errors
    ///
    /// Rejects zero or invalid owner limits.
    pub fn new(budget_bytes: usize, owner_bytes: usize) -> Result<Self, NativeBufferConfigError> {
        if budget_bytes == 0 {
            return Err(NativeBufferConfigError::ZeroBudget);
        }
        if owner_bytes == 0 || owner_bytes > budget_bytes || owner_bytes % MAX_FRAME_BYTES != 0 {
            return Err(NativeBufferConfigError::InvalidOwnerSize);
        }
        Ok(Self {
            state: Arc::new(AllocatorState {
                budget_bytes,
                owner_bytes,
                retained_bytes: AtomicUsize::new(0),
                allocations: AtomicUsize::new(0),
                direct_bytes: AtomicUsize::new(0),
                prefetched_bytes: AtomicUsize::new(0),
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

    #[must_use]
    pub fn direct_bytes(&self) -> usize {
        self.state.direct_bytes.load(Ordering::Relaxed)
    }

    #[must_use]
    pub fn prefetched_bytes(&self) -> usize {
        self.state.prefetched_bytes.load(Ordering::Relaxed)
    }

    #[must_use]
    pub fn object_receiver(&self) -> NativeBodyReceiver {
        NativeBodyReceiver {
            allocator: self.clone(),
            state: UnsafeCell::new(ReceiverState {
                owner: None,
                next_slot: 0,
                issued: None,
            }),
            state_in_use: AtomicBool::new(false),
            capture_frames: AtomicBool::new(false),
            ready_frame: AtomicPtr::new(std::ptr::null_mut()),
        }
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

impl NativeBodyReceiver {
    /// Enable ordered native-slot handoff for a consumer which will call
    /// [`Self::take_ready_frame`] after every received body frame.
    pub fn enable_frame_slots(&self) {
        self.capture_frames.store(true, Ordering::Release);
    }

    fn enter_state(&self) -> io::Result<ReceiverStateGuard<'_>> {
        self.state_in_use
            .compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed)
            .map_err(|_| {
                io::Error::new(
                    io::ErrorKind::WouldBlock,
                    "HTTP body receive provider was entered concurrently",
                )
            })?;
        Ok(ReceiverStateGuard { receiver: self })
    }

    /// Take the native slot corresponding to `payload`, if that body frame
    /// came from a provider-owned socket buffer. Header-buffer read-ahead has
    /// no slot and returns `None`.
    ///
    /// # Errors
    ///
    /// Returns an error if the body frame does not match the next completed
    /// provider slot.
    pub fn take_ready_frame(&self, payload: &Bytes) -> io::Result<Option<NativeFrameSlot>> {
        let pointer = self.ready_frame.swap(std::ptr::null_mut(), Ordering::AcqRel);
        if pointer.is_null() {
            return Ok(None);
        }
        // SAFETY: `on_data_ready` publishes one Box with release ordering and
        // this swap is the unique consumer of that pointer.
        let frame = unsafe { *Box::from_raw(pointer) };
        let expected = frame.owner.pointer.as_ptr().wrapping_add(frame.payload_offset);
        if payload.as_ptr() != expected || payload.len() != frame.payload_len {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "HTTP body frame does not match its native receive slot",
            ));
        }
        Ok(Some(frame))
    }
}

impl Drop for NativeBodyReceiver {
    fn drop(&mut self) {
        let pointer = self.ready_frame.swap(std::ptr::null_mut(), Ordering::AcqRel);
        if !pointer.is_null() {
            // SAFETY: the receiver owns an unpublished-to-consumer Box here.
            drop(unsafe { Box::from_raw(pointer) });
        }
    }
}

impl NativeFrameSlot {
    #[must_use]
    pub fn payload_len(&self) -> usize {
        self.payload_len
    }

    /// Fill the reserved physical-frame bytes and return one immutable view
    /// over header, original payload, and footer.
    ///
    /// # Errors
    ///
    /// Returns a frame error when the payload or reserved regions are invalid.
    pub fn finalize(
        self,
        magic: FrameMagic,
        chunk_id: ChunkId,
        write_time_ms: u64,
    ) -> Result<Bytes, FrameError> {
        let frame_offset = self.payload_offset - FRAME_HEADER_PREFIX_BYTES;
        let frame_len = FRAME_HEADER_PREFIX_BYTES + self.payload_len + FRAME_FOOTER_BYTES;
        // SAFETY: the provider never exposes the reserved header/footer bytes.
        // This consumed slot is their only writer; payload views alias only
        // the disjoint initialized payload range.
        unsafe {
            let header = std::slice::from_raw_parts_mut(
                self.owner.pointer.as_ptr().add(frame_offset),
                FRAME_HEADER_PREFIX_BYTES,
            );
            let payload = std::slice::from_raw_parts(
                self.owner.pointer.as_ptr().add(self.payload_offset),
                self.payload_len,
            );
            let footer = std::slice::from_raw_parts_mut(
                self.owner
                    .pointer
                    .as_ptr()
                    .add(self.payload_offset + self.payload_len),
                FRAME_FOOTER_BYTES,
            );
            encode_frame_regions(magic, chunk_id, payload, write_time_ms, header, footer)?;
        }
        Ok(Bytes::from_owner(NativePhysicalFrameView {
            owner: self.owner,
            frame_offset,
            frame_len,
        }))
    }
}

struct ReceiverStateGuard<'a> {
    receiver: &'a NativeBodyReceiver,
}

impl Drop for ReceiverStateGuard<'_> {
    fn drop(&mut self) {
        self.receiver.state_in_use.store(false, Ordering::Release);
    }
}

impl Http1BodyReceiveProvider for NativeBodyReceiver {
    fn poll_next_buffer(
        &self,
        cx: &mut Context<'_>,
        requested: usize,
    ) -> Poll<io::Result<Box<dyn Http1BodyReceiveBuffer>>> {
        if requested == 0 {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "cannot allocate an empty HTTP body frame",
            )));
        }
        let _guard = match self.enter_state() {
            Ok(guard) => guard,
            Err(error) => return Poll::Ready(Err(error)),
        };
        // SAFETY: `_guard` gives this invocation exclusive state access.
        let state = unsafe { &mut *self.state.get() };
        if state.issued.is_some() {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "previous HTTP body receive buffer is still issued",
            )));
        }
        let slots_per_owner = self.allocator.state.owner_bytes / MAX_FRAME_BYTES;
        if state.owner.is_none() || state.next_slot == slots_per_owner {
            state.owner = None;
            state.next_slot = 0;
            let owner_bytes = self.allocator.state.owner_bytes;
            if !self.allocator.try_reserve(owner_bytes) {
                self.allocator.state.credit_waker.register(cx.waker());
                if !self.allocator.try_reserve(owner_bytes) {
                    return Poll::Pending;
                }
            }
            match NativeOwner::new(owner_bytes, Arc::clone(&self.allocator.state)) {
                Ok(owner) => {
                    self.allocator.state.allocations.fetch_add(1, Ordering::Relaxed);
                    state.owner = Some(Arc::new(owner));
                }
                Err(error) => {
                    self.allocator
                        .state
                        .retained_bytes
                        .fetch_sub(owner_bytes, Ordering::AcqRel);
                    self.allocator.state.credit_waker.wake();
                    return Poll::Ready(Err(error));
                }
            }
        }
        let slot = state.next_slot;
        state.next_slot += 1;
        let payload_offset = slot * MAX_FRAME_BYTES + FRAME_HEADER_PREFIX_BYTES;
        let capacity = requested.min(MAX_FRAME_PAYLOAD_BYTES);
        let owner = Arc::clone(state.owner.as_ref().expect("owner initialized"));
        state.issued = Some(IssuedSlot {
            owner: Arc::downgrade(&owner),
            payload_offset,
            capacity,
        });
        if state.next_slot == slots_per_owner {
            state.owner = None;
        }
        debug_assert_eq!(
            FRAME_HEADER_PREFIX_BYTES + MAX_FRAME_PAYLOAD_BYTES + FRAME_FOOTER_BYTES,
            MAX_FRAME_BYTES
        );
        Poll::Ready(Ok(Box::new(NativeReceiveRegion {
            owner,
            payload_offset,
            capacity,
            initialized: 0,
        })))
    }

    fn on_data_ready(&self, buffer: Box<dyn Http1BodyReceiveBuffer>) -> io::Result<Bytes> {
        let initialized = buffer.initialized_len();
        let _guard = self.enter_state()?;
        // SAFETY: `_guard` gives this invocation exclusive state access.
        let state = unsafe { &mut *self.state.get() };
        let issued = state.issued.take().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "HTTP body receive buffer was not issued",
            )
        })?;
        if initialized > issued.capacity {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "HTTP body receive buffer exceeded its issued capacity",
            ));
        }
        if self.capture_frames.load(Ordering::Acquire) {
            let owner = issued.owner.upgrade().ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "native body owner was released before completion",
                )
            })?;
            let frame = Box::new(NativeFrameSlot {
                owner,
                payload_offset: issued.payload_offset,
                payload_len: initialized,
            });
            let pointer = Box::into_raw(frame);
            if self
                .ready_frame
                .compare_exchange(
                    std::ptr::null_mut(),
                    pointer,
                    Ordering::Release,
                    Ordering::Relaxed,
                )
                .is_err()
            {
                // SAFETY: publication failed, so ownership remains local.
                drop(unsafe { Box::from_raw(pointer) });
                return Err(io::Error::new(
                    io::ErrorKind::WouldBlock,
                    "previous native body frame was not consumed",
                ));
            }
        }
        self.allocator
            .state
            .direct_bytes
            .fetch_add(initialized, Ordering::Relaxed);
        Ok(buffer.freeze())
    }

    fn on_prefetched_data(&self, data: Bytes) -> io::Result<Bytes> {
        self.allocator
            .state
            .prefetched_bytes
            .fetch_add(data.len(), Ordering::Relaxed);
        Ok(data)
    }
}

struct NativeOwner {
    pointer: NonNull<u8>,
    capacity: usize,
    allocator: Arc<AllocatorState>,
}

// SAFETY: provider-issued regions are disjoint physical-frame payload slots.
// The owner is freed only after the provider and every immutable view drop.
unsafe impl Send for NativeOwner {}
unsafe impl Sync for NativeOwner {}

impl NativeOwner {
    fn new(capacity: usize, allocator: Arc<AllocatorState>) -> io::Result<Self> {
        // SAFETY: malloc returns either a suitably aligned allocation of at
        // least `capacity` bytes or null; ownership is immediately wrapped.
        let pointer = NonNull::new(unsafe { libc::malloc(capacity).cast::<u8>() })
            .ok_or_else(|| io::Error::new(io::ErrorKind::OutOfMemory, "native body allocation failed"))?;
        Ok(Self {
            pointer,
            capacity,
            allocator,
        })
    }
}

impl Drop for NativeOwner {
    fn drop(&mut self) {
        // SAFETY: `pointer` came from malloc and this owner frees it once.
        unsafe { libc::free(self.pointer.as_ptr().cast()) };
        self.allocator
            .retained_bytes
            .fetch_sub(self.capacity, Ordering::AcqRel);
        self.allocator.credit_waker.wake();
    }
}

struct NativeReceiveRegion {
    owner: Arc<NativeOwner>,
    payload_offset: usize,
    capacity: usize,
    initialized: usize,
}

impl Http1BodyReceiveBuffer for NativeReceiveRegion {
    fn spare_capacity_mut(&mut self) -> &mut [MaybeUninit<u8>] {
        let remaining = self.capacity - self.initialized;
        // SAFETY: `initialized <= capacity`; this allocation is uniquely
        // borrowed and the returned slice covers only its uninitialized tail.
        unsafe {
            std::slice::from_raw_parts_mut(
                self.owner
                    .pointer
                    .as_ptr()
                    .add(self.payload_offset + self.initialized)
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

    fn initialized_len(&self) -> usize {
        self.initialized
    }

    fn freeze(self: Box<Self>) -> Bytes {
        Bytes::from_owner(NativePayloadView {
            owner: self.owner,
            payload_offset: self.payload_offset,
            initialized: self.initialized,
        })
    }
}

struct NativePayloadView {
    owner: Arc<NativeOwner>,
    payload_offset: usize,
    initialized: usize,
}

struct NativePhysicalFrameView {
    owner: Arc<NativeOwner>,
    frame_offset: usize,
    frame_len: usize,
}

impl AsRef<[u8]> for NativePayloadView {
    fn as_ref(&self) -> &[u8] {
        // SAFETY: Hyper calls `advance` only for bytes reported initialized by
        // the socket. Other regions mutate only disjoint payload slots.
        unsafe {
            std::slice::from_raw_parts(
                self.owner.pointer.as_ptr().add(self.payload_offset),
                self.initialized,
            )
        }
    }
}

impl AsRef<[u8]> for NativePhysicalFrameView {
    fn as_ref(&self) -> &[u8] {
        // SAFETY: `finalize` initialized this complete frame range before the
        // immutable owner view was constructed.
        unsafe {
            std::slice::from_raw_parts(self.owner.pointer.as_ptr().add(self.frame_offset), self.frame_len)
        }
    }
}
