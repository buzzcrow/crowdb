// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Bounded owner-backed native receive buffers for the Hyper HTTP/1 path.

#![allow(unsafe_code)]

use std::cell::UnsafeCell;
use std::io;
use std::mem::MaybeUninit;
use std::ops::Range;
use std::ptr::NonNull;
use std::sync::atomic::{AtomicBool, AtomicPtr, AtomicUsize, Ordering};
use std::sync::{Arc, Weak};
use std::task::{Context, Poll};

use atomic_waker::AtomicWaker;
use crowdb_chunk_client::FramedWriteBuffer;
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
    owner_handoff: AtomicBool,
    prefetched: AtomicBool,
    ready_owner: AtomicPtr<NativeFramedOwner>,
}

/// One contiguous native receive owner with populated physical-frame slots.
pub struct NativeFramedOwner {
    owner: Arc<NativeOwner>,
    payload_lengths: Box<[u16]>,
    logical_len: u64,
    physical_len: usize,
}

// SAFETY: Hyper invokes one provider serially for one Incoming body. The
// atomic guard rejects accidental concurrent entry before accessing state.
unsafe impl Sync for NativeBodyReceiver {}

struct ReceiverState {
    owner: Option<Arc<NativeOwner>>,
    next_slot: usize,
    issued: Option<IssuedSlot>,
    completed_slots: usize,
    payload_lengths: Vec<u16>,
}

struct IssuedSlot {
    owner: Weak<NativeOwner>,
    capacity: usize,
    slot: usize,
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
                completed_slots: 0,
                payload_lengths: vec![0; self.state.owner_bytes / MAX_FRAME_BYTES],
            }),
            state_in_use: AtomicBool::new(false),
            owner_handoff: AtomicBool::new(false),
            prefetched: AtomicBool::new(false),
            ready_owner: AtomicPtr::new(std::ptr::null_mut()),
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
    /// Enable publication of each full native owner. A request which receives
    /// header-buffer read-ahead remains on the generic view path.
    pub fn enable_owner_handoff(&self) {
        self.owner_handoff.store(true, Ordering::Release);
    }

    /// Whether body data can still be delivered as complete native owners.
    #[must_use]
    pub fn owner_handoff_active(&self) -> bool {
        self.owner_handoff.load(Ordering::Acquire) && !self.prefetched.load(Ordering::Acquire)
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

    /// Take a completed contiguous owner after the body frame which filled its
    /// last slot has been delivered.
    #[must_use]
    pub fn take_ready_owner(&self) -> Option<NativeFramedOwner> {
        let pointer = self.ready_owner.swap(std::ptr::null_mut(), Ordering::AcqRel);
        if pointer.is_null() {
            return None;
        }
        // SAFETY: `on_data_ready` publishes one Box with release ordering;
        // this swap is the unique consumer of that pointer.
        Some(unsafe { *Box::from_raw(pointer) })
    }

    /// Finish the populated prefix of the current owner at HTTP body EOF.
    ///
    /// # Errors
    ///
    /// Returns an error if Hyper still owns an issued receive region or the
    /// provider's slot accounting is inconsistent.
    pub fn finish_owner(&self) -> io::Result<Option<NativeFramedOwner>> {
        if let Some(owner) = self.take_ready_owner() {
            return Ok(Some(owner));
        }
        if !self.owner_handoff.load(Ordering::Acquire) || self.prefetched.load(Ordering::Acquire) {
            return Ok(None);
        }
        let _guard = self.enter_state()?;
        // SAFETY: `_guard` gives this invocation exclusive state access.
        let state = unsafe { &mut *self.state.get() };
        if state.issued.is_some() {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "cannot finish native owner while a receive region is issued",
            ));
        }
        if state.completed_slots == 0 {
            return Ok(None);
        }
        let owner = state
            .owner
            .take()
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "partial native owner is missing"))?;
        let result = completed_owner(state, owner)?;
        state.next_slot = 0;
        Ok(Some(result))
    }

    fn publish_owner(&self, owner: NativeFramedOwner) -> io::Result<()> {
        let pointer = Box::into_raw(Box::new(owner));
        if self
            .ready_owner
            .compare_exchange(
                std::ptr::null_mut(),
                pointer,
                Ordering::Release,
                Ordering::Relaxed,
            )
            .is_ok()
        {
            return Ok(());
        }
        // SAFETY: publication failed, so ownership remains local.
        drop(unsafe { Box::from_raw(pointer) });
        Err(io::Error::new(
            io::ErrorKind::WouldBlock,
            "previous native owner was not consumed",
        ))
    }
}

fn completed_owner(state: &mut ReceiverState, owner: Arc<NativeOwner>) -> io::Result<NativeFramedOwner> {
    let lengths = state.payload_lengths[..state.completed_slots]
        .to_vec()
        .into_boxed_slice();
    let logical_len = lengths.iter().map(|length| u64::from(*length)).sum();
    let last_payload = lengths
        .last()
        .copied()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "native owner has no completed slots"))?;
    let physical_len = (lengths.len() - 1) * MAX_FRAME_BYTES
        + FRAME_HEADER_PREFIX_BYTES
        + usize::from(last_payload)
        + FRAME_FOOTER_BYTES;
    state.completed_slots = 0;
    state.payload_lengths.fill(0);
    Ok(NativeFramedOwner {
        owner,
        payload_lengths: lengths,
        logical_len,
        physical_len,
    })
}

impl Drop for NativeBodyReceiver {
    fn drop(&mut self) {
        let pointer = self.ready_owner.swap(std::ptr::null_mut(), Ordering::AcqRel);
        if !pointer.is_null() {
            // SAFETY: the receiver owns an unpublished-to-consumer Box here.
            drop(unsafe { Box::from_raw(pointer) });
        }
    }
}

impl FramedWriteBuffer for NativeFramedOwner {
    fn logical_len(&self) -> u64 {
        self.logical_len
    }

    fn frame_count(&self) -> usize {
        self.payload_lengths.len()
    }

    fn frame_payload_len(&self, index: usize) -> Option<usize> {
        self.payload_lengths.get(index).copied().map(usize::from)
    }

    fn finalize_frame(
        &mut self,
        index: usize,
        magic: FrameMagic,
        chunk_id: ChunkId,
        write_time_ms: u64,
    ) -> Result<Range<usize>, FrameError> {
        let payload_len = self
            .frame_payload_len(index)
            .ok_or(FrameError::InvalidLocationRange)?;
        let frame_offset = index
            .checked_mul(MAX_FRAME_BYTES)
            .ok_or(FrameError::LengthOverflow)?;
        let payload_offset = frame_offset + FRAME_HEADER_PREFIX_BYTES;
        let frame_len = FRAME_HEADER_PREFIX_BYTES + payload_len + FRAME_FOOTER_BYTES;
        // SAFETY: the provider never exposes the reserved header/footer bytes.
        // This consumed slot is their only writer; payload views alias only
        // the disjoint initialized payload range.
        unsafe {
            let header = std::slice::from_raw_parts_mut(
                self.owner.pointer.as_ptr().add(frame_offset),
                FRAME_HEADER_PREFIX_BYTES,
            );
            let payload =
                std::slice::from_raw_parts(self.owner.pointer.as_ptr().add(payload_offset), payload_len);
            let footer = std::slice::from_raw_parts_mut(
                self.owner.pointer.as_ptr().add(payload_offset + payload_len),
                FRAME_FOOTER_BYTES,
            );
            encode_frame_regions(magic, chunk_id, payload, write_time_ms, header, footer)?;
        }
        Ok(frame_offset..frame_offset + frame_len)
    }

    fn view(&self, range: Range<usize>) -> Result<Bytes, FrameError> {
        if range.start >= range.end || range.end > self.physical_len {
            return Err(FrameError::InvalidLocationRange);
        }
        Ok(Bytes::from_owner(NativePhysicalFrameView {
            owner: Arc::clone(&self.owner),
            frame_offset: range.start,
            frame_len: range.len(),
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
            state.completed_slots = 0;
            state.payload_lengths.fill(0);
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
            capacity,
            slot,
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
        if self.owner_handoff.load(Ordering::Acquire) && !self.prefetched.load(Ordering::Acquire) {
            let owner = issued.owner.upgrade().ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "native body owner was released before completion",
                )
            })?;
            if issued.slot != state.completed_slots || initialized == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "native body slots completed out of order",
                ));
            }
            state.payload_lengths[issued.slot] = u16::try_from(initialized).map_err(|_| {
                io::Error::new(io::ErrorKind::InvalidData, "native payload length exceeds u16")
            })?;
            state.completed_slots += 1;
            if state.completed_slots == state.payload_lengths.len() {
                let completed = completed_owner(state, owner)?;
                self.publish_owner(completed)?;
            }
        }
        self.allocator
            .state
            .direct_bytes
            .fetch_add(initialized, Ordering::Relaxed);
        Ok(buffer.freeze())
    }

    fn on_prefetched_data(&self, data: Bytes) -> io::Result<Bytes> {
        if self.owner_handoff.load(Ordering::Acquire) {
            let _guard = self.enter_state()?;
            // SAFETY: `_guard` gives this invocation exclusive state access.
            let state = unsafe { &*self.state.get() };
            if state.completed_slots != 0 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "HTTP read-ahead arrived after native owner assembly started",
                ));
            }
        }
        self.prefetched.store(true, Ordering::Release);
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
