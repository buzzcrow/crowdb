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
use std::time::Instant;

use arc_swap::ArcSwap;
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
    credit_waker: Arc<AtomicWaker>,
    state: UnsafeCell<ReceiverState>,
    state_in_use: AtomicBool,
    owner_handoff: AtomicBool,
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
    append_slot: Option<usize>,
    credit_wait_started: Option<Instant>,
}

struct IssuedSlot {
    owner: Weak<NativeOwner>,
    capacity: usize,
    slot: usize,
    initial_len: usize,
}

struct AllocatorState {
    budget_bytes: usize,
    owner_bytes: usize,
    retained_bytes: AtomicUsize,
    allocations: AtomicUsize,
    direct_bytes: AtomicUsize,
    prefix_copy_bytes: AtomicUsize,
    backpressure_events: AtomicUsize,
    backpressure_wait_ns: AtomicUsize,
    credit_waiters: ArcSwap<Vec<Weak<AtomicWaker>>>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct NativeBufferMetricsSnapshot {
    pub budget_bytes: usize,
    pub owner_bytes: usize,
    pub retained_bytes: usize,
    pub allocations: usize,
    pub direct_bytes: usize,
    pub prefix_copy_bytes: usize,
    pub backpressure_events: usize,
    pub backpressure_wait_ns: usize,
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
                prefix_copy_bytes: AtomicUsize::new(0),
                backpressure_events: AtomicUsize::new(0),
                backpressure_wait_ns: AtomicUsize::new(0),
                credit_waiters: ArcSwap::from_pointee(Vec::new()),
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
    pub fn prefix_copy_bytes(&self) -> usize {
        self.state.prefix_copy_bytes.load(Ordering::Relaxed)
    }

    #[must_use]
    pub fn metrics_snapshot(&self) -> NativeBufferMetricsSnapshot {
        NativeBufferMetricsSnapshot {
            budget_bytes: self.state.budget_bytes,
            owner_bytes: self.state.owner_bytes,
            retained_bytes: self.state.retained_bytes.load(Ordering::Acquire),
            allocations: self.state.allocations.load(Ordering::Relaxed),
            direct_bytes: self.state.direct_bytes.load(Ordering::Relaxed),
            prefix_copy_bytes: self.state.prefix_copy_bytes.load(Ordering::Relaxed),
            backpressure_events: self.state.backpressure_events.load(Ordering::Relaxed),
            backpressure_wait_ns: self.state.backpressure_wait_ns.load(Ordering::Relaxed),
        }
    }

    #[must_use]
    pub fn object_receiver(&self) -> NativeBodyReceiver {
        let credit_waker = Arc::new(AtomicWaker::new());
        self.state.credit_waiters.rcu(|current| {
            let mut waiters = current
                .iter()
                .filter(|waiter| waiter.strong_count() != 0)
                .cloned()
                .collect::<Vec<_>>();
            waiters.push(Arc::downgrade(&credit_waker));
            Arc::new(waiters)
        });
        NativeBodyReceiver {
            allocator: self.clone(),
            credit_waker,
            state: UnsafeCell::new(ReceiverState {
                owner: None,
                next_slot: 0,
                issued: None,
                completed_slots: 0,
                payload_lengths: vec![0; self.state.owner_bytes / MAX_FRAME_BYTES],
                append_slot: None,
                credit_wait_started: None,
            }),
            state_in_use: AtomicBool::new(false),
            owner_handoff: AtomicBool::new(false),
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

    fn wake_credit_waiters(&self) {
        self.state.wake_credit_waiters();
    }
}

impl AllocatorState {
    fn wake_credit_waiters(&self) {
        for waiter in self.credit_waiters.load().iter() {
            if let Some(waker) = waiter.upgrade() {
                waker.wake();
            }
        }
    }
}

impl NativeBodyReceiver {
    /// Enable publication of each full native owner. Header-buffer read-ahead
    /// is copied into the first owner's payload slots when credit is available.
    pub fn enable_owner_handoff(&self) {
        self.owner_handoff.store(true, Ordering::Release);
    }

    /// Whether body data can still be delivered as complete native owners.
    #[must_use]
    pub fn owner_handoff_active(&self) -> bool {
        self.owner_handoff.load(Ordering::Acquire)
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
        if !self.owner_handoff.load(Ordering::Acquire) {
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
    state.append_slot = None;
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

    fn views(&self, range: Range<usize>) -> Result<Vec<Bytes>, FrameError> {
        if range.start >= range.end || range.end > self.physical_len {
            return Err(FrameError::InvalidLocationRange);
        }
        Ok(vec![Bytes::from_owner(NativePhysicalFrameView {
            owner: Arc::clone(&self.owner),
            frame_offset: range.start,
            frame_len: range.len(),
        })])
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
        if state.owner.is_none() {
            state.next_slot = 0;
            state.completed_slots = 0;
            state.payload_lengths.fill(0);
            state.append_slot = None;
            let owner_bytes = self.allocator.state.owner_bytes;
            if !self.allocator.try_reserve(owner_bytes) {
                if state.credit_wait_started.is_none() {
                    state.credit_wait_started = Some(Instant::now());
                    self.allocator
                        .state
                        .backpressure_events
                        .fetch_add(1, Ordering::Relaxed);
                }
                self.credit_waker.register(cx.waker());
                if !self.allocator.try_reserve(owner_bytes) {
                    return Poll::Pending;
                }
            }
            if let Some(started) = state.credit_wait_started.take() {
                self.allocator.state.backpressure_wait_ns.fetch_add(
                    usize::try_from(started.elapsed().as_nanos()).unwrap_or(usize::MAX),
                    Ordering::Relaxed,
                );
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
                    self.allocator.wake_credit_waiters();
                    return Poll::Ready(Err(error));
                }
            }
        }
        let (slot, initial_len) = state.append_slot.take().map_or_else(
            || {
                let slot = state.next_slot;
                state.next_slot += 1;
                (slot, 0)
            },
            |slot| (slot, usize::from(state.payload_lengths[slot])),
        );
        let payload_offset = slot * MAX_FRAME_BYTES + FRAME_HEADER_PREFIX_BYTES + initial_len;
        let capacity = requested.min(MAX_FRAME_PAYLOAD_BYTES - initial_len);
        let owner = Arc::clone(state.owner.as_ref().expect("owner initialized"));
        state.issued = Some(IssuedSlot {
            owner: Arc::downgrade(&owner),
            capacity,
            slot,
            initial_len,
        });
        if !self.owner_handoff.load(Ordering::Acquire) && state.next_slot == state.payload_lengths.len() {
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
        if self.owner_handoff.load(Ordering::Acquire) {
            let owner = issued.owner.upgrade().ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "native body owner was released before completion",
                )
            })?;
            let expected_slot = if issued.initial_len == 0 {
                state.completed_slots
            } else {
                state.completed_slots.saturating_sub(1)
            };
            if issued.slot != expected_slot || initialized == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "native body slots completed out of order",
                ));
            }
            let payload_len = issued.initial_len + initialized;
            state.payload_lengths[issued.slot] = u16::try_from(payload_len).map_err(|_| {
                io::Error::new(io::ErrorKind::InvalidData, "native payload length exceeds u16")
            })?;
            if issued.initial_len == 0 {
                state.completed_slots += 1;
            }
            if payload_len < MAX_FRAME_PAYLOAD_BYTES {
                state.append_slot = Some(issued.slot);
            }
            if state.completed_slots == state.payload_lengths.len() && state.append_slot.is_none() {
                state.owner = None;
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
        if data.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "prefetched body payload is empty",
            ));
        }
        if !self.owner_handoff.load(Ordering::Acquire) {
            return Ok(data);
        }
        let _guard = self.enter_state()?;
        // SAFETY: `_guard` gives this invocation exclusive state access.
        let state = unsafe { &mut *self.state.get() };
        if state.owner.is_some() || state.completed_slots != 0 || state.issued.is_some() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "HTTP read-ahead arrived after native owner assembly started",
            ));
        }
        let owner_bytes = self.allocator.state.owner_bytes;
        let logical_capacity = (owner_bytes / MAX_FRAME_BYTES) * MAX_FRAME_PAYLOAD_BYTES;
        if data.len() > logical_capacity {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "HTTP read-ahead exceeds one native owner",
            ));
        }
        if !self.allocator.try_reserve(owner_bytes) {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "native body owner budget is exhausted by HTTP read-ahead",
            ));
        }
        let owner = match NativeOwner::new(owner_bytes, Arc::clone(&self.allocator.state)) {
            Ok(owner) => Arc::new(owner),
            Err(error) => {
                self.allocator
                    .state
                    .retained_bytes
                    .fetch_sub(owner_bytes, Ordering::AcqRel);
                self.allocator.wake_credit_waiters();
                return Err(error);
            }
        };
        self.allocator.state.allocations.fetch_add(1, Ordering::Relaxed);
        for (slot, payload) in data.chunks(MAX_FRAME_PAYLOAD_BYTES).enumerate() {
            let payload_offset = slot * MAX_FRAME_BYTES + FRAME_HEADER_PREFIX_BYTES;
            // SAFETY: the reserved owner is exclusively initialized here and
            // every destination payload slot is disjoint and large enough.
            unsafe {
                std::ptr::copy_nonoverlapping(
                    payload.as_ptr(),
                    owner.pointer.as_ptr().add(payload_offset),
                    payload.len(),
                );
            }
            state.payload_lengths[slot] = u16::try_from(payload.len()).map_err(|_| {
                io::Error::new(io::ErrorKind::InvalidData, "native payload length exceeds u16")
            })?;
            state.completed_slots += 1;
        }
        state.next_slot = state.completed_slots;
        if let Some(last) = state.completed_slots.checked_sub(1) {
            if usize::from(state.payload_lengths[last]) < MAX_FRAME_PAYLOAD_BYTES {
                state.append_slot = Some(last);
            }
        }
        state.owner = Some(Arc::clone(&owner));
        if state.completed_slots == state.payload_lengths.len() && state.append_slot.is_none() {
            state.owner = None;
            let completed = completed_owner(state, owner)?;
            self.publish_owner(completed)?;
        }
        self.allocator
            .state
            .prefix_copy_bytes
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
        self.allocator.wake_credit_waiters();
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
