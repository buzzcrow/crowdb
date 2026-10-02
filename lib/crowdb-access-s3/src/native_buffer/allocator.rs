// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{
    AllocatorState, Arc, ArcSwap, AtomicBool, AtomicPtr, AtomicUsize, AtomicWaker, Bandwidth, FrameMagic,
    MetricName, NativeBodyAllocator, NativeBodyReceiver, NativeBufferConfigError,
    NativeBufferMetricsSnapshot, Ordering, ReceiverState, UnsafeCell, MAX_FRAME_BYTES,
};

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
                peak_retained_bytes: AtomicUsize::new(0),
                allocations: AtomicUsize::new(0),
                direct_bytes: AtomicUsize::new(0),
                prefix_copy: Arc::new(Bandwidth::new(MetricName::Static(
                    "access.http.receive.prefix_copy.bw",
                ))),
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
        usize::try_from(self.state.prefix_copy.snapshot(0.0).total_bytes).unwrap_or(usize::MAX)
    }

    #[must_use]
    pub fn metrics_snapshot(&self) -> NativeBufferMetricsSnapshot {
        NativeBufferMetricsSnapshot {
            budget_bytes: self.state.budget_bytes,
            owner_bytes: self.state.owner_bytes,
            retained_bytes: self.state.retained_bytes.load(Ordering::Acquire),
            peak_retained_bytes: self.state.peak_retained_bytes.load(Ordering::Acquire),
            allocations: self.state.allocations.load(Ordering::Relaxed),
            direct_bytes: self.state.direct_bytes.load(Ordering::Relaxed),
            prefix_copy_bytes: usize::try_from(self.state.prefix_copy.snapshot(0.0).total_bytes)
                .unwrap_or(usize::MAX),
            backpressure_events: self.state.backpressure_events.load(Ordering::Relaxed),
            backpressure_wait_ns: self.state.backpressure_wait_ns.load(Ordering::Relaxed),
        }
    }

    #[must_use]
    pub fn object_receiver(&self) -> NativeBodyReceiver {
        self.receiver_with_owner_bytes(self.state.owner_bytes)
    }

    /// One exact-size owner for a small payload, including its frame regions.
    ///
    /// # Errors
    /// Rejects payloads whose framed size exceeds the aggregate receive budget.
    pub fn object_receiver_for_payload(
        &self,
        payload_bytes: usize,
    ) -> Result<NativeBodyReceiver, NativeBufferConfigError> {
        let physical = crowdb_protocol::frame::framed_physical_length(payload_bytes as u64)
            .ok()
            .and_then(|bytes| usize::try_from(bytes).ok())
            .filter(|bytes| *bytes <= self.state.budget_bytes)
            .ok_or(NativeBufferConfigError::InvalidOwnerSize)?;
        Ok(self.receiver_with_owner_bytes(physical.max(34)))
    }

    /// Attach the process bandwidth series before sharing this allocator.
    ///
    /// # Panics
    /// Panics if the allocator has already been cloned or used for a receiver.
    #[must_use]
    pub fn with_prefix_copy_metric(mut self, metric: Arc<Bandwidth>) -> Self {
        Arc::get_mut(&mut self.state)
            .expect("attach metrics before sharing native allocator")
            .prefix_copy = metric;
        self
    }

    fn receiver_with_owner_bytes(&self, owner_bytes: usize) -> NativeBodyReceiver {
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
                pending_prefetched: None,
                next_slot: 0,
                issued: None,
                completed_slots: 0,
                prepared_slots: 0,
                payload_lengths: vec![0; owner_bytes.div_ceil(MAX_FRAME_BYTES)],
                append_slot: None,
                credit_wait_started: None,
                magic: FrameMagic::RepoLargeV1,
                owner_bytes,
            }),
            state_in_use: AtomicBool::new(false),
            owner_handoff: AtomicBool::new(false),
            ready_owner: AtomicPtr::new(std::ptr::null_mut()),
        }
    }

    pub(super) fn try_reserve(&self, bytes: usize) -> bool {
        let reserved =
            self.state
                .retained_bytes
                .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                    current
                        .checked_add(bytes)
                        .filter(|next| *next <= self.state.budget_bytes)
                });
        if let Ok(previous) = reserved {
            self.state
                .peak_retained_bytes
                .fetch_max(previous + bytes, Ordering::Relaxed);
            true
        } else {
            false
        }
    }

    pub(super) fn wake_credit_waiters(&self) {
        self.state.wake_credit_waiters();
    }
}
