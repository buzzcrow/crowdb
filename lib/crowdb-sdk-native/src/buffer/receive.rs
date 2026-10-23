// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Runtime-wide allocation charges with lock-free publication and final release.

use std::sync::atomic::Ordering;
use std::sync::{Arc, Weak};

use bytes::Bytes;
use crossbeam_skiplist::SkipMap;
use crowdb_rpc_ffi::{OwnedPayload, PayloadAllocation};

use super::{Backing, BufferLease, LeaseMetadata};
use crate::{CapacityKind, Error, Result, RuntimeResources};

#[derive(Clone, Copy, Eq, PartialEq, Ord, PartialOrd)]
enum AllocationKey {
    Native(u64),
    Shared(usize),
}

#[derive(Default)]
pub(crate) struct AllocationRegistry {
    entries: SkipMap<AllocationKey, Weak<Backing>>,
}

pub(super) struct Registration {
    registry: Weak<AllocationRegistry>,
    key: AllocationKey,
    owner: Weak<Backing>,
}

impl Drop for Backing {
    fn drop(&mut self) {
        if let Some(registration) = &self.registration {
            if let Some(registry) = registration.registry.upgrade() {
                if let Some(entry) = registry.entries.get(&registration.key) {
                    if Weak::ptr_eq(entry.value(), &registration.owner) {
                        entry.remove();
                    }
                }
            }
        }
        // Fields then release backing bytes before returning capacity credit.
    }
}

impl RuntimeResources {
    /// Retains a logical view, charging its actual allocation once across all
    /// runtime handles. Unknown external Bytes owners use an explicit bounded copy.
    /// Call under a request lease to apply its deadline and cancellation.
    /// # Errors
    /// Rejects invalid identities, closed admission or an allocation above the limit.
    pub async fn retain_payload(
        self: &Arc<Self>,
        payload: OwnedPayload,
        request_id: u64,
        stream_id: u64,
        sequence: u64,
        logical_offset: u64,
    ) -> Result<BufferLease> {
        if request_id == 0 || payload.is_empty() || logical_offset.checked_add(payload.len() as u64).is_none()
        {
            return Err(Error::BufferRange);
        }
        if payload.allocation().is_none() {
            return tokio::select! {
                biased;
                () = self.closing.cancelled() => Err(Error::Shutdown),
                result = self.copy_unknown(payload, request_id, stream_id, sequence, logical_offset) => result,
            };
        }
        let allocation = Arc::clone(payload.allocation().ok_or(Error::BufferRange)?);
        let offset = payload.allocation_offset();
        if offset
            .checked_add(payload.len())
            .map_or(true, |end| end > allocation.bytes().len())
        {
            return Err(Error::BufferRange);
        }
        let metadata = LeaseMetadata {
            request_id,
            stream_id,
            sequence,
            logical_offset,
            length: payload.len(),
            allocation_capacity: allocation.capacity(),
            registration_id: allocation.registration_id(),
        };
        tokio::select! {
            biased;
            () = self.closing.cancelled() => Err(Error::Shutdown),
            backing = self.retain_allocation(&allocation) => Ok(BufferLease {
                range: offset..offset + payload.len(),
                source: Some(payload),
                backing: backing?,
                metadata,
            }),
        }
    }

    async fn copy_unknown(
        &self,
        payload: OwnedPayload,
        request_id: u64,
        stream_id: u64,
        sequence: u64,
        logical_offset: u64,
    ) -> Result<BufferLease> {
        let amount = payload.len();
        if !self.held.can_fit(amount) {
            return Err(Error::Capacity(CapacityKind::HeldBuffers));
        }
        let _registration = self.held.register();
        let credit = loop {
            let changed = self.held.wake.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            self.check_open()?;
            if let Ok(credit) = self.held.charge(amount) {
                break credit;
            }
            changed.await;
        };
        let copied = payload.to_vec();
        if copied.capacity() != amount {
            return Err(Error::Capacity(CapacityKind::HeldBuffers));
        }
        self.compatibility_copy_bytes
            .fetch_add(amount as u64, Ordering::Relaxed);
        Ok(BufferLease::new(
            Bytes::from(copied),
            LeaseMetadata {
                request_id,
                stream_id,
                sequence,
                logical_offset,
                length: amount,
                allocation_capacity: amount,
                registration_id: 0,
            },
            credit,
        ))
    }

    async fn retain_allocation(&self, allocation: &Arc<PayloadAllocation>) -> Result<Arc<Backing>> {
        if !self.held.can_fit(allocation.capacity()) {
            return Err(Error::Capacity(CapacityKind::HeldBuffers));
        }
        let key = allocation.native_identity().map_or_else(
            || AllocationKey::Shared(Arc::as_ptr(allocation) as usize),
            AllocationKey::Native,
        );
        let _registration = self.held.register();
        loop {
            let changed = self.held.wake.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            self.check_open()?;
            if let Some(entry) = self.allocations.entries.get(&key) {
                if let Some(backing) = entry.value().upgrade() {
                    return Ok(backing);
                }
                entry.remove();
            }
            if let Ok(credit) = self.held.charge(allocation.capacity()) {
                let candidate = Arc::new_cyclic(|owner| Backing {
                    bytes: allocation.bytes().clone(),
                    registration: Some(Registration {
                        registry: Arc::downgrade(&self.allocations),
                        key,
                        owner: owner.clone(),
                    }),
                    _credit: credit,
                });
                let entry = self
                    .allocations
                    .entries
                    .get_or_insert(key, Arc::downgrade(&candidate));
                let published = entry.value().upgrade();
                // A waiter that raced the first charge must observe publication,
                // even if the new allocation consumes the entire held budget.
                self.held.changed();
                if let Some(backing) = published {
                    return Ok(backing);
                }
                entry.remove();
                continue;
            }
            changed.await;
        }
    }
}
