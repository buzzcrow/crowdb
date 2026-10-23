// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Immutable host leases whose shared views charge one backing allocation.

use std::ops::Range;
use std::sync::Arc;

use bytes::Bytes;
use crowdb_rpc_ffi::{Buffer, OwnedPayload, ProducerBufferAllocator, WritableBuffer};

use crate::admission::Credit;
use crate::{CapacityKind, Error, Result, RuntimeResources};

mod producer;
mod receive;
pub use producer::FrozenObjectBuffer;
mod framed_chain;
pub(crate) use receive::AllocationRegistry;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LeaseMetadata {
    pub request_id: u64,
    pub stream_id: u64,
    pub sequence: u64,
    pub logical_offset: u64,
    pub length: usize,
    pub allocation_capacity: usize,
    pub registration_id: u32,
}

struct Backing {
    // Storage is dropped before admission is returned to another producer.
    bytes: Bytes,
    registration: Option<receive::Registration>,
    credit: Credit,
}

#[derive(Clone)]
pub struct BufferLease {
    // Keep each upstream logical read credit, even when physical storage is shared.
    source: Option<OwnedPayload>,
    backing: Arc<Backing>,
    range: Range<usize>,
    metadata: LeaseMetadata,
}

impl BufferLease {
    pub(crate) fn belongs_to(&self, resources: &RuntimeResources) -> bool {
        let budget = self.backing.credit.budget();
        Arc::ptr_eq(budget, &resources.held) || Arc::ptr_eq(budget, &resources.producers)
    }
    #[must_use]
    pub fn as_slice(&self) -> &[u8] {
        &self.backing.bytes[self.range.clone()]
    }

    #[must_use]
    pub fn metadata(&self) -> LeaseMetadata {
        self.metadata
    }

    /// # Errors
    /// Rejects ranges outside the logical payload or overflowing offsets.
    pub fn slice(&self, range: Range<usize>) -> Result<Self> {
        if range.start > range.end || range.end > self.range.len() {
            return Err(Error::BufferRange);
        }
        let mut metadata = self.metadata;
        metadata.logical_offset = metadata
            .logical_offset
            .checked_add(range.start as u64)
            .ok_or(Error::BufferRange)?;
        metadata.length = range.len();
        Ok(Self {
            source: self.source.clone(),
            backing: Arc::clone(&self.backing),
            range: self.range.start + range.start..self.range.start + range.end,
            metadata,
        })
    }

    /// Transfers the same backing owner to a storage writer or asynchronous copy.
    #[must_use]
    pub fn into_bytes(self) -> Bytes {
        Bytes::from_owner(self)
    }

    fn new(bytes: Bytes, metadata: LeaseMetadata, credit: Credit) -> Self {
        Self {
            source: None,
            range: 0..bytes.len(),
            backing: Arc::new(Backing {
                bytes,
                registration: None,
                credit,
            }),
            metadata,
        }
    }
}

impl AsRef<[u8]> for BufferLease {
    fn as_ref(&self) -> &[u8] {
        self.as_slice()
    }
}

pub struct WritableLease {
    buffer: WritableBuffer,
    // Unique writable storage is released before its capacity credit.
    credit: Credit,
}

impl WritableLease {
    #[must_use]
    pub fn capacity(&self) -> usize {
        self.buffer.capacity()
    }

    /// Initializes a final allocation once and freezes it for immutable submission.
    /// # Errors
    /// Rejects empty or oversized initialized prefixes.
    pub fn initialize_with(
        self,
        length: usize,
        initial: u8,
        initialize: impl FnOnce(&mut [u8]),
    ) -> Result<BufferLease> {
        let buffer = self
            .buffer
            .initialize_with(length, initial, initialize)
            .map_err(|_| Error::BufferRange)?;
        Ok(freeze(buffer, self.credit))
    }

    /// # Errors
    /// Rejects empty input or input larger than the unique allocation.
    pub fn write(self, bytes: &[u8]) -> Result<BufferLease> {
        if bytes.is_empty() {
            return Err(Error::BufferRange);
        }
        let buffer = self.buffer.write(bytes).map_err(|_| Error::BufferRange)?;
        Ok(freeze(buffer, self.credit))
    }
}

fn freeze(buffer: Buffer, credit: Credit) -> BufferLease {
    let native = buffer.lease_metadata();
    let metadata = LeaseMetadata {
        request_id: native.request_id,
        stream_id: native.stream_id,
        sequence: native.sequence,
        logical_offset: native.logical_offset,
        length: buffer.as_slice().len(),
        allocation_capacity: native.capacity as usize,
        registration_id: native.registration_id,
    };
    BufferLease::new(Bytes::from_owner(buffer), metadata, credit)
}

impl RuntimeResources {
    /// Retains ordinary caller storage with its actual Vec allocation capacity.
    /// # Errors
    /// Rejects closed admission, invalid identity/offset or exhausted held capacity.
    pub fn retain_vec(
        &self,
        data: Vec<u8>,
        request_id: u64,
        stream_id: u64,
        sequence: u64,
        logical_offset: u64,
    ) -> Result<BufferLease> {
        self.check_open()?;
        if request_id == 0 || logical_offset.checked_add(data.len() as u64).is_none() {
            return Err(Error::BufferRange);
        }
        let metadata = LeaseMetadata {
            request_id,
            stream_id,
            sequence,
            logical_offset,
            length: data.len(),
            allocation_capacity: data.capacity(),
            registration_id: 0,
        };
        let credit = self.held.charge(data.capacity())?;
        Ok(BufferLease::new(Bytes::from(data), metadata, credit))
    }

    /// Reuses a service's bounded producer allocator; no staging Vec is created.
    /// # Errors
    /// Returns producer capacity exhaustion, invalid size or closed admission.
    pub fn allocate(&self, allocator: &ProducerBufferAllocator, capacity: u32) -> Result<WritableLease> {
        self.check_open()?;
        let buffer = allocator
            .allocate(capacity)
            .ok_or(Error::Capacity(CapacityKind::Producers))?;
        let credit = self.producers.charge(buffer.capacity())?;
        let lease = WritableLease { buffer, credit };
        self.check_open()?;
        Ok(lease)
    }
}
