// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Host payload views retaining actual allocation metadata through storage layers.

use std::ops::{Bound, Deref, RangeBounds};
use std::sync::Arc;

use bytes::Bytes;

use crate::{Buffer, BufferLeaseMetadata};

pub struct PayloadAllocation {
    bytes: Bytes,
    capacity: usize,
    native_identity: Option<u64>,
    registration_id: u32,
}

impl PayloadAllocation {
    #[must_use]
    pub fn bytes(&self) -> &Bytes {
        &self.bytes
    }
    #[must_use]
    pub fn capacity(&self) -> usize {
        self.capacity
    }
    #[must_use]
    pub fn native_identity(&self) -> Option<u64> {
        self.native_identity
    }
    #[must_use]
    pub fn registration_id(&self) -> u32 {
        self.registration_id
    }
}

#[derive(Clone, Default)]
pub struct OwnedPayload {
    bytes: Bytes,
    allocation: Option<Arc<PayloadAllocation>>,
    allocation_offset: usize,
    native: Option<BufferLeaseMetadata>,
}

impl OwnedPayload {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Retains a completed RPC buffer without detaching its receive recycler.
    #[must_use]
    pub fn from_buffer(buffer: Buffer) -> Self {
        let metadata = buffer.lease_metadata();
        let allocation = buffer.allocation_metadata().zip(buffer.allocation_view());
        let bytes = Bytes::from_owner(buffer);
        let allocation_offset = allocation
            .as_ref()
            .map_or(0, |(metadata, _)| metadata.offset as usize);
        let allocation = allocation.map(|(allocation, owner)| {
            Arc::new(PayloadAllocation {
                bytes: Bytes::from_owner(owner),
                capacity: allocation.capacity as usize,
                native_identity: Some(allocation.identity),
                registration_id: metadata.registration_id,
            })
        });
        Self {
            bytes,
            allocation,
            allocation_offset,
            native: Some(metadata),
        }
    }

    #[must_use]
    pub fn allocation(&self) -> Option<&Arc<PayloadAllocation>> {
        self.allocation.as_ref()
    }

    #[must_use]
    pub fn allocation_offset(&self) -> usize {
        self.allocation_offset
    }

    #[must_use]
    pub fn native_metadata(&self) -> Option<BufferLeaseMetadata> {
        self.native
    }

    /// # Panics
    /// Panics for ranges outside the current payload, matching Bytes::slice.
    #[must_use]
    pub fn slice(&self, range: impl RangeBounds<usize>) -> Self {
        let start = match range.start_bound() {
            Bound::Included(start) => *start,
            Bound::Excluded(start) => start.checked_add(1).expect("payload slice overflow"),
            Bound::Unbounded => 0,
        };
        Self {
            bytes: self.bytes.slice(range),
            allocation: self.allocation.clone(),
            allocation_offset: self.allocation_offset + start,
            native: self.native,
        }
    }

    /// Keeps allocation metadata when another owner adds a lifetime dependency
    /// around the exact same byte view, such as a logical stream credit.
    /// # Panics
    /// Panics if the wrapper changes the payload's address or length.
    #[must_use]
    pub fn with_owner(self, wrap: impl FnOnce(Bytes) -> Bytes) -> Self {
        let pointer = self.bytes.as_ptr();
        let length = self.bytes.len();
        let bytes = wrap(self.bytes);
        assert_eq!(bytes.as_ptr(), pointer, "payload owner changed its address");
        assert_eq!(bytes.len(), length, "payload owner changed its length");
        Self {
            bytes,
            allocation: self.allocation,
            allocation_offset: self.allocation_offset,
            native: self.native,
        }
    }

    #[must_use]
    pub fn into_bytes(self) -> Bytes {
        self.bytes
    }
}

impl From<Vec<u8>> for OwnedPayload {
    fn from(data: Vec<u8>) -> Self {
        let capacity = data.capacity();
        let bytes = Bytes::from(data);
        Self {
            allocation: Some(Arc::new(PayloadAllocation {
                bytes: bytes.clone(),
                capacity,
                native_identity: None,
                registration_id: 0,
            })),
            bytes,
            allocation_offset: 0,
            native: None,
        }
    }
}

impl From<Bytes> for OwnedPayload {
    fn from(bytes: Bytes) -> Self {
        Self {
            bytes,
            allocation: None,
            allocation_offset: 0,
            native: None,
        }
    }
}

impl Deref for OwnedPayload {
    type Target = Bytes;
    fn deref(&self) -> &Self::Target {
        &self.bytes
    }
}

impl AsRef<[u8]> for OwnedPayload {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}

impl<T: AsRef<[u8]> + ?Sized> PartialEq<T> for OwnedPayload {
    fn eq(&self, other: &T) -> bool {
        self.as_ref() == other.as_ref()
    }
}
impl Eq for OwnedPayload {}

impl std::fmt::Debug for OwnedPayload {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("OwnedPayload")
            .field("length", &self.bytes.len())
            .field("allocation_offset", &self.allocation_offset)
            .field("capacity", &self.allocation.as_ref().map(|owner| owner.capacity))
            .finish_non_exhaustive()
    }
}
