// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Unique producer storage before immutable buffer publication.

use std::mem::MaybeUninit;
use std::sync::Arc;

use crate::{sys, Buffer, RpcError, RpcServer};

/// Retained allocation source shared with a service's existing RPC transport.
#[derive(Clone, Debug)]
pub struct ProducerBufferAllocator {
    server: Arc<RpcServer>,
    require_registered: bool,
}

impl ProducerBufferAllocator {
    #[must_use]
    pub fn new(server: Arc<RpcServer>, require_registered: bool) -> Self {
        Self {
            server,
            require_registered,
        }
    }

    #[must_use]
    pub fn allocate(&self, capacity: u32) -> Option<WritableBuffer> {
        self.server
            .allocate_producer_buffer(capacity, self.require_registered)
    }
}

/// A unique allocation that cannot be submitted or cloned while writable.
/// Its storage retains the native recycler independently of the server.
#[derive(Debug)]
pub struct WritableBuffer {
    buffer: Buffer,
}

impl WritableBuffer {
    #[must_use]
    pub fn capacity(&self) -> usize {
        self.buffer.lease_metadata().capacity as usize
    }

    #[must_use]
    pub fn registration_id(&self) -> u32 {
        self.buffer.lease_metadata().registration_id
    }

    /// Spare storage for an I/O producer. No initialized bytes are published.
    ///
    /// # Panics
    /// Panics if foreign code has violated this allocation's unique ownership.
    pub fn uninit_bytes_mut(&mut self) -> &mut [MaybeUninit<u8>] {
        let capacity = self.capacity();
        // SAFETY: this type alone owns the uncommitted native allocation.
        let pointer = unsafe { sys::crowdb_rpc_buffer_mutable_data(self.buffer.handle()) };
        assert!(
            !pointer.is_null(),
            "producer buffer is no longer uniquely writable"
        );
        // SAFETY: native capacity covers this allocation and MaybeUninit does
        // not require its bytes to have been initialized. The mutable borrow
        // prevents publication or release until the returned view expires.
        unsafe { std::slice::from_raw_parts_mut(pointer.cast::<MaybeUninit<u8>>(), capacity) }
    }

    /// Initialize from caller bytes and publish an immutable buffer.
    ///
    /// # Errors
    /// Rejects input larger than the allocation.
    pub fn write(mut self, bytes: &[u8]) -> Result<Buffer, RpcError> {
        if bytes.len() > self.capacity() {
            return Err(RpcError::InvalidArg);
        }
        self.buffer.write(bytes);
        Ok(self.buffer)
    }

    /// Publish an initialized prefix without copying it.
    ///
    /// # Safety
    /// The caller must have initialized every byte in `0..initialized_length`
    /// and must have ended all writes and external mutable aliases to it.
    ///
    /// # Errors
    /// Rejects an empty, oversized or no-longer-unique allocation.
    pub unsafe fn assume_init(self, initialized_length: usize) -> Result<Buffer, RpcError> {
        let length = u32::try_from(initialized_length).map_err(|_| RpcError::InvalidArg)?;
        // SAFETY: the caller guarantees initialized storage; native code also
        // checks capacity, unique ownership and the uncommitted state.
        if unsafe { sys::crowdb_rpc_buffer_commit(self.buffer.handle(), length) } == 0 {
            Err(RpcError::InvalidArg)
        } else {
            Ok(self.buffer)
        }
    }
}

impl RpcServer {
    /// Allocate bounded producer storage. Required registration never falls
    /// back to system allocation; an exhausted active arena also returns None.
    /// Capacity is limited to the largest host-buffer class (1 MiB).
    #[must_use]
    pub fn allocate_producer_buffer(
        &self,
        capacity: u32,
        require_registered: bool,
    ) -> Option<WritableBuffer> {
        // SAFETY: the server is borrowed; the returned lease owns its storage.
        let handle = unsafe {
            sys::crowdb_rpc_server_alloc_producer_buffer(
                self.handle(),
                capacity,
                i32::from(require_registered),
            )
        };
        (!handle.is_null()).then(|| WritableBuffer {
            buffer: Buffer::from_raw(handle),
        })
    }
}
