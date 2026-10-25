// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Immutable framed producers retain runtime credits through physical completion.

use super::{framed_chain::FramedChain, WritableLease};
use crate::{Error, Result};
use crowdb_access_s3::native_buffer::NativeFramedOwner;
use crowdb_chunk_client::FramedWriteBuffer;
use crowdb_protocol::frame::{MAX_FRAME_BYTES, MAX_FRAME_V2_PAYLOAD_BYTES};

impl crate::ClientRuntime {
    /// Copies caller bytes once into final frames across bounded allocations.
    /// # Errors
    /// Rejects empty input, shutdown or exhausted producer capacity. Partial
    /// allocation failures release every admitted owner before returning.
    pub fn prepare_framed(&self, data: &[u8]) -> Result<FrozenObjectBuffer> {
        let allocator =
            self.storage.chunks.producer_allocator().ok_or_else(|| {
                Error::Configuration("data transport has no native producer allocator".into())
            })?;
        self.resources.prepare_framed(&allocator, data)
    }
}

impl crate::RuntimeResources {
    /// Initialize final frame storage using the transport's 1-MiB size limit.
    /// # Errors
    /// Returns invalid input or capacity exhaustion, releasing partial work.
    pub fn prepare_framed(
        &self,
        allocator: &crowdb_rpc_ffi::ProducerBufferAllocator,
        data: &[u8],
    ) -> Result<FrozenObjectBuffer> {
        let logical_capacity = (1 << 20) / MAX_FRAME_BYTES * MAX_FRAME_V2_PAYLOAD_BYTES;
        let mut buffers = Vec::new();
        for payload in data.chunks(logical_capacity) {
            let physical = payload.len().div_ceil(MAX_FRAME_V2_PAYLOAD_BYTES) * MAX_FRAME_BYTES;
            let capacity = u32::try_from(physical).map_err(|_| Error::BufferRange)?;
            buffers.push(self.allocate(allocator, capacity)?.initialize_framed(
                payload.len(),
                0,
                |offset, output| {
                    output.copy_from_slice(&payload[offset..offset + output.len()]);
                },
            )?);
        }
        FrozenObjectBuffer::concatenate(buffers)
    }
}

pub struct FrozenObjectBuffer {
    pub(crate) owner: FramedChain,
    budget: std::sync::Arc<crate::admission::Budget>,
}

impl FrozenObjectBuffer {
    pub(crate) fn belongs_to(&self, resources: &crate::RuntimeResources) -> bool {
        std::sync::Arc::ptr_eq(&self.budget, &resources.producers)
    }
    #[must_use]
    pub fn length(&self) -> u64 {
        self.owner.logical_len()
    }

    #[must_use]
    pub fn allocation_capacity(&self) -> usize {
        self.owner
            .0
            .iter()
            .map(NativeFramedOwner::allocation_capacity)
            .sum()
    }

    #[must_use]
    pub fn registration_id(&self) -> u32 {
        let first = self.owner.0[0].registration_id();
        if self.owner.0.iter().all(|owner| owner.registration_id() == first) {
            first
        } else {
            0
        }
    }

    #[must_use]
    pub fn payload_views(&self) -> Vec<bytes::Bytes> {
        self.owner
            .0
            .iter()
            .flat_map(NativeFramedOwner::payload_views)
            .collect()
    }

    /// Joins final frame allocations without copying their payloads.
    /// # Errors
    /// Rejects empty input, mixed runtime budgets, overflowing lengths, or a
    /// partial interior frame. Only the final allocation may have a short tail.
    pub fn concatenate(buffers: Vec<Self>) -> Result<Self> {
        let first = buffers.first().ok_or(Error::BufferRange)?;
        let budget = std::sync::Arc::clone(&first.budget);
        let mut owners = Vec::new();
        let mut frames = 0usize;
        let mut capacity = 0usize;
        for (index, buffer) in buffers.iter().enumerate() {
            if !std::sync::Arc::ptr_eq(&budget, &buffer.budget)
                || (index + 1 < buffers.len()
                    && buffer.length() != (buffer.owner.frame_count() * MAX_FRAME_V2_PAYLOAD_BYTES) as u64)
            {
                return Err(Error::BufferRange);
            }
            frames = frames
                .checked_add(buffer.owner.frame_count())
                .ok_or(Error::BufferRange)?;
            capacity = capacity
                .checked_add(buffer.allocation_capacity())
                .ok_or(Error::BufferRange)?;
        }
        frames.checked_mul(MAX_FRAME_BYTES).ok_or(Error::BufferRange)?;
        for buffer in buffers {
            owners.extend(buffer.owner.0);
        }
        Ok(Self {
            owner: FramedChain(owners),
            budget,
        })
    }
}

impl WritableLease {
    /// Maximum logical payload with the storage headers and footers reserved.
    #[must_use]
    pub fn framed_capacity(&self) -> usize {
        self.capacity() / MAX_FRAME_BYTES * MAX_FRAME_V2_PAYLOAD_BYTES
    }

    /// Prepares data directly in its final registered storage frame slots.
    /// # Errors
    /// Rejects empty or oversized input. The callback runs once per frame and
    /// receives its logical offset, with only initialized payload bytes exposed.
    pub fn initialize_framed(
        self,
        length: usize,
        initial: u8,
        initialize: impl FnMut(usize, &mut [u8]),
    ) -> Result<FrozenObjectBuffer> {
        let budget = std::sync::Arc::clone(self.credit.budget());
        let owner = NativeFramedOwner::initialize_producer(
            self.buffer,
            length,
            initial,
            Box::new(self.credit),
            initialize,
        )
        .map_err(|_| Error::BufferRange)?;
        Ok(FrozenObjectBuffer {
            owner: FramedChain(vec![owner]),
            budget,
        })
    }
}
