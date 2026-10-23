// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Immutable framed producers retain runtime credits through physical completion.

use super::WritableLease;
use crate::{Error, Result};
use crowdb_access_s3::native_buffer::NativeFramedOwner;
use crowdb_chunk_client::FramedWriteBuffer;
use crowdb_protocol::frame::{MAX_FRAME_BYTES, MAX_FRAME_PAYLOAD_BYTES};

pub struct FrozenObjectBuffer {
    pub(crate) owner: NativeFramedOwner,
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
        self.owner.allocation_capacity()
    }

    #[must_use]
    pub fn registration_id(&self) -> u32 {
        self.owner.registration_id()
    }

    #[must_use]
    pub fn payload_views(&self) -> Vec<bytes::Bytes> {
        self.owner.payload_views()
    }
}

impl WritableLease {
    /// Maximum logical payload with the storage headers and footers reserved.
    #[must_use]
    pub fn framed_capacity(&self) -> usize {
        self.capacity() / MAX_FRAME_BYTES * MAX_FRAME_PAYLOAD_BYTES
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
        Ok(FrozenObjectBuffer { owner, budget })
    }
}
