// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Safe producer initialization into payload slots with reserved frame space.

use super::{NativeFramedOwner, NativeOwner, NativePayloadView};
use crowdb_protocol::frame::{
    FrameMagic, FRAME_FOOTER_BYTES, FRAME_HEADER_PREFIX_BYTES, MAX_FRAME_BYTES, MAX_FRAME_PAYLOAD_BYTES,
};
use crowdb_rpc_ffi::WritableBuffer;
use hyper::body::Bytes;
use std::io;
use std::ptr::NonNull;
use std::sync::Arc;

impl NativeFramedOwner {
    /// Initializes disjoint payload slots in the producer's final allocation.
    /// The callback receives the logical offset; it cannot retain mutable views.
    /// `lifetime` remains owned until the final payload/physical view is released.
    /// # Errors
    /// Rejects empty input or a logical length exceeding complete physical slots.
    /// # Panics
    /// Propagates initializer panics, or a violated foreign allocation invariant.
    pub fn initialize_producer(
        mut buffer: WritableBuffer,
        length: usize,
        initial: u8,
        lifetime: Box<dyn Send + Sync>,
        mut initialize: impl FnMut(usize, &mut [u8]),
    ) -> io::Result<Self> {
        let capacity = buffer.capacity();
        let pointer = NonNull::new(buffer.uninit_bytes_mut().as_mut_ptr().cast::<u8>())
            .expect("native producer allocation is non-null");
        let owner = NativeOwner {
            pointer,
            capacity,
            allocator: None,
            native: Some(buffer),
            _lifetime: Some(lifetime),
        };
        if length == 0 || length > capacity / MAX_FRAME_BYTES * MAX_FRAME_PAYLOAD_BYTES {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid framed producer length",
            ));
        }
        let mut lengths = Vec::with_capacity(length.div_ceil(MAX_FRAME_PAYLOAD_BYTES));
        for (index, offset) in (0..length).step_by(MAX_FRAME_PAYLOAD_BYTES).enumerate() {
            let count = (length - offset).min(MAX_FRAME_PAYLOAD_BYTES);
            // SAFETY: this unique producer owns all slots. Initialize each
            // payload before exposing its mutable callback slice; reserved
            // headers/footers stay inaccessible until one-time finalization.
            let payload = unsafe {
                let pointer = owner
                    .pointer
                    .as_ptr()
                    .add(index * MAX_FRAME_BYTES + FRAME_HEADER_PREFIX_BYTES);
                pointer.write_bytes(initial, count);
                std::slice::from_raw_parts_mut(pointer, count)
            };
            initialize(offset, payload);
            lengths.push(u16::try_from(count).expect("frame payload fits u16"));
        }
        let last = usize::from(*lengths.last().expect("nonempty payload"));
        Ok(Self {
            owner: Arc::new(owner),
            logical_len: length as u64,
            physical_len: (lengths.len() - 1) * MAX_FRAME_BYTES
                + FRAME_HEADER_PREFIX_BYTES
                + last
                + FRAME_FOOTER_BYTES,
            prepared_slots: 0,
            prepared: false,
            magic: FrameMagic::RepoLargeV1,
            finalized_chunks: vec![None; lengths.len()].into_boxed_slice(),
            payload_lengths: lengths.into_boxed_slice(),
        })
    }

    #[must_use]
    pub fn allocation_capacity(&self) -> usize {
        self.owner.capacity
    }

    #[must_use]
    pub fn registration_id(&self) -> u32 {
        self.owner
            .native
            .as_ref()
            .map_or(0, WritableBuffer::registration_id)
    }

    /// Immutable logical views for checksumming without materializing a copy.
    #[must_use]
    pub fn payload_views(&self) -> Vec<Bytes> {
        self.payload_lengths
            .iter()
            .enumerate()
            .map(|(index, length)| {
                Bytes::from_owner(NativePayloadView {
                    owner: Arc::clone(&self.owner),
                    payload_offset: index * MAX_FRAME_BYTES + FRAME_HEADER_PREFIX_BYTES,
                    initialized: usize::from(*length),
                })
            })
            .collect()
    }
}
