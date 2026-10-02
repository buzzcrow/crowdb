// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::sync::Arc;

use crowdb_protocol::chunkdb::rpc::ReserveStripGroupRequest;
use crowdb_protocol::generate_chunk_id;

use super::{IoError, OwnedChunk, Result};

impl OwnedChunk {
    pub(super) fn prefetch_at_half(&mut self) {
        if !self.reservation_mode
            || self.policy.conversion_enabled
            || self.prefetched_group.is_some()
            || self.reserved_strips.len() > self.policy.small_strip_prefetch_count as usize / 2
        {
            return;
        }
        let Some(strip) = self.chunk.strips.last() else {
            return;
        };
        let strip_kb = strip.capacity;
        let append_offset = self
            .reserved_strips
            .back()
            .unwrap_or(strip)
            .chunk_offset
            .saturating_add(self.reserved_strips.back().unwrap_or(strip).capacity);
        let remaining = self
            .policy
            .chunk_capacity
            .saturating_sub(u64::from(append_offset) * 1024);
        let count = self
            .policy
            .small_strip_prefetch_count
            .min(u32::try_from(remaining / (u64::from(strip_kb) * 1024)).unwrap_or(u32::MAX));
        if count == 0 {
            return;
        }
        let allocator = Arc::clone(&self.allocator);
        let chunk = self.chunk.clone();
        let group_id = generate_chunk_id(self.policy.chunk_type as u8).to_proto();
        let request = ReserveStripGroupRequest {
            chunk_id: chunk.id,
            reservation_offset_kb: Some(append_offset),
            expected_modify_ts: chunk.modify_ts,
            group_id: Some(group_id),
            writer_epoch: self.writer_epoch,
            lease_generation: self.reservation_generation,
            lease_ms: u64::try_from(self.policy.writer_lease.as_millis()).unwrap_or(u64::MAX),
            strip_size: strip_kb.checked_div(strip.unit_kb).unwrap_or(0).max(1),
            strip_count: count,
            copy_count: self.policy.mirror_copies,
            conversion_data_num: 0,
            conversion_code_num: 0,
        };
        let metrics = Arc::clone(&self.metrics);
        self.prefetched_group = Some(tokio::spawn(async move {
            let started = std::time::Instant::now();
            let result = allocator.reserve_strip_group(request).await.and_then(|response| {
                let returned_offset = response
                    .group
                    .as_ref()
                    .and_then(|group| group.strips.first())
                    .map(|strip| strip.chunk_offset);
                if returned_offset != Some(append_offset) {
                    return Err(IoError::MetadataConflict(
                        "prefetch append offset mismatch".into(),
                    ));
                }
                Ok(response)
            });
            metrics.record_reservation_wait(started.elapsed());
            result
        }));
    }

    pub(super) async fn install_prefetched_group(&mut self) -> Result<()> {
        let pending = self
            .prefetched_group
            .take()
            .ok_or_else(|| IoError::Internal("prefetch task missing".into()))?;
        let response = pending
            .await
            .map_err(|error| IoError::WriteFailed(error.to_string()))??;
        self.apply_reservation_response(response)
    }

    pub(super) fn apply_reservation_response(
        &mut self,
        response: crowdb_protocol::chunkdb::rpc::ReserveStripGroupResponse,
    ) -> Result<()> {
        let chunk = response
            .chunk
            .ok_or_else(|| IoError::AllocationFailed("prefetch missing chunk".into()))?;
        if chunk.modify_ts > self.chunk.modify_ts {
            self.chunk = chunk;
        }
        let group = response
            .group
            .ok_or_else(|| IoError::AllocationFailed("prefetch missing reservation group".into()))?;
        self.reservation_group_id = group.group_id;
        self.reservation_first_sequence = group.strips.first().map(|strip| strip.strip_sequence);
        self.reservation_parity_segments = group.parity_segments;
        self.reserved_strips = group.strips.into();
        Ok(())
    }
}
