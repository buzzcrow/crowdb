// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{advance_chunk, Arc, ChunkState, IoError, OwnedChunk, Result};

impl OwnedChunk {
    pub(super) async fn renew_liveness(&mut self) -> Result<()> {
        let staged = self
            .staged_reservation
            .map(|(_, _, sequence)| {
                self.chunk
                    .strips
                    .iter()
                    .find(|strip| strip.strip_sequence == sequence)
                    .cloned()
                    .ok_or_else(|| IoError::MetadataConflict("staged reservation strip is missing".into()))
            })
            .transpose()?;
        self.flush_pending_advance().await?;
        let chunk_id = self
            .chunk
            .id
            .ok_or_else(|| IoError::AllocationFailed("shared chunk missing id".into()))?;
        let mut refreshed = advance_chunk(
            Arc::clone(&self.allocator),
            chunk_id,
            self.writer_epoch,
            self.chunk.modify_ts,
            self.cursor,
            None,
            u64::try_from(self.policy.writer_lease.as_millis()).unwrap_or(u64::MAX),
        )
        .await?;
        if refreshed.id != Some(chunk_id)
            || refreshed.writer_epoch != self.writer_epoch
            || refreshed.state != ChunkState::Active as i32
        {
            return Err(IoError::MetadataConflict(
                "shared chunk ownership changed during lease renewal".into(),
            ));
        }
        if let Some(staged) = staged {
            // Unconsumed reservations are absent from the readable layout.
            // Retain this local write view without advancing published capacity.
            match refreshed
                .strips
                .iter()
                .find(|strip| strip.strip_sequence == staged.strip_sequence)
            {
                Some(current) if current != &staged => {
                    return Err(IoError::MetadataConflict(
                        "staged reservation changed during lease renewal".into(),
                    ));
                }
                Some(_) => {}
                None => refreshed.strips.push(staged),
            }
        }
        self.chunk = refreshed;
        Ok(())
    }
}
