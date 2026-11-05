// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
use super::{record, Collector, LegacyRetirementPending, LegacyRetirementProgress};
use crowdb_protocol::{
    chunkdb::rpc::{Chunk, ChunkState, DeleteChunkRequest, QueryChunkRequest, Strip, StripType},
    common::ChunkId,
};
use crowdb_tree_ffi::{chunk::legacy::LegacyManifest, CtError};
use std::time::{Duration, Instant};

impl Collector {
    pub(super) async fn retire_key(
        &self,
        key: &[u8],
        state: &mut record::Namespace,
        progress: &mut LegacyRetirementProgress,
    ) -> Result<bool, CtError> {
        let (tree, kind, generation) = record::parse_key(key)?;
        if kind != b"legacy-migration" {
            return Ok(true);
        }
        let (bytes, _) = self.get(key).await?.ok_or(CtError::Corruption)?;
        let manifest = LegacyManifest::decode(&bytes)?;
        if manifest.tree_id() != tree || manifest.generation() != generation {
            return Err(CtError::Corruption);
        }
        let Some(pack) = manifest.packs().nth(state.pack_index) else {
            return Ok(true);
        };
        let id = ChunkId {
            high: pack.chunk_high,
            low: pack.chunk_low,
        };
        let end = pack
            .offset
            .checked_add(u64::from(pack.length))
            .ok_or(CtError::Corruption)?;
        self.retire_allocation(id, end, state, progress).await?;
        if state.pending.is_none() {
            state.pack_index += 1;
        }
        let finished = state.pack_index == manifest.packs().len();
        Ok(finished)
    }
    async fn query(&self, id: ChunkId) -> Result<Option<Chunk>, CtError> {
        match self
            .allocator
            .query_chunk(QueryChunkRequest { chunk_id: Some(id) })
            .await
        {
            Ok(response) => response.chunk.map(Some).ok_or(CtError::Corruption),
            Err(crowdb_chunk_client::IoError::ChunkNotFound(_)) => Ok(None),
            Err(_) => Err(CtError::Unavailable),
        }
    }
    async fn retire_allocation(
        &self,
        id: ChunkId,
        end: u64,
        state: &mut record::Namespace,
        progress: &mut LegacyRetirementProgress,
    ) -> Result<(), CtError> {
        let key = record::allocation_key(id.high, id.low);
        let (mut receipt, revision) = if let Some((bytes, revision)) = self.get(&key).await? {
            (record::decode::<record::AllocationReceipt>(&bytes)?, revision)
        } else {
            let Some(chunk) = self.query(id).await? else {
                state.pending = Some(LegacyRetirementPending::MissingAllocation {
                    high: id.high,
                    low: id.low,
                });
                return Ok(());
            };
            if !matches_allocation(&chunk, id, end) {
                state.pending = Some(LegacyRetirementPending::AllocationChanged {
                    high: id.high,
                    low: id.low,
                });
                return Ok(());
            }
            let receipt = record::AllocationReceipt {
                version: 1,
                physical_bytes: physical_bytes(&chunk)?,
                allocation: chunk,
                complete: false,
            };
            self.put(&key, &record::encode(&receipt)?, 0).await?;
            let (bytes, revision) = self.get(&key).await?.ok_or(CtError::Unavailable)?;
            (record::decode::<record::AllocationReceipt>(&bytes)?, revision)
        };
        if receipt.version != 1 || receipt.allocation.id != Some(id) {
            return Err(CtError::Corruption);
        }
        if receipt.complete {
            return Ok(());
        }
        let Some(current) = self.query(id).await? else {
            state.pending = Some(LegacyRetirementPending::MissingAllocation {
                high: id.high,
                low: id.low,
            });
            return Ok(());
        };
        if current.id != Some(id)
            || current.create_ts_ms != receipt.allocation.create_ts_ms
            || current.chunk_type != receipt.allocation.chunk_type
        {
            state.pending = Some(LegacyRetirementPending::AllocationChanged {
                high: id.high,
                low: id.low,
            });
            return Ok(());
        }
        if !physically_deleted(&current) {
            if current.strips != receipt.allocation.strips || !current.cleanup_intents.is_empty() {
                state.pending = Some(LegacyRetirementPending::AllocationChanged {
                    high: id.high,
                    low: id.low,
                });
                return Ok(());
            }
            self.drain(&receipt.allocation).await?;
            let deleted = self
                .allocator
                .delete_chunk(DeleteChunkRequest { chunk_id: Some(id) })
                .await
                .map_err(|_| CtError::Unavailable)?
                .chunk
                .ok_or(CtError::Corruption)?;
            if deleted.id != Some(id)
                || deleted.create_ts_ms != receipt.allocation.create_ts_ms
                || !physically_deleted(&deleted)
            {
                return Err(CtError::Unavailable);
            }
        }
        receipt.complete = true;
        self.put(&key, &record::encode(&receipt)?, revision).await?;
        progress.reclaimed_chunks += 1;
        progress.reclaimed_bytes = progress
            .reclaimed_bytes
            .checked_add(receipt.physical_bytes)
            .ok_or(CtError::Corruption)?;
        Ok(())
    }
    async fn drain(&self, chunk: &Chunk) -> Result<(), CtError> {
        let deadline = Instant::now() + Duration::from_secs(10);
        for strip in &chunk.strips {
            let Some(Strip::MirrorStrip(mirror)) = &strip.strip else {
                return Err(CtError::Corruption);
            };
            for segment in &mirror.segments {
                self.disks
                    .drain_segment(segment, u64::from(strip.unit_kb) * 1024, deadline)
                    .await
                    .map_err(|_| CtError::Unavailable)?;
            }
        }
        Ok(())
    }
}
fn physically_deleted(chunk: &Chunk) -> bool {
    chunk.state == ChunkState::Deleted as i32
        && chunk.strips.is_empty()
        && chunk.capacity == 0
        && chunk.cleanup_intents.is_empty()
}
fn matches_allocation(chunk: &Chunk, id: ChunkId, end: u64) -> bool {
    chunk.id == Some(id)
        && matches!(chunk.chunk_type, 2 | 3)
        && u64::try_from(chunk.chunk_type).ok() == Some(id.high >> 56)
        && chunk.create_ts_ms != 0
        && chunk.cleanup_intents.is_empty()
        && (physically_deleted(chunk) || u64::from(chunk.capacity) >= end)
}
fn physical_bytes(chunk: &Chunk) -> Result<u64, CtError> {
    let mut bytes = 0_u64;
    for strip in &chunk.strips {
        let Some(Strip::MirrorStrip(mirror)) = &strip.strip else {
            return Err(CtError::Corruption);
        };
        if strip.strip_type != StripType::Mirror as i32 || strip.unit_kb == 0 || mirror.segments.is_empty() {
            return Err(CtError::Corruption);
        }
        for segment in &mirror.segments {
            let segment_bytes = u64::from(segment.unit_count)
                .checked_mul(u64::from(strip.unit_kb))
                .and_then(|units| units.checked_mul(1024))
                .ok_or(CtError::Corruption)?;
            bytes = bytes.checked_add(segment_bytes).ok_or(CtError::Corruption)?;
        }
    }
    Ok(bytes)
}
