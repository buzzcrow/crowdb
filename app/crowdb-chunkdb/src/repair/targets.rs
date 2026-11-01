// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Checkpointed tentative repair destinations and replacement assembly.

use super::{replace_segment, segment_size, strip_segments, RecoveredShards, RepairRunError};
use super::{RepairStripTaskHandler, RepairStripTaskV1};
use crate::ad_hoc::RecoveryKey;
use bytes::Bytes;
use crowdb_protocol::chunk_task::{ChunkTaskValue, RepairTargetCheckpoint, RepairTargetPhase};
use crowdb_protocol::chunkdb::rpc::{Chunk, ChunkStrip, Strip};
use crowdb_protocol::common::DiskId;
use crowdb_protocol::diskdb::rpc::Segment;

impl RepairStripTaskHandler {
    pub(super) async fn reconcile_allocated_targets(
        &self,
        task: &ChunkTaskValue,
        payload: &mut RepairStripTaskV1,
    ) -> Result<(), RepairRunError> {
        if !payload
            .targets
            .iter()
            .any(|target| target.phase == RepairTargetPhase::Allocated)
        {
            return Ok(());
        }
        if payload.targets.iter().any(|target| {
            target.phase == RepairTargetPhase::Allocated && target.destination.disk_id.is_none()
        }) {
            return Err(RepairRunError::Permanent(
                "repair destination has no disk identity".into(),
            ));
        }
        let snapshot = self.lifecycle.topology_snapshot();
        if snapshot.is_empty() {
            return Err(RepairRunError::Retry(
                "repair target topology is unavailable".into(),
            ));
        }
        let healthy = snapshot.healthy_disk_groups();
        let previous = payload.targets.len();
        payload.targets.retain(|target| {
            target.phase != RepairTargetPhase::Allocated
                || target
                    .destination
                    .disk_id
                    .is_some_and(|disk| healthy.iter().any(|group| group.value.disk_ids.contains(&disk)))
        });
        if payload.targets.len() != previous {
            // Publish removal before allocating another destination. The exact
            // old tentative block remains in DiskDB; its owner scanner applies
            // the normal persisted absence grace, including after reply loss.
            self.checkpoint(task, payload).await?;
        }
        Ok(())
    }

    pub(super) async fn prepare_replacement(
        &self,
        task: &ChunkTaskValue,
        payload: &mut RepairStripTaskV1,
        chunk: &Chunk,
        strip: &ChunkStrip,
        recovered: &RecoveredShards,
        unavailable: &[Segment],
    ) -> Result<ChunkStrip, RepairRunError> {
        let segments = strip_segments(strip)?;
        let unit_bytes = u64::from(strip.unit_kb) * 1024;
        let shard_bytes = segment_size(&segments[0], unit_bytes)?;
        let mut replacement = strip.clone();
        let mut surviving: Vec<_> = segments
            .iter()
            .copied()
            .filter(|segment| !unavailable.contains(segment))
            .collect();
        // Preserve disk separation even when the node policy is relaxed.
        let mut excluded: Vec<_> = segments.iter().filter_map(|segment| segment.disk_id).collect();
        for failed in unavailable {
            let index = segments
                .iter()
                .position(|segment| segment == failed)
                .ok_or_else(|| RepairRunError::Permanent("failed segment disappeared".into()))?;
            let target = self
                .target_index(task, payload, failed, &surviving, &excluded)
                .await?;
            let destination = payload.targets[target].destination;
            if payload.targets[target].phase == RepairTargetPhase::Allocated {
                let data = shard_data(chunk, strip, index, shard_bytes, recovered.shards[index].clone());
                self.copy_target(task, payload, target, unit_bytes, data).await?;
            }
            if let Some(shared) = &self.ad_hoc {
                shared.rebuilt(
                    RecoveryKey {
                        chunk_id: payload.chunk_id,
                        strip_sequence: payload.strip_sequence,
                        segment: *failed,
                    },
                    recovered.shards[index].clone(),
                );
            }
            replace_segment(&mut replacement, failed, destination)?;
            surviving.push(destination);
            if let Some(disk_id) = destination.disk_id {
                excluded.push(disk_id);
            }
        }
        replacement
            .unavailable_segments
            .retain(|segment| !unavailable.contains(segment));
        Ok(replacement)
    }

    async fn target_index(
        &self,
        task: &ChunkTaskValue,
        payload: &mut RepairStripTaskV1,
        failed: &Segment,
        surviving: &[Segment],
        excluded: &[DiskId],
    ) -> Result<usize, RepairRunError> {
        if let Some(index) = payload.targets.iter().position(|target| target.source == *failed) {
            return Ok(index);
        }
        let destination = self
            .lifecycle
            .allocate_repair_segment(
                &payload.chunk_id,
                failed,
                surviving,
                excluded,
                self.allow_unsafe_placement,
            )
            .await?;
        payload.targets.push(RepairTargetCheckpoint {
            source: *failed,
            destination,
            phase: RepairTargetPhase::Allocated,
        });
        self.checkpoint(task, payload).await?;
        Ok(payload.targets.len().saturating_sub(1))
    }

    async fn copy_target(
        &self,
        task: &ChunkTaskValue,
        payload: &mut RepairStripTaskV1,
        index: usize,
        unit_bytes: u64,
        data: Bytes,
    ) -> Result<(), RepairRunError> {
        let destination = payload.targets[index].destination;
        if !data.is_empty() {
            self.io
                .write_segment(&destination, unit_bytes, data)
                .await
                .map_err(|error| RepairRunError::Retry(error.to_string()))?;
        }
        self.io
            .fsync_segment(&destination)
            .await
            .map_err(|error| RepairRunError::Retry(error.to_string()))?;
        payload.targets[index].phase = RepairTargetPhase::Copied;
        self.checkpoint(task, payload).await
    }
}

fn shard_data(chunk: &Chunk, strip: &ChunkStrip, index: usize, shard_bytes: u64, data: Bytes) -> Bytes {
    let Some(Strip::EcStrip(ec)) = &strip.strip else {
        return data;
    };
    let sealed = if chunk.acknowledged_cursor == 0 {
        u64::from(strip.sealed_length) * 1024
    } else {
        chunk
            .acknowledged_cursor
            .saturating_sub(u64::from(strip.chunk_offset) * 1024)
            .min(u64::from(strip.capacity) * 1024)
    };
    let valid = if index < ec.data_num as usize {
        sealed.saturating_sub(index as u64 * shard_bytes).min(shard_bytes)
    } else {
        sealed.min(shard_bytes)
    };
    data.slice(..usize::try_from(valid).unwrap_or(data.len()))
}
