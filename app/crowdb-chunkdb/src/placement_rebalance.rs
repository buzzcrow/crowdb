// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Low-rate cross-domain relocation planning through DiskDB's durable mover.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use arc_swap::ArcSwapOption;
use crowdb_protocol::chunkdb::rpc::{ChunkStrip, Strip};
use crowdb_protocol::common::ChunkId;
use crowdb_protocol::diskdb::rpc::{ExecuteRelocationRequest, Segment};

use crate::allocator::{assess_physical_placement, DiskdbClientPool};
use crate::chunkdb_config::PlacementRebalanceConfig;
use crate::lifecycle::{LifecycleError, LifecycleHandler};

#[derive(Clone)]
struct PendingMove {
    target_disk_group_id: u64,
    source: Segment,
    target: Segment,
}

pub struct PlacementRebalancePlanner {
    lifecycle: Arc<LifecycleHandler>,
    pool: Arc<DiskdbClientPool>,
    policy: PlacementRebalanceConfig,
    skew_observed_since_ms: AtomicU64,
    scan_cursor: ArcSwapOption<ChunkId>,
    pending: ArcSwapOption<PendingMove>,
}

impl PlacementRebalancePlanner {
    #[must_use]
    pub fn new(
        lifecycle: Arc<LifecycleHandler>,
        pool: Arc<DiskdbClientPool>,
        policy: PlacementRebalanceConfig,
    ) -> Self {
        Self {
            lifecycle,
            pool,
            policy,
            skew_observed_since_ms: AtomicU64::new(0),
            scan_cursor: ArcSwapOption::empty(),
            pending: ArcSwapOption::empty(),
        }
    }

    /// Plan at most the configured number of moves. The current policy caps
    /// this at one move per strip and defaults to one move per cycle.
    pub async fn run_once(&self, now_ms: u64) -> Result<u32, String> {
        if let Some(pending) = self.pending.load_full() {
            self.deliver(&pending).await?;
            self.pending.store(None);
            return Ok(1);
        }
        let snapshot = self.lifecycle.topology_snapshot();
        let Some((source_dg, target_dg)) = select_skewed_pair(&snapshot, &self.policy) else {
            self.skew_observed_since_ms.store(0, Ordering::Release);
            return Ok(0);
        };
        let observed = self.skew_observed_since_ms.load(Ordering::Acquire);
        if observed == 0 {
            self.skew_observed_since_ms
                .store(now_ms.max(1), Ordering::Release);
            if self.policy.hysteresis_secs != 0 {
                return Ok(0);
            }
        } else if now_ms.saturating_sub(observed) < self.policy.hysteresis_secs.saturating_mul(1_000) {
            return Ok(0);
        }

        let chunks = self
            .lifecycle
            .list_chunks(self.scan_cursor.load_full().as_deref(), 256)
            .await
            .map_err(|error| error.to_string())?;
        if chunks.len() < 256 {
            self.scan_cursor.store(None);
        } else if let Some(last) = chunks.last().and_then(|chunk| chunk.id) {
            self.scan_cursor.store(Some(Arc::new(last)));
        }
        for chunk in chunks {
            let Some(chunk_id) = chunk.id else {
                continue;
            };
            for strip in &chunk.strips {
                if strip.placement_repair_required {
                    continue;
                }
                let Some(source) = strip_segments(strip)
                    .iter()
                    .find(|segment| {
                        segment.disk_id.is_some_and(|disk| {
                            snapshot
                                .disk_location(disk)
                                .is_some_and(|location| location.disk_group_id == source_dg)
                        })
                    })
                    .copied()
                else {
                    continue;
                };
                let excluded = strip_segments(strip)
                    .iter()
                    .filter_map(|segment| segment.disk_id)
                    .collect();
                let response = self
                    .pool
                    .allocate_blocks_excluding(target_dg, 1, source.unit_count, &chunk_id, excluded)
                    .await
                    .map_err(|error| error.to_string())?;
                let Some(target) = response.segments.into_iter().next() else {
                    continue;
                };
                if weakens_strip(&snapshot, strip, source, target)? {
                    self.pool.free_blocks(vec![target]).await?;
                    continue;
                }
                let pending = Arc::new(PendingMove {
                    target_disk_group_id: target_dg,
                    source,
                    target,
                });
                self.pending.store(Some(Arc::clone(&pending)));
                self.deliver(&pending).await?;
                self.pending.store(None);
                return Ok(1);
            }
        }
        Ok(0)
    }

    async fn deliver(&self, pending: &PendingMove) -> Result<(), String> {
        self.pool
            .execute_relocation(ExecuteRelocationRequest {
                target_disk_group_id: pending.target_disk_group_id,
                source: Some(pending.source),
                target: Some(pending.target),
            })
            .await
            .map(|_| ())
            .map_err(|error| error.to_string())
    }
}

fn select_skewed_pair(
    snapshot: &crate::topology::TopologySnapshot,
    policy: &PlacementRebalanceConfig,
) -> Option<(u64, u64)> {
    let mut groups: Vec<_> = snapshot
        .healthy_disk_groups()
        .into_iter()
        .filter_map(|group| {
            let capacity = snapshot.disk_group_capacity(group.dg_id)?;
            (capacity.capacity_bytes != 0).then_some((
                group.dg_id,
                capacity.used_bytes,
                capacity.capacity_bytes,
                capacity.free_bytes,
            ))
        })
        .collect();
    groups.sort_unstable_by(|left, right| {
        (u128::from(left.1) * u128::from(right.2))
            .cmp(&(u128::from(right.1) * u128::from(left.2)))
            .then_with(|| left.0.cmp(&right.0))
    });
    let source = groups.last().copied()?;
    let target = groups
        .iter()
        .copied()
        .find(|group| group.0 != source.0 && group.3 >= policy.min_target_free_bytes)?;
    let target_pct = target.1.saturating_mul(100).checked_div(target.2)?;
    let source_pct = source.1.saturating_mul(100).checked_div(source.2)?;
    (source.0 != target.0
        && source_pct.saturating_sub(target_pct) >= u64::from(policy.imbalance_threshold_pct))
    .then_some((source.0, target.0))
}

fn strip_segments(strip: &ChunkStrip) -> &[Segment] {
    match strip.strip.as_ref() {
        Some(Strip::MirrorStrip(mirror)) => &mirror.segments,
        Some(Strip::EcStrip(ec)) => &ec.segments,
        None => &[],
    }
}

fn weakens_strip(
    snapshot: &crate::topology::TopologySnapshot,
    strip: &ChunkStrip,
    source: Segment,
    target: Segment,
) -> Result<bool, String> {
    let mut replacement = strip_segments(strip).to_vec();
    let position = replacement
        .iter()
        .position(|segment| *segment == source)
        .ok_or_else(|| LifecycleError::StateConflict.to_string())?;
    replacement[position] = target;
    let loss_budget = match strip.strip.as_ref() {
        Some(Strip::MirrorStrip(mirror)) => {
            u32::try_from(mirror.segments.len().saturating_sub(1)).unwrap_or(u32::MAX)
        }
        Some(Strip::EcStrip(ec)) => ec.code_num,
        None => return Ok(true),
    };
    let usage_fresh = strip
        .placement_assessment
        .as_ref()
        .is_some_and(|assessment| assessment.usage_fresh);
    let current = assess_physical_placement(
        snapshot,
        strip_segments(strip),
        loss_budget,
        snapshot.generation(),
        usage_fresh,
    );
    let next = assess_physical_placement(
        snapshot,
        &replacement,
        loss_budget,
        snapshot.generation(),
        usage_fresh,
    );
    Ok((current.rack_protected && !next.rack_protected)
        || (current.node_protected && !next.node_protected)
        || (current.disk_protected && !next.disk_protected)
        || next.max_fragments_per_rack > current.max_fragments_per_rack
        || next.max_fragments_per_node > current.max_fragments_per_node
        || next.max_fragments_per_disk > current.max_fragments_per_disk)
}
