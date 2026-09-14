// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! EC placement — distribute data_num + code_num blocks across racks.
//!
//! Design §7.2:
//! - Safe mode: max `code_num` blocks per node (guarantees
//!   recoverability — losing `code_num` blocks still has enough data).
//! - Unsafe mode: relax per-node limit when cluster too small for safe
//!   mode. Logs a warning; allocation succeeds with reduced fault
//!   tolerance.

use std::collections::{HashMap, HashSet};

use tracing::warn;

use crowdb_protocol::{NodeId, RackId};

use super::{
    finish_plan, healthy_dgs, FailureDomainPriority, PlacementConstraints, PlacementEntry, PlacementError,
    PlacementPlan,
};
use crate::topology::TopologySnapshot;

/// EC placement selector.
pub struct EcPlacement;

impl EcPlacement {
    /// Select placement for `data_num + code_num` blocks.
    ///
    /// # Errors
    /// Returns `PlacementError::NoHealthyDiskGroups` if no disk-groups
    /// pass the health + exclusion filters, or
    /// `PlacementError::InsufficientNodes` if fewer nodes than blocks
    /// even in unsafe mode.
    pub fn select(
        snap: &TopologySnapshot,
        data_num: usize,
        code_num: usize,
        constraints: &PlacementConstraints,
    ) -> Result<PlacementPlan, PlacementError> {
        if data_num == 0 || code_num == 0 {
            return Err(PlacementError::InvalidShape(
                "EC data_num and code_num must both be non-zero".to_string(),
            ));
        }
        let total_blocks = data_num + code_num;
        let dgs = healthy_dgs(snap, constraints);
        if dgs.is_empty() {
            return Err(PlacementError::NoHealthyDiskGroups);
        }

        // Group disk-groups by rack → node.
        let mut by_rack: HashMap<RackId, Vec<&crowdb_protocol::sysdata::DiskGroupEntry>> = HashMap::new();
        for dg in &dgs {
            by_rack.entry(dg.rack_id).or_default().push(dg);
        }

        let node_count = dgs.iter().map(|dg| dg.node_id).collect::<HashSet<_>>().len();

        // Try safe mode first: max `code_num` blocks per node.
        let safe_plan = try_distribute(
            &by_rack,
            total_blocks,
            code_num,
            constraints.failure_domain_priority,
        );
        if let Some(entries) = safe_plan {
            return finish_plan(
                entries,
                u32::try_from(code_num).unwrap_or(u32::MAX),
                constraints,
                true,
            );
        }

        if !constraints.allow_unsafe_ec {
            return Err(PlacementError::UnsafePlacementRequired);
        }

        // Fall back to unsafe mode: max `total_blocks` per node (i.e.
        // no practical limit — just spread as evenly as possible).
        warn!(
            data_num,
            code_num,
            nodes = node_count,
            "EC placement: safe mode failed, falling back to unsafe mode"
        );
        let unsafe_limit = total_blocks;
        if let Some(entries) = try_distribute(
            &by_rack,
            total_blocks,
            unsafe_limit,
            constraints.failure_domain_priority,
        ) {
            return finish_plan(
                entries,
                u32::try_from(code_num).unwrap_or(u32::MAX),
                constraints,
                true,
            );
        }

        // Not enough nodes even for unsafe mode.
        Err(PlacementError::InsufficientNodes {
            needed: total_blocks,
            available: node_count,
        })
    }
}

/// Try to distribute `total_blocks` across nodes, with max
/// `max_per_node` blocks per node. Distributes across racks first
/// (round-robin), then within each rack across nodes.
fn try_distribute(
    by_rack: &HashMap<RackId, Vec<&crowdb_protocol::sysdata::DiskGroupEntry>>,
    total_blocks: usize,
    max_per_node: usize,
    priority: FailureDomainPriority,
) -> Option<Vec<PlacementEntry>> {
    let mut rack_ids: Vec<RackId> = by_rack.keys().copied().collect();
    rack_ids.sort_unstable();

    let mut node_load: HashMap<NodeId, u32> = HashMap::new();
    let mut rack_load: HashMap<RackId, u32> = HashMap::new();
    let mut entries: Vec<PlacementEntry> = Vec::new();
    let mut placed = 0;
    let mut rack_index = 0;

    while placed < total_blocks {
        let candidate = match priority {
            FailureDomainPriority::RackFirst => {
                let rack = rack_ids[rack_index];
                rack_index = (rack_index + 1) % rack_ids.len();
                by_rack
                    .get(&rack)?
                    .iter()
                    .copied()
                    .filter(|dg| {
                        let load = node_load.get(&dg.node_id).copied().unwrap_or(0);
                        (load as usize) < max_per_node
                    })
                    .min_by_key(|dg| {
                        (
                            node_load.get(&dg.node_id).copied().unwrap_or(0),
                            dg.node_id,
                            dg.dg_id,
                        )
                    })
            }
            FailureDomainPriority::NodeFirst => by_rack
                .values()
                .flatten()
                .copied()
                .filter(|dg| {
                    let load = node_load.get(&dg.node_id).copied().unwrap_or(0);
                    (load as usize) < max_per_node
                })
                .min_by_key(|dg| {
                    (
                        node_load.get(&dg.node_id).copied().unwrap_or(0),
                        rack_load.get(&dg.rack_id).copied().unwrap_or(0),
                        dg.node_id,
                        dg.dg_id,
                    )
                }),
        };

        if let Some(dg) = candidate {
            *node_load.entry(dg.node_id).or_insert(0) += 1;
            *rack_load.entry(dg.rack_id).or_insert(0) += 1;
            entries.push(PlacementEntry {
                rack_id: dg.rack_id,
                node_id: dg.node_id,
                disk_group_id: dg.dg_id,
                block_count: 1,
            });
            placed += 1;
        } else {
            // No node in this rack has capacity. Check if any rack
            // still has capacity — if we've cycled through all racks
            // without placing a block, safe mode fails.
            let all_full = rack_ids.iter().all(|r| {
                by_rack.get(r).is_some_and(|dgs| {
                    dgs.iter().all(|dg| {
                        let load = node_load.get(&dg.node_id).copied().unwrap_or(0);
                        (load as usize) >= max_per_node
                    })
                })
            });
            if all_full {
                return None;
            }
        }
    }

    Some(entries)
}
