// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Placement selector — chooses racks/nodes/disk-groups for strip blocks.
//!
//! Mirror: N distinct nodes across distinct racks (design §7.1).
//! EC: rack-aware safe/unsafe modes (design §7.2).

pub mod ec;
pub mod mirror;

use std::collections::HashMap;

use crowdb_protocol::{DiskGroupId, NodeId, RackId};
use serde::{Deserialize, Serialize};

use crate::topology::TopologySnapshot;

/// Re-export the placement selector trait + implementations.
pub use ec::EcPlacement;
pub use mirror::MirrorPlacement;

/// Lexicographic failure-domain priority for new placement decisions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FailureDomainPriority {
    /// Protect and balance racks before considering nodes within them.
    #[default]
    RackFirst,
    /// Protect and balance nodes before using rack diversity as a tie-breaker.
    NodeFirst,
}

/// Placement constraints — negative hints for exclusion.
#[derive(Debug, Clone, Default)]
pub struct PlacementConstraints {
    /// Nodes to exclude (e.g. failed or in recovery).
    pub exclude_nodes: Vec<NodeId>,
    /// Racks to exclude.
    pub exclude_racks: Vec<RackId>,
    /// Disk-groups to exclude.
    pub exclude_disk_groups: Vec<DiskGroupId>,
    /// Permit EC placement that exceeds the safe per-node failure bound.
    pub allow_unsafe_ec: bool,
    /// Permit a plan that cannot satisfy every requested failure domain.
    pub allow_degraded_failure_domains: bool,
    /// Ordering used to choose among otherwise eligible domains.
    pub failure_domain_priority: FailureDomainPriority,
}

impl PlacementConstraints {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn exclude_node(mut self, node: NodeId) -> Self {
        self.exclude_nodes.push(node);
        self
    }

    #[must_use]
    pub fn exclude_rack(mut self, rack: RackId) -> Self {
        self.exclude_racks.push(rack);
        self
    }

    #[must_use]
    pub fn exclude_disk_group(mut self, dg: DiskGroupId) -> Self {
        self.exclude_disk_groups.push(dg);
        self
    }

    #[must_use]
    pub fn allow_unsafe_ec(mut self) -> Self {
        self.allow_unsafe_ec = true;
        self
    }

    #[must_use]
    pub fn allow_degraded_failure_domains(mut self) -> Self {
        self.allow_degraded_failure_domains = true;
        self
    }

    #[must_use]
    pub fn with_failure_domain_priority(mut self, priority: FailureDomainPriority) -> Self {
        self.failure_domain_priority = priority;
        self
    }

    /// Check if a rack is excluded.
    pub fn is_rack_excluded(&self, rack: RackId) -> bool {
        self.exclude_racks.contains(&rack)
    }

    /// Check if a node is excluded.
    pub fn is_node_excluded(&self, node: NodeId) -> bool {
        self.exclude_nodes.contains(&node)
    }

    /// Check if a disk-group is excluded.
    pub fn is_dg_excluded(&self, dg: DiskGroupId) -> bool {
        self.exclude_disk_groups.contains(&dg)
    }
}

/// A single placement decision: where to place `block_count` blocks.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlacementEntry {
    pub rack_id: RackId,
    pub node_id: NodeId,
    pub disk_group_id: DiskGroupId,
    pub block_count: u32,
}

/// The output of a placement selection — a list of entries.
#[derive(Debug, Clone)]
pub struct PlacementPlan {
    pub entries: Vec<PlacementEntry>,
    /// Whether every failure domain assessable before disk allocation is protected.
    pub safe_mode: bool,
    pub priority: FailureDomainPriority,
    pub protection: PlacementProtection,
}

impl PlacementPlan {
    pub fn total_blocks(&self) -> u32 {
        self.entries.iter().map(|e| e.block_count).sum()
    }
}

/// Rack and node protection assessed before DiskDB chooses physical disks.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct PlacementProtection {
    pub loss_budget: u32,
    pub max_fragments_per_rack: u32,
    pub max_fragments_per_node: u32,
    pub rack_protected: bool,
    pub node_protected: bool,
}

impl PlacementProtection {
    #[must_use]
    pub fn degraded(self) -> bool {
        !self.rack_protected || !self.node_protected
    }
}

/// Placement error.
#[derive(Debug, thiserror::Error)]
pub enum PlacementError {
    #[error("insufficient nodes: need {needed}, have {available}")]
    InsufficientNodes { needed: usize, available: usize },
    #[error("insufficient capacity after applying exclusion hints")]
    InsufficientCapacity,
    #[error("no healthy disk-groups available")]
    NoHealthyDiskGroups,
    #[error("safe EC placement is unavailable and unsafe placement was not enabled")]
    UnsafePlacementRequired,
    #[error("rack protection is unavailable: loss budget {loss_budget}, maximum fragments {actual}")]
    RackProtectionUnavailable { loss_budget: u32, actual: u32 },
    #[error("node protection is unavailable: loss budget {loss_budget}, maximum fragments {actual}")]
    NodeProtectionUnavailable { loss_budget: u32, actual: u32 },
    #[error("disk protection is unavailable: loss budget {loss_budget}, maximum fragments {actual}")]
    DiskProtectionUnavailable { loss_budget: u32, actual: u32 },
    #[error("invalid placement shape: {0}")]
    InvalidShape(String),
}

pub(super) fn assess_entries(entries: &[PlacementEntry], loss_budget: u32) -> PlacementProtection {
    let mut rack_counts = HashMap::<RackId, u32>::new();
    let mut node_counts = HashMap::<NodeId, u32>::new();
    for entry in entries {
        *rack_counts.entry(entry.rack_id).or_default() += entry.block_count;
        *node_counts.entry(entry.node_id).or_default() += entry.block_count;
    }
    let max_fragments_per_rack = rack_counts.values().copied().max().unwrap_or(0);
    let max_fragments_per_node = node_counts.values().copied().max().unwrap_or(0);
    PlacementProtection {
        loss_budget,
        max_fragments_per_rack,
        max_fragments_per_node,
        rack_protected: max_fragments_per_rack <= loss_budget,
        node_protected: max_fragments_per_node <= loss_budget,
    }
}

pub(super) fn finish_plan(
    entries: Vec<PlacementEntry>,
    loss_budget: u32,
    constraints: &PlacementConstraints,
    ec_shape: bool,
) -> Result<PlacementPlan, PlacementError> {
    let protection = assess_entries(&entries, loss_budget);
    if ec_shape && protection.max_fragments_per_node > loss_budget && !constraints.allow_unsafe_ec {
        return Err(PlacementError::UnsafePlacementRequired);
    }
    // A single-copy mirror has no recoverable domain-loss budget. It still
    // reports unprotected domains, but must remain allocatable as an explicit
    // non-redundant shape.
    if loss_budget > 0 && protection.degraded() && !constraints.allow_degraded_failure_domains {
        if !protection.rack_protected {
            return Err(PlacementError::RackProtectionUnavailable {
                loss_budget,
                actual: protection.max_fragments_per_rack,
            });
        }
        return Err(PlacementError::NodeProtectionUnavailable {
            loss_budget,
            actual: protection.max_fragments_per_node,
        });
    }
    Ok(PlacementPlan {
        entries,
        safe_mode: !protection.degraded(),
        priority: constraints.failure_domain_priority,
        protection,
    })
}

/// Filter healthy disk-groups from the snapshot, applying exclusion hints.
fn healthy_dgs(
    snap: &TopologySnapshot,
    constraints: &PlacementConstraints,
) -> Vec<crowdb_protocol::sysdata::DiskGroupEntry> {
    snap.healthy_disk_groups()
        .into_iter()
        .filter(|dg| {
            !constraints.is_rack_excluded(dg.rack_id)
                && !constraints.is_node_excluded(dg.node_id)
                && !constraints.is_dg_excluded(dg.dg_id)
        })
        .cloned()
        .collect()
}
