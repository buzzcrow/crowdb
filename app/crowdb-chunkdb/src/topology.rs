// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Topology cache for chunkdb placement decisions.
//!
//! `TopologyCache` holds an `Arc<RwLock<TopologySnapshot>>` with the
//! current cluster hierarchy (racks, nodes, disk-groups). Placement
//! (R87) calls `snapshot()` to get a consistent point-in-time view.
//!
//! Update path: periodic full refresh from group-0 via
//! `HardwareClient` + watch/notify for immediate status changes.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use arc_swap::ArcSwap;
use tracing::warn;

use crowdb_protocol::common::{DiskGroupUsageSummary, DiskId, HwStatus};
use crowdb_protocol::sysdata::DiskGroupEntry;
use crowdb_protocol::{DiskGroupId, NodeId, RackId};

use crate::selector::PlacementEntry;

pub mod notify;
pub mod refresh;

/// Group-0 store + group ids (system group).
pub const G0_STORE: u64 = 0;
pub const G0_GROUP: u64 = 0;

/// Watch prefixes for chunkdb topology updates.
pub const CHUNKDB_WATCH_PREFIXES: &[&[u8]] = &[b"/hw/node/", b"/hw/dg/"];

/// `HwStatus::Up` as `i32` (prost represents enums as i32 in messages).
const HW_UP: i32 = HwStatus::Up as i32;

/// Point-in-time immutable topology snapshot.
///
/// Cloned from `TopologyCache` for a single placement decision;
/// concurrent cache updates do not affect the in-flight snapshot.
#[derive(Debug, Clone, Default)]
pub struct TopologySnapshot {
    /// rack_id → (status_i32, node_ids)
    racks: HashMap<RackId, (i32, Vec<NodeId>)>,
    /// (rack_id, node_id) → (status_i32, dg_ids)
    nodes: HashMap<(RackId, NodeId), (i32, Vec<DiskGroupId>)>,
    /// dg_id → disk-group entry (with rack_id, node_id, status)
    disk_groups: HashMap<DiskGroupId, DiskGroupEntry>,
    /// disk_id → physical failure-domain location for post-allocation checks.
    disks: HashMap<DiskId, DiskLocation>,
    /// Capacity observations joined to live disk-group membership.
    usage: HashMap<DiskGroupId, DiskGroupCapacity>,
    /// Monotonic local publication generation.
    generation: u64,
    /// Unit size in bytes (from disk records). Used to convert
    /// `write_granularity` (KB) to `unit_count` for diskdb allocation.
    /// 0 if not yet populated.
    unit_size_bytes: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DiskLocation {
    pub rack_id: RackId,
    pub node_id: NodeId,
    pub disk_group_id: DiskGroupId,
}

#[derive(Debug, Clone)]
pub struct DiskGroupCapacity {
    pub allocatable_disk_count: u32,
    pub capacity_bytes: u64,
    pub used_bytes: u64,
    pub free_bytes: u64,
    pub sampled_at_ms: u64,
    in_flight_bytes: Arc<AtomicU64>,
}

impl DiskGroupCapacity {
    #[must_use]
    pub fn in_flight_bytes(&self) -> u64 {
        self.in_flight_bytes.load(Ordering::Acquire)
    }

    #[must_use]
    pub fn projected_bytes(&self, planned_bytes: u64) -> u64 {
        self.used_bytes
            .saturating_add(self.in_flight_bytes())
            .saturating_add(planned_bytes)
    }
}

/// A normalized utilization value. Known observations sort before unknown
/// observations and are compared without floating-point rounding.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CapacityScore {
    used: u64,
    capacity: u64,
    known: bool,
}

impl Ord for CapacityScore {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        match (self.known, other.known) {
            (true, true) => u128::from(self.used)
                .saturating_mul(u128::from(other.capacity))
                .cmp(&u128::from(other.used).saturating_mul(u128::from(self.capacity))),
            (true, false) => std::cmp::Ordering::Less,
            (false, true) => std::cmp::Ordering::Greater,
            (false, false) => std::cmp::Ordering::Equal,
        }
    }
}

impl PartialOrd for CapacityScore {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

/// Releases projected bytes automatically on success, failure, or cancellation.
#[derive(Debug)]
pub struct InFlightReservation {
    counters: Vec<(Arc<AtomicU64>, u64)>,
}

impl Drop for InFlightReservation {
    fn drop(&mut self) {
        for (counter, bytes) in &self.counters {
            counter.fetch_sub(*bytes, Ordering::AcqRel);
        }
    }
}

impl TopologySnapshot {
    /// All disk-groups with healthy status (rack/node/dg all `Up`).
    pub fn healthy_disk_groups(&self) -> Vec<&DiskGroupEntry> {
        self.disk_groups
            .values()
            .filter(|dg| {
                let dg_ok = dg.value.status == HwStatus::Up as i32;
                let node_ok = self
                    .nodes
                    .get(&(dg.rack_id, dg.node_id))
                    .is_some_and(|(s, _)| *s == HW_UP);
                let rack_ok = self.racks.get(&dg.rack_id).is_some_and(|(s, _)| *s == HW_UP);
                dg_ok && node_ok && rack_ok
            })
            .collect()
    }

    /// Get a disk-group entry by ID.
    pub fn disk_group(&self, dg_id: DiskGroupId) -> Option<&DiskGroupEntry> {
        self.disk_groups.get(&dg_id)
    }

    #[must_use]
    pub fn disk_location(&self, disk_id: DiskId) -> Option<DiskLocation> {
        self.disks.get(&disk_id).copied()
    }

    #[must_use]
    pub fn disk_group_capacity(&self, dg_id: DiskGroupId) -> Option<&DiskGroupCapacity> {
        self.usage.get(&dg_id)
    }

    #[must_use]
    pub fn generation(&self) -> u64 {
        self.generation
    }

    #[must_use]
    pub fn capacity_score(&self, dg_id: DiskGroupId, planned_bytes: u64) -> CapacityScore {
        self.usage.get(&dg_id).map_or(
            CapacityScore {
                used: 0,
                capacity: 0,
                known: false,
            },
            |capacity| CapacityScore {
                used: capacity.projected_bytes(planned_bytes),
                capacity: capacity.capacity_bytes,
                known: capacity.capacity_bytes > 0,
            },
        )
    }

    #[must_use]
    pub fn node_capacity_score(&self, node_id: NodeId, planned_bytes: u64) -> CapacityScore {
        self.domain_capacity_score(
            self.disk_groups
                .values()
                .filter(|disk_group| disk_group.node_id == node_id)
                .map(|disk_group| disk_group.dg_id),
            planned_bytes,
        )
    }

    #[must_use]
    pub fn rack_capacity_score(&self, rack_id: RackId, planned_bytes: u64) -> CapacityScore {
        self.domain_capacity_score(
            self.disk_groups
                .values()
                .filter(|disk_group| disk_group.rack_id == rack_id)
                .map(|disk_group| disk_group.dg_id),
            planned_bytes,
        )
    }

    fn domain_capacity_score(
        &self,
        disk_group_ids: impl Iterator<Item = DiskGroupId>,
        planned_bytes: u64,
    ) -> CapacityScore {
        let mut used = 0u64;
        let mut capacity_bytes = 0u64;
        let mut found = false;
        for disk_group_id in disk_group_ids {
            let Some(capacity) = self.usage.get(&disk_group_id) else {
                return CapacityScore {
                    used: 0,
                    capacity: 0,
                    known: false,
                };
            };
            if capacity.capacity_bytes == 0 {
                return CapacityScore {
                    used: 0,
                    capacity: 0,
                    known: false,
                };
            }
            found = true;
            used = used.saturating_add(capacity.projected_bytes(0));
            capacity_bytes = capacity_bytes.saturating_add(capacity.capacity_bytes);
        }
        CapacityScore {
            used: used.saturating_add(planned_bytes),
            capacity: capacity_bytes,
            known: found,
        }
    }

    /// Atomically account for every byte in a selected plan. The returned
    /// guard releases all reservations when the allocation attempt ends.
    #[must_use]
    pub fn reserve_plan(&self, entries: &[PlacementEntry], bytes_per_block: u64) -> InFlightReservation {
        let mut by_group = HashMap::<DiskGroupId, u64>::new();
        for entry in entries {
            let bytes = bytes_per_block.saturating_mul(u64::from(entry.block_count));
            by_group
                .entry(entry.disk_group_id)
                .and_modify(|reserved| *reserved = reserved.saturating_add(bytes))
                .or_insert(bytes);
        }
        let mut counters = Vec::with_capacity(by_group.len());
        for (dg_id, bytes) in by_group {
            if let Some(capacity) = self.usage.get(&dg_id) {
                capacity.in_flight_bytes.fetch_add(bytes, Ordering::AcqRel);
                counters.push((Arc::clone(&capacity.in_flight_bytes), bytes));
            }
        }
        InFlightReservation { counters }
    }

    /// Complete disk-group entries for synchronizing dependent route caches.
    pub fn disk_groups(&self) -> Vec<DiskGroupEntry> {
        self.disk_groups.values().cloned().collect()
    }

    /// Nodes in a given rack.
    pub fn nodes_in_rack(&self, rack_id: RackId) -> Vec<NodeId> {
        self.racks
            .get(&rack_id)
            .map(|(_, nodes)| nodes.clone())
            .unwrap_or_default()
    }

    /// Rack ID for a given node.
    pub fn rack_for_node(&self, node_id: NodeId) -> Option<RackId> {
        self.nodes
            .iter()
            .find(|((_, nid), _)| *nid == node_id)
            .map(|((rack_id, _), _)| *rack_id)
    }

    /// All rack IDs.
    pub fn rack_ids(&self) -> Vec<RackId> {
        self.racks.keys().copied().collect()
    }

    /// Number of disk-groups in the snapshot.
    pub fn disk_group_count(&self) -> usize {
        self.disk_groups.len()
    }

    /// Check if the snapshot is empty (no racks loaded yet).
    pub fn is_empty(&self) -> bool {
        self.racks.is_empty()
    }

    /// Unit size in bytes (0 if not yet populated).
    pub fn unit_size_bytes(&self) -> u32 {
        self.unit_size_bytes
    }
}

/// Thread-safe topology cache with point-in-time snapshots.
#[derive(Clone)]
pub struct TopologyCache {
    inner: Arc<ArcSwap<TopologySnapshot>>,
    next_generation: Arc<AtomicU64>,
}

impl TopologyCache {
    #[must_use]
    pub fn new() -> Self {
        Self {
            inner: Arc::new(ArcSwap::from_pointee(TopologySnapshot::default())),
            next_generation: Arc::new(AtomicU64::new(1)),
        }
    }

    /// Get a point-in-time snapshot clone for placement.
    pub fn snapshot(&self) -> TopologySnapshot {
        (*self.inner.load_full()).clone()
    }

    /// Replace the entire snapshot (periodic refresh).
    pub fn replace(&self, mut snapshot: TopologySnapshot) {
        let current = self.inner.load_full();
        for (dg_id, capacity) in &mut snapshot.usage {
            if let Some(previous) = current.usage.get(dg_id) {
                capacity.in_flight_bytes = Arc::clone(&previous.in_flight_bytes);
            }
        }
        snapshot.generation = self.next_generation.fetch_add(1, Ordering::AcqRel);
        self.inner.store(Arc::new(snapshot));
    }

    /// Update a single disk-group entry (watch/notify fine-grained update).
    #[allow(clippy::needless_pass_by_value)]
    pub fn update_disk_group(&self, entry: DiskGroupEntry) {
        self.inner.rcu(|current| {
            let mut next = (**current).clone();
            next.disk_groups.insert(entry.dg_id, entry.clone());
            next.generation = self.next_generation.fetch_add(1, Ordering::AcqRel);
            next
        });
    }

    /// Update one usage observation while retaining its in-flight counter.
    pub fn update_disk_group_usage(&self, dg_id: DiskGroupId, summary: &DiskGroupUsageSummary) {
        self.inner.rcu(|current| {
            let mut next = (**current).clone();
            let mut capacity = capacity_from_summary(summary);
            if let Some(previous) = current.usage.get(&dg_id) {
                capacity.in_flight_bytes = Arc::clone(&previous.in_flight_bytes);
            }
            next.usage.insert(dg_id, capacity);
            next.generation = self.next_generation.fetch_add(1, Ordering::AcqRel);
            next
        });
    }

    /// Publish one healthy physical disk location for allocation validation.
    pub fn update_disk_location(&self, disk_id: DiskId, location: DiskLocation) {
        self.inner.rcu(|current| {
            let mut next = (**current).clone();
            next.disks.insert(disk_id, location);
            next.generation = self.next_generation.fetch_add(1, Ordering::AcqRel);
            next
        });
    }

    /// Remove a disk-group entry (deleted disk-group).
    pub fn remove_disk_group(&self, dg_id: DiskGroupId) {
        self.inner.rcu(|current| {
            let mut next = (**current).clone();
            next.disk_groups.remove(&dg_id);
            next.usage.remove(&dg_id);
            next.disks.retain(|_, location| location.disk_group_id != dg_id);
            next.generation = self.next_generation.fetch_add(1, Ordering::AcqRel);
            next
        });
    }

    /// Update a node's status.
    #[allow(clippy::needless_pass_by_value)]
    pub fn update_node_status(
        &self,
        rack_id: RackId,
        node_id: NodeId,
        status: i32,
        dg_ids: Vec<DiskGroupId>,
    ) {
        self.inner.rcu(|current| {
            let mut next = (**current).clone();
            next.nodes.insert((rack_id, node_id), (status, dg_ids.clone()));
            next.generation = self.next_generation.fetch_add(1, Ordering::AcqRel);
            next
        });
    }

    /// Update a rack's status + node list.
    #[allow(clippy::needless_pass_by_value)]
    pub fn update_rack(&self, rack_id: RackId, status: i32, node_ids: Vec<NodeId>) {
        self.inner.rcu(|current| {
            let mut next = (**current).clone();
            next.racks.insert(rack_id, (status, node_ids.clone()));
            next.generation = self.next_generation.fetch_add(1, Ordering::AcqRel);
            next
        });
    }

    /// Check if the cache is empty (no topology loaded yet).
    pub fn is_empty(&self) -> bool {
        self.inner.load().is_empty()
    }
}

impl Default for TopologyCache {
    fn default() -> Self {
        Self::new()
    }
}

/// Build a `TopologySnapshot` from `HardwareClient` data.
///
/// Fetches racks, nodes, and disk-groups from group-0 and assembles
/// them into a snapshot. If the fetch fails or returns empty, returns
/// `None` (caller should keep the previous snapshot).
pub async fn build_snapshot(hw: &crowdb_kv_client::HardwareClient) -> Option<TopologySnapshot> {
    let racks = match hw.list_racks().await {
        Ok(r) => r,
        Err(e) => {
            warn!(error = %e, "topology refresh: list_racks failed");
            return None;
        }
    };

    let nodes = match hw.list_nodes().await {
        Ok(n) => n,
        Err(e) => {
            warn!(error = %e, "topology refresh: list_nodes failed");
            return None;
        }
    };

    let disk_groups = match hw.list_disk_groups().await {
        Ok(d) => d,
        Err(e) => {
            warn!(error = %e, "topology refresh: list_disk_groups failed");
            return None;
        }
    };

    let disks = match hw.list_all_disks().await {
        Ok(disks) => disks,
        Err(error) => {
            warn!(%error, "topology refresh: list_all_disks failed");
            return None;
        }
    };

    let mut usages = match crowdb_kv_client::SpaceUsageClient::from_shared(hw.clone())
        .list_disk_group_usages()
        .await
    {
        Ok(usages) => usages,
        Err(error) => {
            warn!(%error, "topology refresh: usage summaries unavailable; using topology-only ranking");
            Vec::new()
        }
    };
    let service = crowdb_kv_client::ServiceRegistryClient::from_shared(hw.shared_kv());
    if let Ok(instances) = service.read_all_diskdb_instances().await {
        for (_, instance) in instances {
            if let Some(diskdb) = instance.extra.and_then(|extra| extra.diskdb) {
                for summary in diskdb.group_usages {
                    if let Some((_, current)) = usages
                        .iter_mut()
                        .find(|(disk_group_id, _)| *disk_group_id == summary.disk_group_id)
                    {
                        *current = summary;
                    } else {
                        usages.push((summary.disk_group_id, summary));
                    }
                }
            }
        }
    }

    if racks.is_empty() && nodes.is_empty() && disk_groups.is_empty() {
        warn!("topology refresh: all lists empty, keeping previous snapshot");
        return None;
    }

    let mut snap = TopologySnapshot::default();
    for (rack_id, rv) in racks {
        snap.racks.insert(rack_id, (rv.status, rv.node_ids));
    }
    for (rack_id, node_id, nv) in nodes {
        snap.nodes
            .insert((rack_id, node_id), (nv.status, nv.disk_group_ids));
    }
    for mut dg in disk_groups {
        dg.value.disk_ids = disks
            .iter()
            .filter(|disk| {
                disk.rack_id == dg.rack_id && disk.node_id == dg.node_id && disk.disk_group_id == dg.dg_id
            })
            .map(|disk| disk.disk_id)
            .collect();
        snap.disk_groups.insert(dg.dg_id, dg);
    }
    for disk in &disks {
        if disk.value.status == HW_UP {
            snap.disks.insert(
                disk.disk_id,
                DiskLocation {
                    rack_id: disk.rack_id,
                    node_id: disk.node_id,
                    disk_group_id: disk.disk_group_id,
                },
            );
        }
    }
    for (dg_id, summary) in usages {
        if snap.disk_groups.contains_key(&dg_id) {
            snap.usage.insert(dg_id, capacity_from_summary(&summary));
        }
    }

    snap.unit_size_bytes = disks.first().map_or(0, |disk| disk.value.unit_size_bytes);

    Some(snap)
}

fn capacity_from_summary(summary: &DiskGroupUsageSummary) -> DiskGroupCapacity {
    DiskGroupCapacity {
        allocatable_disk_count: summary.allocatable_disk_count,
        capacity_bytes: summary.allocatable_capacity_bytes,
        used_bytes: summary.allocatable_used_bytes,
        free_bytes: summary.allocatable_free_bytes,
        sampled_at_ms: summary.sampled_at_ms,
        in_flight_bytes: Arc::new(AtomicU64::new(0)),
    }
}
