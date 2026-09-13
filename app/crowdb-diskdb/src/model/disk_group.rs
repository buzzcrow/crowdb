// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! `DdbDiskGroup` — per-disk-group manager: owns the disks, the RCU
//! allocatable-disk context, and the round-robin cursor.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicI32, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};

use arc_swap::ArcSwap;
use crossbeam_skiplist::SkipMap;
use crowdb_protocol::common::{DiskId, HwStatus};
use crowdb_protocol::diskdb::rpc::BusyBlockValue;
use crowdb_protocol::DiskGroupId;

use crate::metrics::DiskMetrics;
use crate::model::disk::{DdbDisk, DiskUsage};
use crate::model::zone::{AllocatedRange, DdbZone, ZoneUsage};

/// RCU-published set of allocatable disks within the named
/// disk-group, replaced via `Arc` swap on add/remove/status-change.
pub type AllocateDiskContext = Vec<Arc<DdbDisk>>;
pub type Bind = (u64, u64);

#[derive(Default)]
struct DiskMembership {
    by_id: HashMap<DiskId, Arc<DdbDisk>>,
    allocating: AllocateDiskContext,
}

/// Result of a successful allocation: `(disk, zone, range)`.
pub type AllocClaim = (Arc<DdbDisk>, Arc<DdbZone>, AllocatedRange);

// Keeps abandoned tentative allocations from growing the process without
// bound. Eviction is safe: commit falls back to the durable KV record.
const MAX_TENTATIVE_BLOCKS: usize = 262_144;
const TENTATIVE_PENDING: u8 = 0;
const TENTATIVE_COUNTED: u8 = 1;
const TENTATIVE_REMOVED: u8 = 2;

/// Tentative allocation retained until the normal near-term commit arrives.
#[derive(Clone)]
pub struct TentativeBlock {
    pub disk_id: DiskId,
    pub zone_index: u32,
    pub unit_offset: u64,
    pub value: BusyBlockValue,
    pub revision: u64,
}

struct TentativeEntry {
    block: TentativeBlock,
    state: std::sync::atomic::AtomicU8,
}

/// A disk-group manager — one per owned disk-group.
pub struct DdbDiskGroup {
    pub disk_group_id: DiskGroupId,
    pub node_id: u64,
    pub rack_id: u64,
    status: AtomicI32,
    /// `(store_id, group_id)` for the bound paxos data group.
    bind: ArcSwap<Bind>,
    pub disks: RwLock<Vec<Arc<DdbDisk>>>,
    /// Coherent RCU snapshot of disk lookup and allocation routes.
    membership: ArcSwap<DiskMembership>,
    /// Round-robin cursor over the snapshot's allocatable disks.
    pos_v_disk_ctx: AtomicU64,
    /// Per-disk-group monotonic allocation-incarnation source.
    allocation_ts_source: AtomicU64,
    /// `allocation_ts -> tentative allocation`; recovery-safe KV reads are
    /// used when an entry is absent after restart or eviction.
    tentative_blocks: SkipMap<u64, Arc<TentativeEntry>>,
    tentative_count: AtomicUsize,
    tentative_trim_owner: AtomicBool,
    tentative_capacity: usize,
}

impl DdbDiskGroup {
    pub fn new(disk_group_id: DiskGroupId, node_id: u64, rack_id: u64) -> Self {
        Self::with_tentative_capacity(disk_group_id, node_id, rack_id, MAX_TENTATIVE_BLOCKS)
    }

    fn with_tentative_capacity(
        disk_group_id: DiskGroupId,
        node_id: u64,
        rack_id: u64,
        tentative_capacity: usize,
    ) -> Self {
        Self {
            disk_group_id,
            node_id,
            rack_id,
            // A.1: start at Init — the sync loop applies the real
            // group-0 status on the first tick.
            status: AtomicI32::new(HwStatus::Init as i32),
            bind: ArcSwap::from_pointee((0, 0)),
            disks: RwLock::new(Vec::new()),
            membership: ArcSwap::from_pointee(DiskMembership::default()),
            pos_v_disk_ctx: AtomicU64::new(0),
            allocation_ts_source: AtomicU64::new(now_nanos()),
            tentative_blocks: SkipMap::new(),
            tentative_count: AtomicUsize::new(0),
            tentative_trim_owner: AtomicBool::new(false),
            tentative_capacity,
        }
    }

    pub fn cache_tentative(&self, block: TentativeBlock) {
        let allocation_ts = block.value.allocation_ts;
        let candidate = Arc::new(TentativeEntry {
            block,
            state: std::sync::atomic::AtomicU8::new(TENTATIVE_PENDING),
        });
        let published = self
            .tentative_blocks
            .get_or_insert(allocation_ts, Arc::clone(&candidate));
        if Arc::ptr_eq(published.value(), &candidate) {
            self.tentative_count.fetch_add(1, Ordering::AcqRel);
            if candidate
                .state
                .compare_exchange(
                    TENTATIVE_PENDING,
                    TENTATIVE_COUNTED,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                )
                .is_ok()
            {
                self.trim_tentative_blocks();
            } else {
                self.tentative_count.fetch_sub(1, Ordering::AcqRel);
            }
        }
    }

    pub fn tentative(&self, allocation_ts: u64) -> Option<TentativeBlock> {
        self.tentative_blocks
            .get(&allocation_ts)
            .map(|entry| entry.value().block.clone())
    }

    pub fn remove_tentative(&self, allocation_ts: u64) -> bool {
        let Some(entry) = self.tentative_blocks.get(&allocation_ts) else {
            return false;
        };
        self.retire_tentative_entry(&entry)
    }

    pub fn remove_matching_tentative(
        &self,
        allocation_ts: u64,
        disk_id: DiskId,
        zone_index: u32,
        unit_offset: u64,
    ) -> bool {
        let Some(entry) = self.tentative_blocks.get(&allocation_ts) else {
            return false;
        };
        let block = &entry.value().block;
        if block.disk_id != disk_id || block.zone_index != zone_index || block.unit_offset != unit_offset {
            return false;
        }
        self.retire_tentative_entry(&entry)
    }

    fn trim_tentative_blocks(&self) {
        loop {
            if self.tentative_count.load(Ordering::Acquire) <= self.tentative_capacity {
                return;
            }
            if self
                .tentative_trim_owner
                .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                .is_err()
            {
                return;
            }

            while self.tentative_count.load(Ordering::Acquire) > self.tentative_capacity {
                let Some(oldest) = self.tentative_blocks.front() else {
                    break;
                };
                self.retire_tentative_entry(&oldest);
            }
            self.tentative_trim_owner.store(false, Ordering::Release);
        }
    }

    fn retire_tentative_entry(
        &self,
        entry: &crossbeam_skiplist::map::Entry<'_, u64, Arc<TentativeEntry>>,
    ) -> bool {
        if !entry.remove() {
            return false;
        }
        if entry.value().state.swap(TENTATIVE_REMOVED, Ordering::AcqRel) == TENTATIVE_COUNTED {
            self.tentative_count.fetch_sub(1, Ordering::AcqRel);
        }
        true
    }

    #[cfg(feature = "test-util")]
    #[must_use]
    pub fn new_with_tentative_capacity(
        disk_group_id: DiskGroupId,
        node_id: u64,
        rack_id: u64,
        capacity: usize,
    ) -> Self {
        Self::with_tentative_capacity(disk_group_id, node_id, rack_id, capacity)
    }

    #[cfg(feature = "test-util")]
    #[must_use]
    pub fn tentative_count(&self) -> usize {
        self.tentative_count.load(Ordering::Acquire)
    }

    /// Add a disk to this disk-group. Rebuilds the allocatable disk set.
    pub fn add_disk(&self, disk: Arc<DdbDisk>) {
        self.disks.write().unwrap().push(disk);
        self.rebuild_allocating_disks();
    }

    /// Remove a disk from in-memory state and its published membership.
    /// Used when a disk is absent from sync and its status is
    /// `Offline`, `Maintenance`, or `Init` — the disk's `DiskKey` was
    /// deleted from group 0 (moved or removed), so absence means it's
    /// gone. Rebuilds the allocatable disk set.
    pub fn remove_disk_from_memory(&self, disk_id: &DiskId) {
        {
            let mut disks = self.disks.write().unwrap();
            disks.retain(|d| d.disk_id != *disk_id);
        }
        self.rebuild_allocating_disks();
    }

    /// Rebuild and atomically publish disk lookup and allocation routes.
    pub fn rebuild_allocating_disks(&self) {
        let disks = self.disks.read().unwrap();
        let by_id = disks
            .iter()
            .map(|disk| (disk.disk_id, Arc::clone(disk)))
            .collect();
        let allocating = disks.iter().filter(|d| d.allocatable()).cloned().collect();
        self.membership
            .store(Arc::new(DiskMembership { by_id, allocating }));
    }

    /// Generate the next monotonic allocation incarnation.
    /// Advances the source by `max(now(), last + 1)` to guarantee
    /// monotonicity even if the wall clock jumps backwards.
    pub fn next_allocation_ts(&self) -> u64 {
        let now = now_nanos();
        loop {
            let prev = self.allocation_ts_source.load(Ordering::Acquire);
            // Saturating add avoids wrapping to a reusable incarnation.
            let next = now.max(prev.saturating_add(1));
            if self
                .allocation_ts_source
                .compare_exchange(prev, next, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                return next;
            }
        }
    }

    /// Initialize the incarnation source above every recovered incarnation.
    pub fn init_allocation_ts_source_after_load(&self, max_allocation_ts: u64) {
        let now = now_nanos();
        let target = now.max(max_allocation_ts.saturating_add(1));
        self.allocation_ts_source.fetch_max(target, Ordering::AcqRel);
    }

    /// Whether this disk-group can accept allocations.
    pub fn allocatable(&self) -> bool {
        self.status() == HwStatus::Up
    }

    pub fn status(&self) -> HwStatus {
        HwStatus::try_from(self.status.load(Ordering::Acquire)).unwrap_or(HwStatus::Init)
    }

    pub fn bind(&self) -> Bind {
        **self.bind.load()
    }

    pub fn set_bind(&self, bind: Bind) {
        self.bind.store(Arc::new(bind));
    }

    pub fn set_status(&self, status: HwStatus) {
        self.status.store(status as i32, Ordering::Release);
    }

    /// Allocate a single block — round-robin over allocatable disks
    /// within this disk-group, skipping `exclude_disks`.
    ///
    /// Returns `NoSpace` if no disk can satisfy the request.
    pub fn allocate_block(
        &self,
        unit_count: u32,
        exclude_disks: &[DiskId],
        cas_retry_limit: u32,
        zone_rotate_count: u32,
    ) -> Result<AllocClaim, AllocError> {
        if !self.allocatable() {
            return Err(AllocError::NoSpace);
        }
        let membership = self.membership.load_full();
        let ctx = &membership.allocating;
        if ctx.is_empty() {
            return Err(AllocError::NoSpace);
        }
        let ctx_len = ctx.len();
        #[allow(clippy::cast_possible_truncation)]
        let start = self.pos_v_disk_ctx.fetch_add(1, Ordering::Relaxed) as usize % ctx_len;
        for i in 0..ctx_len {
            let disk = &ctx[(start + i) % ctx_len];
            if exclude_disks.contains(&disk.disk_id) {
                continue;
            }
            if let Some((zone, range)) = disk.disk_allocate(unit_count, cas_retry_limit, zone_rotate_count) {
                return Ok((Arc::clone(disk), zone, range));
            }
        }
        Err(AllocError::NoSpace)
    }

    /// Allocate `count` blocks of `unit_count` units each, spreading
    /// across disks (anti-affinity via `exclude_disks`).
    ///
    /// Tries round-robin first; if not all `count` claimed, retries
    /// remaining with a full scan.
    pub fn allocate_blocks(
        &self,
        unit_count: u32,
        count: u32,
        exclude_disks: &[DiskId],
        cas_retry_limit: u32,
        zone_rotate_count: u32,
    ) -> Result<Vec<AllocClaim>, AllocError> {
        let mut results: Vec<AllocClaim> = Vec::new();
        let mut used_disks: Vec<DiskId> = exclude_disks.to_vec();

        // First pass: round-robin.
        for _ in 0..count {
            match self.allocate_block(unit_count, &used_disks, cas_retry_limit, zone_rotate_count) {
                Ok((disk, zone, range)) => {
                    used_disks.push(disk.disk_id);
                    results.push((disk, zone, range));
                }
                Err(AllocError::NoSpace) => break,
                Err(error @ AllocError::Persistence) => return Err(error),
            }
        }

        if results.len() == count as usize {
            return Ok(results);
        }

        // Second pass: full scan (random start, skip excluded + used).
        let membership = self.membership.load_full();
        let ctx = &membership.allocating;
        while results.len() < count as usize {
            let mut claimed = false;
            #[allow(clippy::cast_possible_truncation)]
            let start = rand::random_range(0..ctx.len().max(1));
            for i in 0..ctx.len() {
                let disk = &ctx[(start + i) % ctx.len()];
                if used_disks.contains(&disk.disk_id) {
                    continue;
                }
                if let Some((zone, range)) =
                    disk.disk_allocate(unit_count, cas_retry_limit, zone_rotate_count)
                {
                    used_disks.push(disk.disk_id);
                    results.push((Arc::clone(disk), zone, range));
                    claimed = true;
                    if results.len() >= count as usize {
                        break;
                    }
                }
            }
            if !claimed {
                break;
            }
        }

        if results.len() == count as usize {
            Ok(results)
        } else {
            for (_, zone, range) in &results {
                if !zone.rollback_allocate(range.unit_offset, range.unit_count) {
                    tracing::error!(
                        disk_group_id = self.disk_group_id,
                        zone_index = zone.zone_index,
                        unit_offset = range.unit_offset,
                        unit_count = range.unit_count,
                        "partial allocation rollback failed; range remains conservatively busy"
                    );
                }
            }
            Err(AllocError::NoSpace)
        }
    }

    /// Allocate a batch while allowing disks to be reused after every
    /// anti-affinity pass. The first pass remains spread across distinct
    /// disks, and all claims are rolled back if the complete batch cannot be
    /// satisfied.
    pub fn allocate_blocks_reusing_disks(
        &self,
        unit_count: u32,
        count: u32,
        exclude_disks: &[DiskId],
        cas_retry_limit: u32,
        zone_rotate_count: u32,
    ) -> Result<Vec<AllocClaim>, AllocError> {
        let mut results = Vec::with_capacity(count as usize);
        let mut used_disks = exclude_disks.to_vec();

        while results.len() < count as usize {
            match self.allocate_block(unit_count, &used_disks, cas_retry_limit, zone_rotate_count) {
                Ok((disk, zone, range)) => {
                    used_disks.push(disk.disk_id);
                    results.push((disk, zone, range));
                }
                Err(AllocError::NoSpace) if used_disks.len() > exclude_disks.len() => {
                    used_disks.truncate(exclude_disks.len());
                }
                Err(AllocError::NoSpace) => break,
                Err(error @ AllocError::Persistence) => return Err(error),
            }
        }

        if results.len() == count as usize {
            return Ok(results);
        }
        for (_, zone, range) in &results {
            if !zone.rollback_allocate(range.unit_offset, range.unit_count) {
                tracing::error!(
                    disk_group_id = self.disk_group_id,
                    zone_index = zone.zone_index,
                    unit_offset = range.unit_offset,
                    unit_count = range.unit_count,
                    "partial allocation rollback failed; range remains conservatively busy"
                );
            }
        }
        Err(AllocError::NoSpace)
    }

    /// Free a block by `(disk_id, zone_index, unit_offset, unit_count)`.
    pub fn free_block(&self, disk_id: &DiskId, zone_index: u32, unit_offset: u64, unit_count: u32) -> bool {
        let disk = self.membership.load().by_id.get(disk_id).cloned();
        match disk {
            Some(d) => d.free(zone_index, unit_offset, unit_count),
            None => false,
        }
    }

    // ── R74 space-metrics accessors ───────────────────────────────

    /// Aggregated usage across all disks (R74 §2). `disk_count` =
    /// total disks; `allocatable_disk_count` = RCU `allocating_disks`
    /// size (disks currently `Up` and allocatable). A `Bad` disk's
    /// capacity still counts in the total.
    #[must_use]
    pub fn aggregate_usage(&self) -> DiskGroupUsage {
        let disks_guard = self.disks.read().unwrap();
        let mut capacity_bytes = 0u64;
        let mut busy_bytes = 0u64;
        let mut disk_usages: Vec<DiskUsage> = Vec::with_capacity(disks_guard.len());
        for disk in disks_guard.iter() {
            let u = disk.usage();
            capacity_bytes += u.capacity_bytes;
            busy_bytes += u.busy_bytes;
            disk_usages.push(u);
        }
        #[allow(clippy::cast_possible_truncation)]
        let disk_count = disks_guard.len() as u32;
        #[allow(clippy::cast_possible_truncation)]
        let allocatable_disk_count = self.membership.load().allocating.len() as u32;
        let free_bytes = capacity_bytes.saturating_sub(busy_bytes);
        DiskGroupUsage {
            disk_group_id: self.disk_group_id,
            capacity_bytes,
            busy_bytes,
            free_bytes,
            disk_count,
            allocatable_disk_count,
            disks: disk_usages,
        }
    }

    /// Brief per-zone usage for `(disk_id, zone_index)` (R74 §2).
    /// Returns `None` for an unknown disk or out-of-range zone.
    #[must_use]
    pub fn zone_usage(&self, disk_id: DiskId, zone_index: u32) -> Option<ZoneUsage> {
        let disk = self.membership.load().by_id.get(&disk_id).cloned()?;
        let zones = disk.zones.load();
        let idx = zone_index as usize;
        if idx >= zones.len() {
            return None;
        }
        let unit_size_bytes = disk.disk_value.unit_size_bytes;
        Some(ZoneUsage::from_zone(&zones[idx], unit_size_bytes))
    }

    /// Per-disk hot-path metrics handle for `disk_id` (R74 §3).
    /// Returns `None` for an unknown disk or a disk with no metrics
    /// attached (test disks).
    #[must_use]
    pub fn disk_metrics(&self, disk_id: DiskId) -> Option<Arc<DiskMetrics>> {
        self.membership
            .load()
            .by_id
            .get(&disk_id)
            .and_then(|disk| disk.metrics.clone())
    }

    /// The disk's `unit_size_bytes` (from `disk_value`), or `None` for
    /// an unknown disk. Used by the free path to record byte counters.
    #[must_use]
    pub fn disk_unit_size(&self, disk_id: DiskId) -> Option<u32> {
        let membership = self.membership.load();
        let disk = membership.by_id.get(&disk_id)?;
        let unit_size = disk.disk_value.unit_size_bytes;
        Some(unit_size)
    }

    /// Get a cloned `Arc<DdbDisk>` by `disk_id` (R74 query handler).
    /// Returns `None` for an unknown disk.
    #[must_use]
    pub fn get_disk(&self, disk_id: DiskId) -> Option<Arc<DdbDisk>> {
        self.membership.load().by_id.get(&disk_id).cloned()
    }

    #[cfg(feature = "test-util")]
    #[must_use]
    pub fn membership_snapshot_ids(&self) -> (Vec<DiskId>, Vec<DiskId>) {
        let membership = self.membership.load();
        (
            membership.by_id.keys().copied().collect(),
            membership.allocating.iter().map(|disk| disk.disk_id).collect(),
        )
    }
}

/// Per-disk-group usage (aggregated across disks, R74 §2).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DiskGroupUsage {
    pub disk_group_id: DiskGroupId,
    pub capacity_bytes: u64,
    pub busy_bytes: u64,
    pub free_bytes: u64,
    pub disk_count: u32,
    pub allocatable_disk_count: u32,
    pub disks: Vec<DiskUsage>,
}

/// Allocation errors.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AllocError {
    /// No disk/zone can satisfy the request.
    NoSpace,
    /// Durable allocation record persistence failed.
    Persistence,
}

/// Current wall-clock time in nanoseconds.
pub(crate) fn now_nanos() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_nanos()).unwrap_or(0))
}
