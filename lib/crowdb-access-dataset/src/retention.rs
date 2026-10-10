use std::collections::{HashMap, HashSet};
use std::sync::{
    atomic::{AtomicU64, AtomicUsize, Ordering},
    Arc,
};

use crate::{FieldLocator, ManifestRecord, SnapshotId, SnapshotRecord};

/// Input and output of the bounded Dataset retention planner.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RetentionPlan {
    /// Published snapshots that must remain readable.
    pub reachable_snapshots: HashSet<SnapshotId>,
    /// Dataset-owned chunk locations that are safe to reclaim.
    pub reclaimable_chunks: Vec<Vec<u8>>,
    pub reclaimable_snapshots: Vec<SnapshotId>,
}

/// Computes conservative retention without deleting storage itself. Callers
/// can execute the returned candidates through their store's idempotent GC.
pub struct RetentionPlanner;

/// Lock-free lease registry used by a reclaimer to protect active Dataset
/// reads. A released lease is immediately reclaimable once no other reader is
/// active; an abandoned lease expires after its configured TTL.
pub struct RetentionLeaseRegistry {
    active: AtomicUsize,
    deadline: AtomicU64,
    ttl_seconds: u64,
}

impl RetentionLeaseRegistry {
    /// # Errors
    /// Rejects a zero inactivity TTL.
    pub fn new(ttl_seconds: u64) -> Result<Self, crate::DatasetError> {
        if ttl_seconds == 0 {
            return Err(crate::DatasetError::InvalidManifest);
        }
        Ok(Self {
            active: AtomicUsize::new(0),
            deadline: AtomicU64::new(0),
            ttl_seconds,
        })
    }

    #[must_use]
    pub fn acquire(&self, now_seconds: u64) -> RetentionLease<'_> {
        self.active.fetch_add(1, Ordering::AcqRel);
        self.deadline
            .store(now_seconds.saturating_add(self.ttl_seconds), Ordering::Release);
        RetentionLease { registry: self }
    }

    #[must_use]
    pub fn reclaim_allowed(&self, now_seconds: u64) -> bool {
        self.active.load(Ordering::Acquire) == 0 || now_seconds >= self.deadline.load(Ordering::Acquire)
    }

    #[must_use]
    pub fn active(&self) -> usize {
        self.active.load(Ordering::Acquire)
    }
}

pub struct RetentionLease<'a> {
    registry: &'a RetentionLeaseRegistry,
}

/// Owned lease used across async Dataset request boundaries.
pub struct RetentionLeaseHandle {
    registry: Arc<RetentionLeaseRegistry>,
}

impl RetentionLeaseHandle {
    #[must_use]
    pub fn new(registry: Arc<RetentionLeaseRegistry>, now_seconds: u64) -> Self {
        registry.active.fetch_add(1, Ordering::AcqRel);
        registry.deadline.store(
            now_seconds.saturating_add(registry.ttl_seconds),
            Ordering::Release,
        );
        Self { registry }
    }
}

impl Drop for RetentionLeaseHandle {
    fn drop(&mut self) {
        self.registry.active.fetch_sub(1, Ordering::AcqRel);
    }
}

impl Drop for RetentionLease<'_> {
    fn drop(&mut self) {
        self.registry.active.fetch_sub(1, Ordering::AcqRel);
    }
}

impl RetentionPlanner {
    /// Marks retained snapshots and their explicit parent chains, then returns
    /// Dataset-owned chunk locators from snapshots outside that reachability
    /// set. External locations are supplied by the caller and are preserved.
    ///
    /// # Errors
    /// Returns an error when a published snapshot points at a missing parent.
    pub fn plan(
        snapshots: &[SnapshotRecord],
        manifests: &HashMap<SnapshotId, ManifestRecord>,
        retained: &HashSet<SnapshotId>,
        active_lease: bool,
        external_locations: &HashSet<Vec<u8>>,
    ) -> Result<RetentionPlan, crate::DatasetError> {
        let by_id: HashMap<_, _> = snapshots
            .iter()
            .map(|record| (record.publication.snapshot, record))
            .collect();
        let mut reachable = HashSet::new();
        for root in retained {
            let mut current = Some(*root);
            while let Some(id) = current {
                if !reachable.insert(id) {
                    break;
                }
                let record = by_id.get(&id).ok_or(crate::DatasetError::InvalidManifest)?;
                current = record.publication.parent;
            }
        }
        if active_lease {
            return Ok(RetentionPlan {
                reachable_snapshots: reachable,
                reclaimable_chunks: Vec::new(),
                reclaimable_snapshots: Vec::new(),
            });
        }
        let mut reclaimable = HashSet::new();
        for (snapshot, manifest) in manifests {
            if reachable.contains(snapshot) {
                continue;
            }
            for sample in &manifest.samples {
                for field in &sample.fields {
                    if let FieldLocator::Chunk { location, .. } = &field.value {
                        if !external_locations.contains(location) {
                            reclaimable.insert(location.clone());
                        }
                    }
                }
            }
        }
        let reclaimable_snapshots = snapshots
            .iter()
            .map(|record| record.publication.snapshot)
            .filter(|snapshot| !reachable.contains(snapshot))
            .collect();
        Ok(RetentionPlan {
            reachable_snapshots: reachable,
            reclaimable_chunks: reclaimable.into_iter().collect(),
            reclaimable_snapshots,
        })
    }
}
