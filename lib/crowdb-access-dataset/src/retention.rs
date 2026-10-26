use crate::{FieldLocator, ManifestRecord, SnapshotId, SnapshotRecord};
use std::collections::{HashMap, HashSet};

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
