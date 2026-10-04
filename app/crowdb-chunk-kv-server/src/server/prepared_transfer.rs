// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Prepared transfer handles remain separate from catalog-owned assignments.

use super::{Arc, ChunkKvError, ChunkKvRangeCatalogPage, ChunkKvService, HashMap, Id128, Partition};
use crowdb_chunk_kv::PartitionLifecycle;

impl ChunkKvService {
    pub(crate) fn install_prepared_transfer_target(&self, partition: &Partition) -> Result<(), ChunkKvError> {
        let snapshot = partition.snapshot();
        if snapshot.lifecycle != PartitionLifecycle::Prepared {
            return Err(ChunkKvError::InvalidRequest(
                "transfer target must remain prepared".into(),
            ));
        }
        let id = Id128 {
            high: snapshot.partition_id.high,
            low: snapshot.partition_id.low,
        };
        let observed = self.observed_partition_snapshot();
        if !observed.contains_key(&id) && observed.len() >= self.max_partitions {
            return Err(ChunkKvError::Overloaded);
        }
        self.prepared_transfer_targets.rcu(|current| {
            let mut next = (**current).clone();
            next.insert(id, partition.clone());
            Arc::new(next)
        });
        Ok(())
    }

    pub(super) fn observed_partition_snapshot(&self) -> Arc<HashMap<Id128, Partition>> {
        let mut observed = (*self.partitions.load_full()).clone();
        for (id, target) in self.prepared_transfer_targets.load().iter() {
            observed.entry(*id).or_insert_with(|| target.clone());
        }
        Arc::new(observed)
    }

    pub(super) fn finish_prepared_transfer_reconciliation(&self, pages: &[ChunkKvRangeCatalogPage]) {
        let installed = self.partitions.load_full();
        self.prepared_transfer_targets.rcu(|current| {
            let mut next = (**current).clone();
            next.retain(|id, partition| {
                let epoch = partition.snapshot().ownership_epoch;
                // Publication into the catalog-owned map precedes retiring the
                // staging handle, so the readiness observation has no gap.
                if installed
                    .get(id)
                    .is_some_and(|live| live.snapshot().ownership_epoch == epoch)
                {
                    return false;
                }
                !pages
                    .iter()
                    .flat_map(|page| &page.entries)
                    .any(|entry| entry.partition_id == *id && entry.owner_epoch >= epoch)
            });
            Arc::new(next)
        });
    }

    pub(crate) fn discard_prepared_transfer_target(&self, id: Id128, epoch: u64) {
        self.prepared_transfer_targets.rcu(|current| {
            let mut next = (**current).clone();
            if next
                .get(&id)
                .is_some_and(|partition| partition.snapshot().ownership_epoch == epoch)
            {
                next.remove(&id);
            }
            Arc::new(next)
        });
    }
}
