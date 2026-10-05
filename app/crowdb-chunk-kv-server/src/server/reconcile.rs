// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Catalog-owned partition reconciliation.

use super::{
    local_partition_for_entry, partition_matches_entry, recoverable_local_entry, Arc, CatalogSnapshot,
    ChunkKvError, ChunkKvRangeCatalogError, ChunkKvRangeCatalogHead, ChunkKvRangeCatalogPage,
    ChunkKvRangeCatalogReconcileError, ChunkKvService, HashMap, Id128, Partition,
};

impl ChunkKvService {
    /// Replaces the hosted snapshot with exactly the local, recoverable
    /// assignments from a validated catalog.
    ///
    /// Existing exact assignments retain their live handles. Every new or
    /// changed assignment must be supplied after replay in `recovered`.
    ///
    /// # Errors
    ///
    /// Returns an overload or invalid-assignment error without changing the
    /// hosted snapshot.
    pub fn reconcile_partitions(
        &self,
        pages: &[ChunkKvRangeCatalogPage],
        recovered: &[Partition],
    ) -> Result<(), ChunkKvError> {
        let next = self.reconciled_partition_snapshot(pages, recovered)?;
        self.partitions.store(next);
        self.finish_prepared_transfer_reconciliation(pages);
        Ok(())
    }

    /// Validates and installs one catalog together with its exact local
    /// partition snapshot.
    ///
    /// # Errors
    ///
    /// Returns a catalog or partition reconciliation error without changing
    /// either active snapshot.
    pub fn install_catalog_and_reconcile(
        &self,
        head: &ChunkKvRangeCatalogHead,
        pages: &[ChunkKvRangeCatalogPage],
        recovered: &[Partition],
    ) -> Result<(), ChunkKvRangeCatalogReconcileError> {
        let candidate = Arc::new(CatalogSnapshot::from_catalog(head, pages)?);
        let previous = self.catalog.load_full();
        if candidate.generation <= previous.generation {
            return Err(ChunkKvRangeCatalogError::GenerationConflict.into());
        }
        let next = self.reconciled_partition_snapshot(pages, recovered)?;
        let mut released_pins = Vec::new();
        for entry in &candidate.entries {
            if entry.artifact.tail_overlay.is_some() {
                continue;
            }
            let Some(prior) = previous.entry_for_partition(entry.partition_id) else {
                continue;
            };
            let (Some(_), Some(transition_id)) = (&prior.artifact.tail_overlay, prior.transition_id) else {
                continue;
            };
            if let Some(partition) = next.get(&entry.partition_id) {
                released_pins.push((partition.clone(), transition_id));
            }
        }
        self.activate_catalog(&candidate)?;
        let current = self.partitions.load_full();
        for (partition_id, partition) in current.iter() {
            if !next.contains_key(partition_id) {
                self.metrics.retire_partition(&partition.metrics().snapshot());
            }
        }
        self.partitions.store(next);
        self.finish_prepared_transfer_reconciliation(pages);
        for (partition, transition_id) in released_pins {
            partition.release_generation_pin(crowdb_chunk_kv::TransitionId {
                high: transition_id.high,
                low: transition_id.low,
            })?;
        }
        Ok(())
    }

    fn reconciled_partition_snapshot(
        &self,
        pages: &[ChunkKvRangeCatalogPage],
        recovered: &[Partition],
    ) -> Result<Arc<HashMap<Id128, Partition>>, ChunkKvError> {
        let current = self.partitions.load_full();
        let prepared = self.prepared_transfer_targets.load_full();
        let recovered: HashMap<_, _> = recovered
            .iter()
            .map(|partition| {
                let snapshot = partition.snapshot();
                (
                    Id128 {
                        high: snapshot.partition_id.high,
                        low: snapshot.partition_id.low,
                    },
                    partition.clone(),
                )
            })
            .collect();
        let desired: Vec<_> = pages
            .iter()
            .flat_map(|page| &page.entries)
            .filter(|entry| recoverable_local_entry(entry, self.instance_id))
            .collect();
        if desired.len() > self.max_partitions {
            return Err(ChunkKvError::Overloaded);
        }
        let mut next = HashMap::with_capacity(desired.len());
        for entry in desired {
            let partition = current
                .get(&entry.partition_id)
                .and_then(|partition| local_partition_for_entry(partition, entry))
                .or_else(|| self.local_split_writer(entry))
                .or_else(|| {
                    prepared
                        .get(&entry.partition_id)
                        .filter(|partition| partition_matches_entry(partition, entry))
                        .cloned()
                })
                .or_else(|| {
                    recovered
                        .get(&entry.partition_id)
                        .filter(|partition| partition_matches_entry(partition, entry))
                        .cloned()
                })
                .ok_or_else(|| {
                    ChunkKvError::InvalidRequest(
                        "catalog assignment was not recovered before reconciliation".into(),
                    )
                })?;
            next.insert(entry.partition_id, partition.clone());
        }
        Ok(Arc::new(next))
    }
}
