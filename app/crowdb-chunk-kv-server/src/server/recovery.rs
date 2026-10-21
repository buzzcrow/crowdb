// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Terminal writer uncertainty requires process teardown and durable replay.

use super::{ChunkKvService, HostedPartitionHealth, Id128};
use crowdb_chunk_kv::PartitionLifecycle;

impl ChunkKvService {
    /// Reports terminal writer failures without reopening or replacing handles.
    #[must_use]
    pub fn recovery_required(&self) -> Vec<HostedPartitionHealth> {
        let mut writers: Vec<_> = self.observed_partition_snapshot().values().cloned().collect();
        writers.extend(
            self.local_split_sessions
                .load()
                .values()
                .map(|session| session.dispatcher.clone()),
        );
        let ingress_writers: Vec<_> = writers
            .iter()
            .filter_map(crowdb_chunk_kv::Partition::split_ingress)
            .flat_map(|ingress| [ingress.retained_parent(), ingress.child()])
            .collect();
        writers.extend(ingress_writers);
        let mut failed: Vec<_> = writers
            .iter()
            .filter_map(|partition| {
                let snapshot = partition.snapshot();
                (snapshot.lifecycle == PartitionLifecycle::Recovering).then_some(HostedPartitionHealth {
                    partition_id: Id128 {
                        high: snapshot.partition_id.high,
                        low: snapshot.partition_id.low,
                    },
                    owner_epoch: snapshot.ownership_epoch,
                    lifecycle: snapshot.lifecycle,
                    durable_seq: snapshot.journal_durable_seq,
                    applied_seq: snapshot.applied_seq,
                })
            })
            .collect();
        failed.sort_unstable_by_key(|partition| (partition.partition_id, partition.owner_epoch));
        failed.dedup_by_key(|partition| (partition.partition_id, partition.owner_epoch));
        failed
    }

    /// Waits for uncertainty that cannot be repaired by catalog reconciliation.
    pub async fn wait_for_recovery(&self) -> Vec<HostedPartitionHealth> {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(1));
        loop {
            interval.tick().await;
            let failed = self.recovery_required();
            if !failed.is_empty() {
                return failed;
            }
        }
    }
}
