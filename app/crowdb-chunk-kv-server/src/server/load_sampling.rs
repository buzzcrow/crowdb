// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Advisory split boundaries; bounded observation isolated from serving leases.

use std::collections::HashMap;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

use arc_swap::ArcSwap;
use crowdb_chunk_kv::{ChunkKvError, Partition, PartitionLifecycle};
use crowdb_protocol::chunk_kv::Id128;
use tracing::warn;

type Samples = Vec<(Vec<u8>, u64)>;

#[derive(Default)]
pub(super) struct LoadSampling {
    running: AtomicBool,
    cached: ArcSwap<HashMap<Id128, (u64, Samples)>>,
}

struct SamplingJob(Arc<LoadSampling>);
impl Drop for SamplingJob {
    fn drop(&mut self) {
        self.0.running.store(false, Ordering::Release);
    }
}

impl LoadSampling {
    pub(super) fn samples(&self, id: Id128, epoch: u64) -> Samples {
        self.cached
            .load()
            .get(&id)
            .filter(|(observed_epoch, _)| *observed_epoch == epoch)
            .map_or_else(Vec::new, |(_, samples)| samples.clone())
    }

    pub(super) fn launch(self: &Arc<Self>, partitions: Arc<HashMap<Id128, Partition>>, limit: usize) {
        if self
            .running
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return;
        }
        let job = SamplingJob(Arc::clone(self));
        let runtime = tokio::runtime::Handle::current();
        // Native page reads may block on cold storage. One owned job per service
        // isolates that delay and prevents repeated heartbeat ticks spawning work.
        tokio::task::spawn_blocking(move || {
            runtime.block_on(async move {
                let mut observations = HashMap::new();
                for (id, partition) in partitions.iter() {
                    let snapshot = partition.snapshot();
                    if snapshot.lifecycle != PartitionLifecycle::Serving {
                        continue;
                    }
                    match sample(partition, snapshot.ownership_epoch, limit).await {
                        Ok(samples) => {
                            observations.insert(*id, (snapshot.ownership_epoch, samples));
                        }
                        Err(error) => warn!(partition_id_high = id.high, partition_id_low = id.low, %error,
                        "split observation failed; partition remains unsampled"),
                    }
                }
                job.0.cached.store(Arc::new(observations));
            });
        });
    }
}

async fn sample(partition: &Partition, epoch: u64, limit: usize) -> Result<Samples, ChunkKvError> {
    const PAGE_BYTES: usize = 64 * 1024;
    if let Some(key) = partition.approximate_split_key()? {
        let first = partition
            .scan_forward(epoch, None, None, 1, PAGE_BYTES, None)
            .await?;
        let right = partition
            .scan_forward(epoch, Some(&key), None, 1, PAGE_BYTES, None)
            .await?;
        if let (Some(left), Some(right)) = (first.entries.first(), right.entries.first()) {
            if left.key < right.key {
                // Two live witnesses preserve the existing planner's nonempty
                // child checks; equal weights select the right-hand boundary.
                return Ok(vec![(left.key.to_vec(), 1), (right.key.to_vec(), 1)]);
            }
        }
    }
    // Unflushed small trees or cold indexes need only one bounded key window.
    // One additional witness is enough when a large value fills the page.
    // No continuation loop: this work never scales with the partition's size.
    let mut page = partition
        .scan_forward(epoch, None, None, limit.min(64), PAGE_BYTES, None)
        .await?;
    if page.truncated && page.entries.len() == 1 && limit >= 2 {
        let right = partition
            .scan_forward_after(epoch, &page.entries[0].key, None, 1, PAGE_BYTES, None)
            .await?;
        page.entries.extend(right.entries);
    }
    Ok(page
        .entries
        .into_iter()
        .map(|entry| {
            (
                entry.key.to_vec(),
                u64::try_from(entry.key.len() + entry.value.value.len()).unwrap_or(u64::MAX),
            )
        })
        .collect())
}
