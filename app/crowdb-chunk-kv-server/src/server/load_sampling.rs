// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Advisory split boundaries; bounded observation isolated from serving leases.

use std::collections::HashMap;
use std::sync::{
    atomic::{AtomicU8, Ordering},
    Arc,
};

use arc_swap::ArcSwap;
use crowdb_chunk_kv::{ChunkKvError, Partition, PartitionLifecycle};
use crowdb_protocol::chunk_kv::Id128;
use tokio::sync::Notify;
use tracing::warn;

type Samples = Vec<(Vec<u8>, u64)>;

const RUNNING: u8 = 1;
const CLOSED: u8 = 2;

#[derive(Default)]
pub(super) struct LoadSampling {
    state: AtomicU8,
    cached: ArcSwap<HashMap<Id128, (u64, Samples)>>,
    completed: Notify,
}

struct SamplingJob(Arc<LoadSampling>);
impl Drop for SamplingJob {
    fn drop(&mut self) {
        self.0.state.fetch_and(!RUNNING, Ordering::AcqRel);
        self.0.completed.notify_waiters();
    }
}

// Field order also keeps completion last if queued work is dropped before entry.
struct SamplingWork {
    partitions: Arc<HashMap<Id128, Partition>>,
    runtime: tokio::runtime::Handle,
    completion: SamplingJob,
}

impl SamplingWork {
    fn run(self, limit: usize) {
        // Declared first and completed last, including during unwinding.
        let completion = self.completion;
        let runtime_handle = self.runtime;
        let owned_partitions = self.partitions;
        runtime_handle.block_on(async {
            let mut observations = HashMap::new();
            for (id, partition) in owned_partitions.iter() {
                if completion.0.is_closed() {
                    return;
                }
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
            if !completion.0.is_closed() {
                completion.0.cached.store(Arc::new(observations));
            }
        });
        drop(owned_partitions);
        drop(runtime_handle);
        drop(completion);
    }
}

impl LoadSampling {
    pub(super) fn samples(&self, id: Id128, epoch: u64) -> Samples {
        if self.is_closed() {
            return Vec::new();
        }
        self.cached
            .load()
            .get(&id)
            .filter(|(observed_epoch, _)| *observed_epoch == epoch)
            .map_or_else(Vec::new, |(_, samples)| samples.clone())
    }

    pub(super) fn close(&self) {
        self.state.fetch_or(CLOSED, Ordering::AcqRel);
    }

    pub(super) async fn drain(&self) {
        self.close();
        loop {
            let completed = self.completed.notified();
            tokio::pin!(completed);
            completed.as_mut().enable();
            if self.state.load(Ordering::Acquire) & RUNNING == 0 {
                // Closed admission and completed payload teardown make this final.
                self.cached.store(Arc::new(HashMap::new()));
                return;
            }
            completed.await;
        }
    }

    fn is_closed(&self) -> bool {
        self.state.load(Ordering::Acquire) & CLOSED != 0
    }

    pub(super) fn launch(self: &Arc<Self>, partitions: Arc<HashMap<Id128, Partition>>, limit: usize) {
        if self
            .state
            .compare_exchange(0, RUNNING, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return;
        }
        let job = SamplingJob(Arc::clone(self));
        let runtime = tokio::runtime::Handle::current();
        // Native page reads may block on cold storage. One owned job per service
        // isolates that delay and prevents repeated heartbeat ticks spawning work.
        let work = SamplingWork {
            partitions,
            runtime,
            completion: job,
        };
        tokio::task::spawn_blocking(move || work.run(limit));
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
