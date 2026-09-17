// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Bounded client-side accumulation and shared full-fragment futures.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use arc_swap::{ArcSwap, ArcSwapOption};
use bytes::Bytes;
use crowdb_protocol::chunkdb::rpc::{AdHocEcRecoveryDisposition, AdHocEcRecoveryRequest};
use crowdb_protocol::common::ChunkId;
use crowdb_protocol::diskdb::rpc::Segment;
use tokio::sync::{watch, OwnedSemaphorePermit, Semaphore};

use crate::metrics::ReadRecoveryMetrics;
use crate::ChunkAllocator;

#[derive(Clone, Copy, Hash, PartialEq, Eq)]
struct Key {
    chunk_id: ChunkId,
    revision: u64,
    strip_sequence: u32,
    segment: Segment,
}

struct Entry {
    created: Instant,
    observed: AtomicU64,
    started: AtomicBool,
    result: watch::Sender<Option<Result<Bytes, ()>>>,
    memory: ArcSwapOption<OwnedSemaphorePermit>,
}

/// A failed fragment is counted for one short window. Once enough matching
/// slice reads accumulate, one request is sent to its ChunkDB owner and all
/// simultaneous readers await the same result.
pub(crate) struct ClientRecovery {
    chunkdb: Arc<dyn ChunkAllocator>,
    entries: ArcSwap<HashMap<Key, Arc<Entry>>>,
    memory: Arc<Semaphore>,
    jobs: Arc<Semaphore>,
    threshold_bytes: u64,
    window: Duration,
    metrics: Arc<ReadRecoveryMetrics>,
}

impl ClientRecovery {
    pub(crate) fn new(
        chunkdb: Arc<dyn ChunkAllocator>,
        memory: Arc<Semaphore>,
        max_jobs: usize,
        threshold_bytes: u64,
        window: Duration,
        metrics: Arc<ReadRecoveryMetrics>,
    ) -> Self {
        Self {
            chunkdb,
            entries: ArcSwap::from_pointee(HashMap::new()),
            memory,
            jobs: Arc::new(Semaphore::new(max_jobs)),
            threshold_bytes,
            window,
            metrics,
        }
    }

    fn entry(&self, key: Key) -> Option<Arc<Entry>> {
        loop {
            let current = self.entries.load_full();
            if let Some(entry) = current.get(&key) {
                if entry.created.elapsed() < self.window
                    || (entry.started.load(Ordering::Acquire) && entry.result.borrow().is_none())
                {
                    return Some(Arc::clone(entry));
                }
            }
            let mut next = HashMap::with_capacity(current.len().min(4_096).saturating_add(1));
            for (key, entry) in current.iter() {
                if entry.created.elapsed() < self.window
                    || (entry.started.load(Ordering::Acquire) && entry.result.borrow().is_none())
                {
                    next.insert(*key, Arc::clone(entry));
                }
            }
            if next.len() >= 4_096 {
                return None;
            }
            let (result, _) = watch::channel(None);
            let entry = Arc::new(Entry {
                created: Instant::now(),
                observed: AtomicU64::new(0),
                started: AtomicBool::new(false),
                result,
                memory: ArcSwapOption::empty(),
            });
            next.insert(key, Arc::clone(&entry));
            if Arc::ptr_eq(&self.entries.compare_and_swap(&current, Arc::new(next)), &current) {
                return Some(entry);
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn observe(
        &self,
        chunk_id: ChunkId,
        revision: u64,
        strip_sequence: u32,
        segment: Segment,
        shard_bytes: u64,
        offset: u64,
        length: u32,
    ) -> Option<Bytes> {
        let key = Key {
            chunk_id,
            revision,
            strip_sequence,
            segment,
        };
        let entry = self.entry(key)?;
        self.metrics.slices.inc();
        let mut receiver = entry.result.subscribe();
        let threshold = self.threshold_bytes.min(shard_bytes / 2).max(1);
        let observed = entry
            .observed
            .fetch_add(u64::from(length), Ordering::AcqRel)
            .saturating_add(u64::from(length));
        if observed >= threshold
            && entry
                .started
                .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
        {
            let job = Arc::clone(&self.jobs).try_acquire_owned();
            let memory = u32::try_from(shard_bytes)
                .ok()
                .and_then(|bytes| Arc::clone(&self.memory).try_acquire_many_owned(bytes).ok());
            if let (Ok(job), Some(memory)) = (job, memory) {
                self.metrics.full_starts.inc();
                let chunkdb = Arc::clone(&self.chunkdb);
                let entry = Arc::clone(&entry);
                let metrics = Arc::clone(&self.metrics);
                tokio::spawn(async move {
                    run_full_request(chunkdb, entry, key, shard_bytes, job, memory, metrics).await;
                });
            } else {
                self.metrics.rejected.inc();
                entry.result.send_replace(Some(Err(())));
            }
        } else if entry.started.load(Ordering::Acquire) {
            self.metrics.coalesced.inc();
        }
        if !entry.started.load(Ordering::Acquire) {
            self.metrics.fallback_background.inc();
            return None;
        }
        let result = tokio::time::timeout(Duration::from_secs(30), async {
            loop {
                if let Some(result) = receiver.borrow_and_update().clone() {
                    return result.ok();
                }
                receiver.changed().await.ok()?;
            }
        })
        .await
        .ok()
        .flatten()?;
        let start = usize::try_from(offset).ok()?;
        let end = start.checked_add(length as usize)?;
        let slice = result.get(start..end).map(Bytes::copy_from_slice);
        if slice.is_some() {
            self.metrics.bytes_reused.inc_by(u64::from(length));
        }
        slice
    }
}

async fn run_full_request(
    chunkdb: Arc<dyn ChunkAllocator>,
    entry: Arc<Entry>,
    key: Key,
    shard_bytes: u64,
    _job: OwnedSemaphorePermit,
    memory: OwnedSemaphorePermit,
    metrics: Arc<ReadRecoveryMetrics>,
) {
    let operation_id = ChunkId {
        high: key.chunk_id.high ^ key.segment.allocation_ts ^ u64::from(key.strip_sequence),
        low: key.chunk_id.low ^ key.segment.unit_offset ^ key.revision,
    };
    let request = AdHocEcRecoveryRequest {
        version: 1,
        chunk_id: Some(key.chunk_id),
        expected_modify_ts: key.revision,
        strip_sequence: key.strip_sequence,
        failed_segment: Some(key.segment),
        operation_id: Some(operation_id),
        request_full_block: true,
    };
    let result = match chunkdb.ad_hoc_ec_recovery(request).await {
        Ok(response)
            if matches!(
                response.disposition,
                AdHocEcRecoveryDisposition::Started | AdHocEcRecoveryDisposition::Coalesced
            ) && u64::try_from(response.data.len()).ok() == Some(shard_bytes) =>
        {
            Ok(Bytes::from(response.data))
        }
        Ok(response) => {
            if response.disposition == AdHocEcRecoveryDisposition::Stale {
                metrics.stale.inc();
            }
            metrics.fallback_background.inc();
            Err(())
        }
        Err(_) => {
            metrics.fallback_background.inc();
            Err(())
        }
    };
    if result.is_ok() {
        entry.memory.store(Some(Arc::new(memory)));
    }
    entry.result.send_replace(Some(result));
}
