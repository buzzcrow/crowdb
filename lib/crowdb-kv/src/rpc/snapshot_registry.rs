// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use dashmap::DashMap;
use tokio::sync::{mpsc, oneshot, OwnedSemaphorePermit, Semaphore};

use crate::kv::{SnapshotChunk, SnapshotExporter, SnapshotMetadata};
use crate::metrics::{Counter, Gauge, LatencySummary, MetricsRegistry};

pub(crate) struct SnapshotSourceMetrics {
    active: Arc<Gauge>,
    rejected: Arc<Counter>,
    expired: Arc<Counter>,
    aborted: Arc<Counter>,
    completed: Arc<Counter>,
    bytes: Arc<Counter>,
    chunks: Arc<Counter>,
    retries: Arc<Counter>,
    integrity_failures: Arc<Counter>,
    export_latency: Arc<LatencySummary>,
}

impl SnapshotSourceMetrics {
    pub(crate) fn register(registry: &mut MetricsRegistry, store_id: u64) -> Arc<Self> {
        let prefix = format!("s.{store_id}.snapshot.source");
        Arc::new(Self {
            active: registry.register_gauge(format!("{prefix}.active.g")),
            rejected: registry.register_counter(format!("{prefix}.rejected.c")),
            expired: registry.register_counter(format!("{prefix}.expired.c")),
            aborted: registry.register_counter(format!("{prefix}.aborted.c")),
            completed: registry.register_counter(format!("{prefix}.completed.c")),
            bytes: registry.register_counter(format!("{prefix}.bytes.c")),
            chunks: registry.register_counter(format!("{prefix}.chunks.c")),
            retries: registry.register_counter(format!("{prefix}.exact_retries.c")),
            integrity_failures: registry.register_counter(format!("{prefix}.integrity_failures.c")),
            export_latency: registry.register_summary(format!("{prefix}.export.l")),
        })
    }

    pub(crate) fn observe_export(&self, elapsed: Duration) {
        self.export_latency
            .observe(elapsed.as_nanos().try_into().unwrap_or(u64::MAX));
    }
}

#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
pub(crate) struct SnapshotSessionId {
    pub boot_nonce: u64,
    pub session_number: u64,
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum SnapshotSessionError {
    NotFound,
    Expired,
    Backpressure,
    InvalidOffset { expected: u64 },
    Export(String),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct SnapshotRead {
    pub chunk: SnapshotChunk,
    pub payload_crc32c: u32,
}

#[derive(Debug)]
pub(crate) struct SnapshotRegistration {
    pub id: SnapshotSessionId,
    pub group_id: u64,
    pub membership_epoch: u64,
    pub metadata: SnapshotMetadata,
}

struct RegistryEntry {
    group_id: u64,
    membership_epoch: u64,
    metadata: SnapshotMetadata,
    last_touch_ms: AtomicU64,
    tx: mpsc::Sender<SessionCommand>,
}

enum SessionCommand {
    Read {
        offset: u64,
        reply: oneshot::Sender<Result<SnapshotRead, SnapshotSessionError>>,
    },
    Finish {
        final_offset: u64,
        reply: oneshot::Sender<Result<(), SnapshotSessionError>>,
    },
    Abort,
}

pub(crate) struct SnapshotRegistry {
    boot_nonce: u64,
    next_session: AtomicU64,
    started: Instant,
    lease_ms: u64,
    permits: Arc<Semaphore>,
    sessions: DashMap<SnapshotSessionId, Arc<RegistryEntry>>,
    expired: DashMap<SnapshotSessionId, u64>,
    metrics: Option<Arc<SnapshotSourceMetrics>>,
}

impl SnapshotRegistry {
    pub(crate) fn new(
        capacity: usize,
        lease: Duration,
        metrics: Option<Arc<SnapshotSourceMetrics>>,
    ) -> Arc<Self> {
        let time_nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |duration| {
                duration.as_secs() ^ u64::from(duration.subsec_nanos()).rotate_left(32)
            });
        Arc::new(Self {
            boot_nonce: time_nonce ^ u64::from(std::process::id()).rotate_left(32),
            next_session: AtomicU64::new(1),
            started: Instant::now(),
            lease_ms: lease.as_millis().try_into().unwrap_or(u64::MAX),
            permits: Arc::new(Semaphore::new(capacity)),
            sessions: DashMap::new(),
            expired: DashMap::new(),
            metrics,
        })
    }

    pub(crate) fn begin(
        &self,
        group_id: u64,
        membership_epoch: u64,
        exporter: Box<dyn SnapshotExporter>,
        permit: OwnedSemaphorePermit,
    ) -> SnapshotRegistration {
        let id = SnapshotSessionId {
            boot_nonce: self.boot_nonce,
            session_number: self.next_session.fetch_add(1, Ordering::Relaxed),
        };
        let metadata = exporter.metadata();
        let (tx, rx) = mpsc::channel(1);
        self.sessions.insert(
            id,
            Arc::new(RegistryEntry {
                group_id,
                membership_epoch,
                metadata,
                last_touch_ms: AtomicU64::new(self.now_ms()),
                tx,
            }),
        );
        if let Some(metrics) = &self.metrics {
            metrics.active.inc();
        }
        tokio::spawn(run_session(exporter, rx, permit, self.metrics.clone()));
        SnapshotRegistration {
            id,
            group_id,
            membership_epoch,
            metadata,
        }
    }

    pub(crate) fn reserve(&self) -> Result<OwnedSemaphorePermit, SnapshotSessionError> {
        Arc::clone(&self.permits).try_acquire_owned().map_err(|_| {
            if let Some(metrics) = &self.metrics {
                metrics.rejected.inc();
            }
            SnapshotSessionError::Backpressure
        })
    }

    pub(crate) fn observe_export(&self, elapsed: Duration) {
        if let Some(metrics) = &self.metrics {
            metrics.observe_export(elapsed);
        }
    }

    pub(crate) fn registration(
        &self,
        id: SnapshotSessionId,
    ) -> Result<SnapshotRegistration, SnapshotSessionError> {
        let Some(entry) = self.sessions.get(&id) else {
            return Err(self.missing_error(id));
        };
        Ok(SnapshotRegistration {
            id,
            group_id: entry.group_id,
            membership_epoch: entry.membership_epoch,
            metadata: entry.metadata,
        })
    }

    pub(crate) async fn read(
        &self,
        id: SnapshotSessionId,
        offset: u64,
    ) -> Result<SnapshotRead, SnapshotSessionError> {
        let entry = self.entry(id)?;
        let (reply, response) = oneshot::channel();
        entry
            .tx
            .send(SessionCommand::Read { offset, reply })
            .await
            .map_err(|_| SnapshotSessionError::NotFound)?;
        let result = response.await.map_err(|_| SnapshotSessionError::NotFound)?;
        if result.is_ok() {
            entry.last_touch_ms.store(self.now_ms(), Ordering::Relaxed);
        }
        result
    }

    pub(crate) async fn finish(
        &self,
        id: SnapshotSessionId,
        final_offset: u64,
    ) -> Result<(), SnapshotSessionError> {
        let Some(entry) = self.sessions.get(&id).map(|entry| Arc::clone(entry.value())) else {
            return if self.expired.contains_key(&id) {
                Err(SnapshotSessionError::Expired)
            } else {
                Ok(())
            };
        };
        let (reply, response) = oneshot::channel();
        entry
            .tx
            .send(SessionCommand::Finish { final_offset, reply })
            .await
            .map_err(|_| SnapshotSessionError::NotFound)?;
        let result = response.await.map_err(|_| SnapshotSessionError::NotFound)?;
        if result.is_ok() {
            self.sessions.remove(&id);
            if let Some(metrics) = &self.metrics {
                metrics.completed.inc();
            }
        }
        result
    }

    pub(crate) async fn abort(&self, id: SnapshotSessionId) {
        if let Some((_, entry)) = self.sessions.remove(&id) {
            if let Some(metrics) = &self.metrics {
                metrics.aborted.inc();
            }
            let _ = entry.tx.send(SessionCommand::Abort).await;
        }
    }

    pub(crate) fn expire(&self, id: SnapshotSessionId) {
        if let Some((_, entry)) = self.sessions.remove(&id) {
            self.expired.insert(id, self.now_ms());
            if let Some(metrics) = &self.metrics {
                metrics.expired.inc();
            }
            let _ = entry.tx.try_send(SessionCommand::Abort);
        }
    }

    pub(crate) fn reap_expired(&self) -> usize {
        let now = self.now_ms();
        let stale_tombstones: Vec<_> = self
            .expired
            .iter()
            .filter(|entry| now.saturating_sub(*entry.value()) >= self.lease_ms)
            .map(|entry| *entry.key())
            .collect();
        for id in stale_tombstones {
            self.expired.remove(&id);
        }
        let ids: Vec<_> = self
            .sessions
            .iter()
            .filter(|entry| now.saturating_sub(entry.last_touch_ms.load(Ordering::Relaxed)) >= self.lease_ms)
            .map(|entry| *entry.key())
            .collect();
        for id in &ids {
            self.expire(*id);
        }
        ids.len()
    }

    pub(crate) fn shutdown(&self) {
        let ids: Vec<_> = self.sessions.iter().map(|entry| *entry.key()).collect();
        for id in ids {
            if let Some((_, entry)) = self.sessions.remove(&id) {
                let _ = entry.tx.try_send(SessionCommand::Abort);
            }
        }
    }

    fn entry(&self, id: SnapshotSessionId) -> Result<Arc<RegistryEntry>, SnapshotSessionError> {
        self.sessions
            .get(&id)
            .map(|entry| Arc::clone(entry.value()))
            .ok_or_else(|| self.missing_error(id))
    }

    fn missing_error(&self, id: SnapshotSessionId) -> SnapshotSessionError {
        if self.expired.contains_key(&id) {
            SnapshotSessionError::Expired
        } else {
            SnapshotSessionError::NotFound
        }
    }

    fn now_ms(&self) -> u64 {
        self.started.elapsed().as_millis().try_into().unwrap_or(u64::MAX)
    }
}

async fn run_session(
    mut exporter: Box<dyn SnapshotExporter>,
    mut rx: mpsc::Receiver<SessionCommand>,
    _permit: OwnedSemaphorePermit,
    metrics: Option<Arc<SnapshotSourceMetrics>>,
) {
    let metadata = exporter.metadata();
    let mut last_read: Option<SnapshotRead> = None;
    while let Some(command) = rx.recv().await {
        match command {
            SessionCommand::Read { offset, reply } => {
                let retry = last_read.as_ref().is_some_and(|read| read.chunk.offset == offset);
                let result = if retry {
                    if let Some(metrics) = &metrics {
                        metrics.retries.inc();
                    }
                    Ok(last_read.clone().expect("last_read checked"))
                } else if last_read.as_ref().is_some_and(|read| read.chunk.done)
                    || offset != exporter.offset()
                {
                    Err(SnapshotSessionError::InvalidOffset {
                        expected: exporter.offset(),
                    })
                } else {
                    match exporter.read(offset) {
                        Ok(chunk) => Ok(SnapshotRead {
                            payload_crc32c: crowdb_tree_ffi::crc32c(&chunk.bytes),
                            chunk,
                        }),
                        Err(error) => {
                            if let Some(metrics) = &metrics {
                                metrics.integrity_failures.inc();
                            }
                            Err(SnapshotSessionError::Export(error))
                        }
                    }
                };
                if let Ok(read) = &result {
                    if !retry {
                        if let Some(metrics) = &metrics {
                            metrics.bytes.inc_by(read.chunk.bytes.len() as u64);
                            metrics.chunks.inc();
                        }
                    }
                    last_read = Some(read.clone());
                }
                let _ = reply.send(result);
            }
            SessionCommand::Finish { final_offset, reply } => {
                let result = if final_offset == metadata.total_bytes && exporter.offset() == final_offset {
                    Ok(())
                } else {
                    Err(SnapshotSessionError::InvalidOffset {
                        expected: exporter.offset(),
                    })
                };
                let done = result.is_ok();
                let _ = reply.send(result);
                if done {
                    break;
                }
            }
            SessionCommand::Abort => break,
        }
    }
    if let Some(metrics) = &metrics {
        metrics.active.dec();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kv::SnapshotFormat;

    struct Exporter {
        data: Vec<u8>,
        offset: usize,
        chunk_bytes: usize,
    }

    impl SnapshotExporter for Exporter {
        fn metadata(&self) -> SnapshotMetadata {
            SnapshotMetadata {
                format: SnapshotFormat::InMemoryTest,
                at_slot: 7,
                total_bytes: self.data.len() as u64,
                final_crc32c: crowdb_tree_ffi::crc32c(&self.data),
                chunk_bytes: self.chunk_bytes,
            }
        }

        fn offset(&self) -> u64 {
            self.offset as u64
        }

        fn read(&mut self, offset: u64) -> Result<SnapshotChunk, String> {
            let end = self.data.len().min(self.offset + self.chunk_bytes);
            let chunk = SnapshotChunk {
                offset,
                bytes: self.data[self.offset..end].to_vec(),
                done: end == self.data.len(),
            };
            self.offset = end;
            Ok(chunk)
        }
    }

    fn exporter() -> Box<dyn SnapshotExporter> {
        Box::new(Exporter {
            data: b"abcdefgh".to_vec(),
            offset: 0,
            chunk_bytes: 4,
        })
    }

    #[tokio::test]
    async fn exact_retry_is_identical_and_capacity_releases_on_finish() {
        let registry = SnapshotRegistry::new(1, Duration::from_secs(30), None);
        let first = registry.begin(2, 3, exporter(), registry.reserve().unwrap());
        assert_eq!(
            registry.reserve().unwrap_err(),
            SnapshotSessionError::Backpressure
        );
        let read = registry.read(first.id, 0).await.unwrap();
        let retry = registry.read(first.id, 0).await.unwrap();
        assert_eq!(read, retry);
        assert_eq!(
            registry.read(first.id, 2).await.unwrap_err(),
            SnapshotSessionError::InvalidOffset { expected: 4 }
        );
        let second = registry.read(first.id, 4).await.unwrap();
        assert!(second.chunk.done);
        assert_eq!(
            registry.read(first.id, 8).await.unwrap_err(),
            SnapshotSessionError::InvalidOffset { expected: 8 }
        );
        assert_eq!(
            registry.finish(first.id, 7).await.unwrap_err(),
            SnapshotSessionError::InvalidOffset { expected: 8 }
        );
        registry.finish(first.id, 8).await.unwrap();
        registry.finish(first.id, 8).await.unwrap();
        tokio::task::yield_now().await;
        assert!(registry.reserve().is_ok());
    }

    #[tokio::test]
    async fn expiry_is_typed_and_releases_capacity() {
        let registry = SnapshotRegistry::new(1, Duration::from_millis(1), None);
        let first = registry.begin(2, 3, exporter(), registry.reserve().unwrap());
        tokio::time::sleep(Duration::from_millis(2)).await;
        assert_eq!(registry.reap_expired(), 1);
        assert_eq!(
            registry.read(first.id, 0).await.unwrap_err(),
            SnapshotSessionError::Expired
        );
        tokio::task::yield_now().await;
        assert!(registry.reserve().is_ok());
    }

    #[tokio::test]
    async fn abort_and_shutdown_release_capacity() {
        let registry = SnapshotRegistry::new(2, Duration::from_secs(30), None);
        let first = registry.begin(2, 3, exporter(), registry.reserve().unwrap());
        let _second = registry.begin(2, 3, exporter(), registry.reserve().unwrap());
        assert_eq!(
            registry.reserve().unwrap_err(),
            SnapshotSessionError::Backpressure
        );

        registry.abort(first.id).await;
        tokio::task::yield_now().await;
        let replacement = registry.reserve().unwrap();
        drop(replacement);

        registry.shutdown();
        tokio::task::yield_now().await;
        assert_eq!(registry.sessions.len(), 0);
        assert_eq!(registry.permits.available_permits(), 2);
    }
}
