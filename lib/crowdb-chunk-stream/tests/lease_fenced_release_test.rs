// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use async_trait::async_trait;
use crowdb_chunk_stream::{
    memory::MemoryStreamStore, ChunkStream, Result, StreamConfig, StreamError, StreamName,
    StreamOwnerAuthority, StreamPurpose, StreamRecord, StreamRecordState, StreamRegistry,
    StreamWriterSession,
};
use std::{
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

const HOUR: Duration = Duration::from_secs(3600);

struct TestAuthority {
    deadline: Instant,
    fenced: AtomicBool,
    releases: AtomicUsize,
}

#[async_trait]
impl StreamOwnerAuthority for TestAuthority {
    fn deadline(&self) -> Result<Instant> {
        if self.fenced.load(Ordering::Acquire) {
            Err(StreamError::StaleWriter)
        } else {
            Ok(self.deadline)
        }
    }

    async fn release(&self) -> Result<()> {
        self.releases.fetch_add(1, Ordering::AcqRel);
        Ok(())
    }
}

struct TestRegistry {
    store: Arc<MemoryStreamStore>,
    session: Arc<StreamWriterSession>,
}

#[async_trait]
impl StreamRegistry for TestRegistry {
    async fn load(&self, name: StreamName) -> Result<Option<StreamRecord>> {
        self.store.load(name).await
    }

    async fn create(&self, record: StreamRecord) -> Result<()> {
        self.store.create(record).await
    }

    async fn claim_writer(&self, _name: StreamName, _epoch: u64) -> Result<Arc<StreamWriterSession>> {
        Ok(self.session.clone())
    }
}

struct TestIdleWriter {
    stream: ChunkStream,
    store: Arc<MemoryStreamStore>,
    authority: Arc<TestAuthority>,
    session: Arc<StreamWriterSession>,
}

impl TestIdleWriter {
    async fn create(low: u64) -> Self {
        // This fixed std::Instant deadline does not advance with Tokio's
        // paused clock. Revocation below is explicit, rather than a TTL race.
        let authority = Arc::new(TestAuthority {
            deadline: Instant::now() + Duration::from_secs(24 * 3600),
            fenced: AtomicBool::new(false),
            releases: AtomicUsize::new(0),
        });
        let session = StreamWriterSession::new(authority.clone());
        // An empty 8 MiB chunk exceeds the initial lookahead, so no successor
        // preparation can contribute a liveness attempt or release error.
        let store = Arc::new(MemoryStreamStore::new(8 * 1024 * 1024));
        let registry = Arc::new(TestRegistry {
            store: store.clone(),
            session: session.clone(),
        });
        let stream = ChunkStream::create(
            StreamRecord {
                state: StreamRecordState::Active,
                ..StreamRecord::creating(StreamName { high: 748, low }, None, StreamPurpose::Wal)
            },
            1,
            StreamConfig {
                liveness_interval: HOUR,
                ..StreamConfig::default()
            },
            registry,
            store.clone(),
            store.clone(),
        )
        .await
        .unwrap();
        for _ in 0..10 {
            tokio::task::yield_now().await;
        }
        assert_eq!(
            store.chunks_for_tests().await.len(),
            1,
            "no successor may affect the renewal count"
        );
        Self {
            stream,
            store,
            authority,
            session,
        }
    }

    fn assert_no_payload_or_release(&self) {
        assert_eq!(self.stream.tail(), 0);
        assert_eq!(self.stream.metrics().completed, 0);
        assert_eq!(self.store.chunk_write_count(), 0);
        assert_eq!(self.store.cursor_advance_count(), 0);
        assert_eq!(self.authority.releases.load(Ordering::Acquire), 0);
        assert!(!self.session.is_released());
    }
}

async fn enter_failed_liveness_maintenance(fixture: &TestIdleWriter) {
    fixture.store.fail_next_renewals(usize::MAX);
    tokio::time::advance(HOUR).await;
    wait_for_attempts(&fixture.store, 1).await;
    tokio::time::advance(Duration::from_millis(100)).await;
    wait_for_attempts(&fixture.store, 2).await;
    tokio::time::advance(Duration::from_millis(100)).await;
    wait_for_attempts(&fixture.store, 4).await;
    assert_eq!(fixture.store.liveness_attempt_count_for_tests(), 4);
    fixture.assert_no_payload_or_release();
}

async fn wait_for_attempts(store: &MemoryStreamStore, attempts: u64) {
    // Runnable bounded yields prevent paused Tokio time from auto-advancing
    // to the next hourly timer while waiting for a worker transition.
    for _ in 0..1000 {
        if store.liveness_attempt_count_for_tests() >= attempts {
            return;
        }
        tokio::task::yield_now().await;
    }
    panic!("liveness worker did not reach {attempts} attempts");
}

#[tokio::test(start_paused = true)]
async fn healthy_idle_writer_releases_without_appending() {
    let fixture = TestIdleWriter::create(1).await;
    fixture.assert_no_payload_or_release();
    fixture.stream.release_writer().await.unwrap();
    assert_eq!(fixture.authority.releases.load(Ordering::Acquire), 1);
    assert!(fixture.session.is_released());
    assert_eq!(fixture.store.liveness_attempt_count_for_tests(), 0);
    assert_eq!(fixture.store.chunk_write_count(), 0);
}

#[tokio::test(start_paused = true)]
async fn failed_idle_liveness_then_revocation_stalls_release_before_owner_release() {
    let fixture = TestIdleWriter::create(2).await;
    let original_deadline = fixture.authority.deadline;
    enter_failed_liveness_maintenance(&fixture).await;
    // The renewal task performs only three attempts before notifying the
    // worker. With no successor or next hourly tick, attempt four proves the
    // real worker is in maintain(), sleeping after its first failed renewal.
    assert_eq!(fixture.store.liveness_attempt_count_for_tests(), 4);
    fixture.assert_no_payload_or_release();
    fixture.authority.fenced.store(true, Ordering::Release);
    let stream = fixture.stream.clone();
    let release = tokio::spawn(async move { stream.release_writer().await });
    tokio::task::yield_now().await;
    assert!(!release.is_finished(), "maintain must still own the worker");
    tokio::time::advance(Duration::from_millis(100)).await;
    for _ in 0..1000 {
        if release.is_finished() {
            break;
        }
        tokio::task::yield_now().await;
    }
    assert!(
        release.is_finished(),
        "release command did not complete after fencing"
    );
    assert_eq!(release.await.unwrap(), Err(StreamError::WriteStalled));
    fixture.assert_no_payload_or_release();
    assert_eq!(fixture.authority.deadline, original_deadline);
    assert_eq!(fixture.store.liveness_attempt_count_for_tests(), 4);
}

#[tokio::test(start_paused = true)]
async fn transient_idle_liveness_failure_recovers_before_release_with_live_authority() {
    let fixture = TestIdleWriter::create(3).await;
    enter_failed_liveness_maintenance(&fixture).await;
    fixture.store.fail_next_renewals(0);
    let stream = fixture.stream.clone();
    let release = tokio::spawn(async move { stream.release_writer().await });
    tokio::task::yield_now().await;
    assert!(!release.is_finished(), "maintain must still own the worker");
    tokio::time::advance(Duration::from_millis(100)).await;
    for _ in 0..1000 {
        if release.is_finished() {
            break;
        }
        tokio::task::yield_now().await;
    }
    assert!(
        release.is_finished(),
        "release did not complete after liveness recovered"
    );
    release.await.unwrap().unwrap();
    assert_eq!(fixture.store.liveness_attempt_count_for_tests(), 5);
    assert_eq!(fixture.authority.releases.load(Ordering::Acquire), 1);
    assert!(fixture.session.is_released());
    assert!(!fixture.authority.fenced.load(Ordering::Acquire));
    assert_eq!(fixture.store.chunk_write_count(), 0);
    assert_eq!(fixture.store.cursor_advance_count(), 0);
}
