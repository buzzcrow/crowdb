// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use async_trait::async_trait;
use bytes::Bytes;
use crowdb_access_s3::{
    auth::TrustedNetworkAuthenticator,
    metrics::{DependencyHealth, S3Health, S3Metrics},
    route::S3Route,
};
use crowdb_access_server::s3::{
    DependencyProbeFuture, HealthObserver, ProductionS3Operations, S3Dispatcher, S3Operations,
    S3OperationsFuture,
};
use crowdb_chunk_client::{metrics::ReadinessMetrics, ChunkIoClient, DiskWriter, Result};
use crowdb_paxos_kv_client::DiskRecord;
use crowdb_protocol::{
    common::{DiskId, HwStatus},
    diskdb::rpc::{DiskValue, Segment},
};
use http_body_util::BodyExt;
use hyper::{body::Incoming, Request};
use serde_json::Value;
use std::{
    sync::{
        atomic::{AtomicU64, AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::time::Instant;

pub const HELD_OUTER: u64 = u64::MAX;

#[derive(Default)]
pub struct TestDisk {
    pub active: AtomicUsize,
}

struct TestRead<'a>(&'a AtomicUsize);
impl Drop for TestRead<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

#[async_trait]
impl DiskWriter for TestDisk {
    async fn write(&self, _: &Segment, _: u64, _: Bytes) -> Result<()> {
        panic!("probe must not write");
    }
    async fn write_at_byte_offset(&self, _: &Segment, _: u64, _: u64, _: Bytes) -> Result<()> {
        panic!("probe must not write");
    }
    async fn read_priority(&self, segment: &Segment, unit: u64, offset: u64, length: u32) -> Result<Bytes> {
        assert!(segment.disk_id.is_some());
        assert_eq!((unit, offset, length), (4096, 0, 4096));
        self.active.fetch_add(1, Ordering::SeqCst);
        let _read = TestRead(&self.active);
        tokio::time::sleep(Duration::from_millis(100)).await;
        Ok(Bytes::from(vec![7; 4096]))
    }
}

pub struct TestOperations {
    pub delay_ms: AtomicU64,
    pub calls: AtomicUsize,
    pub active: AtomicUsize,
    pub peak: AtomicUsize,
    pub cancelled: AtomicUsize,
    pub metrics: Arc<ReadinessMetrics>,
    pub disk: Arc<TestDisk>,
}

struct TestRound<'a> {
    state: &'a TestOperations,
    completed: bool,
}
impl Drop for TestRound<'_> {
    fn drop(&mut self) {
        self.state.active.fetch_sub(1, Ordering::SeqCst);
        if !self.completed {
            self.state.cancelled.fetch_add(1, Ordering::SeqCst);
        }
    }
}

fn disks() -> Vec<DiskRecord> {
    vec![DiskRecord {
        rack_id: 1,
        node_id: 1,
        disk_group_id: 1,
        disk_id: DiskId { high: 1, low: 1 },
        value: DiskValue {
            status: HwStatus::Up as i32,
            ..Default::default()
        },
    }]
}

impl S3Operations for TestOperations {
    fn observe_dependencies(self: Arc<Self>) -> DependencyProbeFuture {
        Box::pin(async move {
            self.calls.fetch_add(1, Ordering::SeqCst);
            let count = self.active.fetch_add(1, Ordering::SeqCst) + 1;
            self.peak.fetch_max(count, Ordering::SeqCst);
            let mut round = TestRound {
                state: self.as_ref(),
                completed: false,
            };
            let delay = self.delay_ms.load(Ordering::SeqCst);
            if delay == HELD_OUTER {
                std::future::pending::<()>().await;
            }
            let metrics = self.metrics.clone();
            let disk = self.disk.clone();
            let observed = ProductionS3Operations::observe_storage_dependencies_for_tests(
                async move {
                    tokio::time::sleep(Duration::from_millis(delay)).await;
                    Ok("current metadata read".into())
                },
                move |budget| async move {
                    let online = ChunkIoClient::observe_readiness_dependencies_for_tests(
                        Instant::now() + budget,
                        &metrics,
                        disk,
                        async { Ok(disks()) },
                        async move {
                            tokio::time::sleep(Duration::from_millis(delay)).await;
                            Ok(())
                        },
                    )
                    .await
                    .map_err(|error| error.to_string())?;
                    Ok(format!("current physical read on {online} disk"))
                },
            )
            .await;
            round.completed = true;
            Some(observed)
        })
    }
    fn execute(
        self: Arc<Self>,
        _: S3Route,
        _: Request<Incoming>,
        _: String,
        _: String,
    ) -> S3OperationsFuture {
        panic!("health must not execute application requests");
    }
}

pub struct TestSampler {
    pub dispatcher: Arc<S3Dispatcher>,
    pub operations: Arc<TestOperations>,
    pub observer: Option<HealthObserver>,
}
impl TestSampler {
    pub fn new(delay_ms: u64) -> Self {
        let operations = Arc::new(TestOperations {
            delay_ms: AtomicU64::new(delay_ms),
            calls: AtomicUsize::new(0),
            active: AtomicUsize::new(0),
            peak: AtomicUsize::new(0),
            cancelled: AtomicUsize::new(0),
            metrics: Arc::default(),
            disk: Arc::default(),
        });
        let health = Arc::new(S3Health::starting(1));
        health.set_listener(DependencyHealth::Ready);
        let dispatcher = Arc::new(
            S3Dispatcher::new(
                Arc::new(TrustedNetworkAuthenticator::new()),
                operations.clone(),
                Arc::new(S3Metrics::default()),
                "probe-test".into(),
                false,
            )
            .with_health(health),
        );
        let observer = Some(dispatcher.start_health_observer().unwrap());
        Self {
            dispatcher,
            operations,
            observer,
        }
    }
    pub async fn read(&self, status: u16) -> Value {
        let response = self.dispatcher.health_response_for_tests();
        assert_eq!(response.status().as_u16(), status);
        serde_json::from_slice(&response.into_body().collect().await.unwrap().to_bytes()).unwrap()
    }
    pub fn generation(&self) -> u64 {
        self.dispatcher.health_generation_for_tests()
    }
}
impl Drop for TestSampler {
    fn drop(&mut self) {
        self.observer.take();
    }
}

pub async fn settle() {
    for _ in 0..8 {
        tokio::task::yield_now().await;
    }
}
pub async fn advance(milliseconds: u64) {
    tokio::time::advance(Duration::from_millis(milliseconds)).await;
    settle().await;
}

#[derive(Default)]
pub struct TestFailureWarnings {
    pub count: AtomicU64,
    pub generation: AtomicU64,
    pub budget_ms: AtomicU64,
    pub elapsed_ms: AtomicU64,
    pub checked_at_unix_ms: AtomicU64,
}

struct TestWarningLayer(Arc<TestFailureWarnings>);
struct TestWarningFields<'a>(&'a TestFailureWarnings);
impl tracing::field::Visit for TestWarningFields<'_> {
    fn record_debug(&mut self, _: &tracing::field::Field, _: &dyn std::fmt::Debug) {}
    fn record_u64(&mut self, field: &tracing::field::Field, value: u64) {
        let target = match field.name() {
            "generation" => &self.0.generation,
            "configured_budget_ms" => &self.0.budget_ms,
            "total_elapsed_ms" => &self.0.elapsed_ms,
            "checked_at_unix_ms" => &self.0.checked_at_unix_ms,
            _ => return,
        };
        target.store(value, Ordering::SeqCst);
    }
}
impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for TestWarningLayer {
    fn on_event(&self, event: &tracing::Event<'_>, _: tracing_subscriber::layer::Context<'_, S>) {
        if event.metadata().level() == &tracing::Level::WARN
            && event.metadata().fields().field("configured_budget_ms").is_some()
        {
            self.0.count.fetch_add(1, Ordering::SeqCst);
            event.record(&mut TestWarningFields(self.0.as_ref()));
        }
    }
}

pub fn failure_warnings() -> (Arc<TestFailureWarnings>, tracing::subscriber::DefaultGuard) {
    use tracing_subscriber::prelude::*;
    let observed = Arc::new(TestFailureWarnings::default());
    let subscriber = tracing_subscriber::registry().with(TestWarningLayer(observed.clone()));
    let guard = tracing::subscriber::set_default(subscriber);
    (observed, guard)
}
