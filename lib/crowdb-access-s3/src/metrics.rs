// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Lock-free bounded-cardinality S3 request counters.

use std::fmt::Write as _;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicU8, Ordering};

use crowdb_chunk_client::{
    ChunkIoClient, LargeWriteBufferMetricsSnapshot, LargeWriteRepairMetricsSnapshot,
    SmallWriteMetricsSnapshot,
};

use crate::native_buffer::{NativeBodyAllocator, NativeBufferMetricsSnapshot};

const OPERATION_COUNT: usize = 9;
const OUTCOME_COUNT: usize = 6;
const OPERATION_NAMES: [&str; OPERATION_COUNT] = [
    "create_bucket",
    "head_bucket",
    "list_buckets",
    "delete_bucket",
    "put_object",
    "head_object",
    "get_object",
    "list_objects_v2",
    "delete_object",
];
const OUTCOME_NAMES: [&str; OUTCOME_COUNT] = [
    "success",
    "client_error",
    "throttled",
    "timeout",
    "unavailable",
    "internal",
];

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OutcomeClass {
    Success,
    ClientError,
    Throttled,
    Timeout,
    Unavailable,
    Internal,
}

pub struct S3Metrics {
    requests: [[AtomicU64; OUTCOME_COUNT]; OPERATION_COUNT],
    request_latency_ns: [[AtomicU64; OUTCOME_COUNT]; OPERATION_COUNT],
    time_to_first_byte_ns: [[AtomicU64; OUTCOME_COUNT]; OPERATION_COUNT],
    authentication_latency_ns: [[AtomicU64; OUTCOME_COUNT]; OPERATION_COUNT],
    operation_latency_ns: [[AtomicU64; OUTCOME_COUNT]; OPERATION_COUNT],
    predispatch_requests: [AtomicU64; OUTCOME_COUNT],
    predispatch_latency_ns: [AtomicU64; OUTCOME_COUNT],
    request_bytes: AtomicU64,
    response_bytes: AtomicU64,
    trusted_auth_bypass: AtomicU64,
    in_flight: AtomicU64,
    max_in_flight: AtomicU64,
    checksum_bytes: AtomicU64,
    metadata_retries: AtomicU64,
    cleanup_enqueued: AtomicU64,
    cleanup_completed: AtomicU64,
    cleanup_failed: AtomicU64,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct RequestMeasurement {
    pub request_bytes: u64,
    pub response_bytes: u64,
    pub latency_ns: u64,
    pub authentication_latency_ns: u64,
    pub operation_latency_ns: u64,
}

impl Default for S3Metrics {
    fn default() -> Self {
        Self {
            requests: std::array::from_fn(|_| std::array::from_fn(|_| AtomicU64::new(0))),
            request_latency_ns: std::array::from_fn(|_| std::array::from_fn(|_| AtomicU64::new(0))),
            time_to_first_byte_ns: std::array::from_fn(|_| std::array::from_fn(|_| AtomicU64::new(0))),
            authentication_latency_ns: std::array::from_fn(|_| std::array::from_fn(|_| AtomicU64::new(0))),
            operation_latency_ns: std::array::from_fn(|_| std::array::from_fn(|_| AtomicU64::new(0))),
            predispatch_requests: std::array::from_fn(|_| AtomicU64::new(0)),
            predispatch_latency_ns: std::array::from_fn(|_| AtomicU64::new(0)),
            request_bytes: AtomicU64::new(0),
            response_bytes: AtomicU64::new(0),
            trusted_auth_bypass: AtomicU64::new(0),
            in_flight: AtomicU64::new(0),
            max_in_flight: AtomicU64::new(0),
            checksum_bytes: AtomicU64::new(0),
            metadata_retries: AtomicU64::new(0),
            cleanup_enqueued: AtomicU64::new(0),
            cleanup_completed: AtomicU64::new(0),
            cleanup_failed: AtomicU64::new(0),
        }
    }
}

impl S3Metrics {
    pub fn finish_request(
        &self,
        operation: crate::route::S3Operation,
        outcome: OutcomeClass,
        measurement: RequestMeasurement,
    ) {
        self.requests[operation as usize][outcome as usize].fetch_add(1, Ordering::Relaxed);
        self.request_latency_ns[operation as usize][outcome as usize]
            .fetch_add(measurement.latency_ns, Ordering::Relaxed);
        self.authentication_latency_ns[operation as usize][outcome as usize]
            .fetch_add(measurement.authentication_latency_ns, Ordering::Relaxed);
        self.operation_latency_ns[operation as usize][outcome as usize]
            .fetch_add(measurement.operation_latency_ns, Ordering::Relaxed);
        self.request_bytes
            .fetch_add(measurement.request_bytes, Ordering::Relaxed);
        self.response_bytes
            .fetch_add(measurement.response_bytes, Ordering::Relaxed);
    }

    pub fn record_trusted_auth_bypass(&self) {
        self.trusted_auth_bypass.fetch_add(1, Ordering::Relaxed);
    }

    pub fn finish_predispatch(&self, outcome: OutcomeClass, latency_ns: u64) {
        self.predispatch_requests[outcome as usize].fetch_add(1, Ordering::Relaxed);
        self.predispatch_latency_ns[outcome as usize].fetch_add(latency_ns, Ordering::Relaxed);
    }

    #[must_use]
    pub fn begin_request(&self) -> InFlightRequest<'_> {
        let current = self.in_flight.fetch_add(1, Ordering::Relaxed) + 1;
        self.max_in_flight.fetch_max(current, Ordering::Relaxed);
        InFlightRequest(self)
    }

    pub fn record_time_to_first_byte(
        &self,
        operation: crate::route::S3Operation,
        outcome: OutcomeClass,
        nanoseconds: u64,
    ) {
        self.time_to_first_byte_ns[operation as usize][outcome as usize]
            .fetch_add(nanoseconds, Ordering::Relaxed);
    }

    pub fn record_checksum_bytes(&self, bytes: usize) {
        self.checksum_bytes.fetch_add(bytes as u64, Ordering::Relaxed);
    }

    pub fn record_metadata_retry(&self) {
        self.metadata_retries.fetch_add(1, Ordering::Relaxed);
    }

    pub fn enqueue_cleanup(&self, targets: usize) {
        self.cleanup_enqueued.fetch_add(targets as u64, Ordering::Relaxed);
    }

    pub fn complete_cleanup(&self, targets: usize) {
        self.cleanup_completed
            .fetch_add(targets as u64, Ordering::Relaxed);
    }

    pub fn fail_cleanup(&self, targets: usize) {
        self.cleanup_failed.fetch_add(targets as u64, Ordering::Relaxed);
    }

    #[must_use]
    pub fn snapshot(&self) -> S3MetricsSnapshot {
        S3MetricsSnapshot {
            requests: std::array::from_fn(|operation| {
                std::array::from_fn(|outcome| self.requests[operation][outcome].load(Ordering::Relaxed))
            }),
            request_latency_ns: std::array::from_fn(|operation| {
                std::array::from_fn(|outcome| {
                    self.request_latency_ns[operation][outcome].load(Ordering::Relaxed)
                })
            }),
            time_to_first_byte_ns: snapshot_matrix(&self.time_to_first_byte_ns),
            authentication_latency_ns: snapshot_matrix(&self.authentication_latency_ns),
            operation_latency_ns: snapshot_matrix(&self.operation_latency_ns),
            predispatch_requests: std::array::from_fn(|outcome| {
                self.predispatch_requests[outcome].load(Ordering::Relaxed)
            }),
            predispatch_latency_ns: std::array::from_fn(|outcome| {
                self.predispatch_latency_ns[outcome].load(Ordering::Relaxed)
            }),
            request_bytes: self.request_bytes.load(Ordering::Relaxed),
            response_bytes: self.response_bytes.load(Ordering::Relaxed),
            trusted_auth_bypass: self.trusted_auth_bypass.load(Ordering::Relaxed),
            in_flight: self.in_flight.load(Ordering::Relaxed),
            max_in_flight: self.max_in_flight.load(Ordering::Relaxed),
            checksum_bytes: self.checksum_bytes.load(Ordering::Relaxed),
            metadata_retries: self.metadata_retries.load(Ordering::Relaxed),
            cleanup_enqueued: self.cleanup_enqueued.load(Ordering::Relaxed),
            cleanup_completed: self.cleanup_completed.load(Ordering::Relaxed),
            cleanup_failed: self.cleanup_failed.load(Ordering::Relaxed),
        }
    }

    #[must_use]
    pub fn data_path_snapshot(
        &self,
        native: &NativeBodyAllocator,
        chunks: &ChunkIoClient,
    ) -> S3DataPathMetricsSnapshot {
        let request = self.snapshot();
        S3DataPathMetricsSnapshot {
            native: native.metrics_snapshot(),
            large_write_buffers: chunks.large_write_buffer_metrics(),
            large_write_repairs: chunks.large_write_repair_metrics(),
            small_writes: chunks.small_write_metrics(),
            checksum_bytes: request.checksum_bytes,
            metadata_retries: request.metadata_retries,
            cleanup_enqueued: request.cleanup_enqueued,
            cleanup_completed: request.cleanup_completed,
            cleanup_failed: request.cleanup_failed,
            cleanup_backlog: request.cleanup_enqueued.saturating_sub(request.cleanup_completed),
        }
    }

    #[must_use]
    pub fn render_prometheus(
        &self,
        native: Option<&NativeBodyAllocator>,
        chunks: Option<&ChunkIoClient>,
    ) -> String {
        let snapshot = self.snapshot();
        let mut output = String::with_capacity(24 * 1024);
        append_request_metrics(&mut output, &snapshot);
        append_metric(
            &mut output,
            "crowdb_s3_request_bytes_total",
            "",
            snapshot.request_bytes,
        );
        append_metric(
            &mut output,
            "crowdb_s3_response_bytes_total",
            "",
            snapshot.response_bytes,
        );
        append_metric(
            &mut output,
            "crowdb_s3_requests_in_flight",
            "",
            snapshot.in_flight,
        );
        append_metric(
            &mut output,
            "crowdb_s3_checksum_bytes_total",
            "",
            snapshot.checksum_bytes,
        );
        append_metric(
            &mut output,
            "crowdb_s3_metadata_retries_total",
            "",
            snapshot.metadata_retries,
        );
        append_metric(
            &mut output,
            "crowdb_s3_cleanup_backlog",
            "",
            snapshot
                .cleanup_enqueued
                .saturating_sub(snapshot.cleanup_completed),
        );
        if let Some(native) = native {
            append_native_metrics(&mut output, native.metrics_snapshot());
        }
        if let Some(chunks) = chunks {
            append_chunk_metrics(&mut output, chunks.large_write_buffer_metrics());
        }
        output
    }
}

fn append_request_metrics(output: &mut String, snapshot: &S3MetricsSnapshot) {
    let metric_matrices = [
        ("crowdb_s3_requests_total", &snapshot.requests),
        ("crowdb_s3_request_latency_ns_total", &snapshot.request_latency_ns),
        (
            "crowdb_s3_time_to_first_byte_ns_total",
            &snapshot.time_to_first_byte_ns,
        ),
        (
            "crowdb_s3_authentication_latency_ns_total",
            &snapshot.authentication_latency_ns,
        ),
        (
            "crowdb_s3_operation_latency_ns_total",
            &snapshot.operation_latency_ns,
        ),
    ];
    for (name, matrix) in metric_matrices {
        for (operation, operation_name) in OPERATION_NAMES.iter().enumerate() {
            for (outcome, outcome_name) in OUTCOME_NAMES.iter().enumerate() {
                let labels = format!(r#"operation="{operation_name}",outcome="{outcome_name}""#);
                append_metric(output, name, &labels, matrix[operation][outcome]);
            }
        }
    }
}

fn append_native_metrics(output: &mut String, native: NativeBufferMetricsSnapshot) {
    for (name, value) in [
        ("crowdb_s3_native_retained_bytes", native.retained_bytes),
        ("crowdb_s3_native_direct_bytes_total", native.direct_bytes),
        ("crowdb_s3_native_prefetched_bytes_total", native.prefetched_bytes),
        (
            "crowdb_s3_native_backpressure_events_total",
            native.backpressure_events,
        ),
        (
            "crowdb_s3_native_backpressure_wait_ns_total",
            native.backpressure_wait_ns,
        ),
    ] {
        append_metric(output, name, "", u64::try_from(value).unwrap_or(u64::MAX));
    }
}

fn append_chunk_metrics(output: &mut String, buffers: LargeWriteBufferMetricsSnapshot) {
    for (name, value) in [
        ("crowdb_s3_large_write_framed_owners_total", buffers.framed_owners),
        ("crowdb_s3_large_write_framed_views_total", buffers.framed_views),
        (
            "crowdb_s3_large_write_payload_copy_operations_total",
            buffers.payload_copy_operations,
        ),
        (
            "crowdb_s3_large_write_payload_copy_bytes_total",
            buffers.payload_copy_bytes,
        ),
    ] {
        append_metric(output, name, "", value);
    }
}

fn append_metric(output: &mut String, name: &str, labels: &str, value: u64) {
    if labels.is_empty() {
        let _ = writeln!(output, "{name} {value}");
    } else {
        let _ = writeln!(output, "{name}{{{labels}}} {value}");
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct S3MetricsSnapshot {
    pub requests: [[u64; OUTCOME_COUNT]; OPERATION_COUNT],
    pub request_latency_ns: [[u64; OUTCOME_COUNT]; OPERATION_COUNT],
    pub time_to_first_byte_ns: [[u64; OUTCOME_COUNT]; OPERATION_COUNT],
    pub authentication_latency_ns: [[u64; OUTCOME_COUNT]; OPERATION_COUNT],
    pub operation_latency_ns: [[u64; OUTCOME_COUNT]; OPERATION_COUNT],
    pub predispatch_requests: [u64; OUTCOME_COUNT],
    pub predispatch_latency_ns: [u64; OUTCOME_COUNT],
    pub request_bytes: u64,
    pub response_bytes: u64,
    pub trusted_auth_bypass: u64,
    pub in_flight: u64,
    pub max_in_flight: u64,
    pub checksum_bytes: u64,
    pub metadata_retries: u64,
    pub cleanup_enqueued: u64,
    pub cleanup_completed: u64,
    pub cleanup_failed: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct S3DataPathMetricsSnapshot {
    pub native: NativeBufferMetricsSnapshot,
    pub large_write_buffers: LargeWriteBufferMetricsSnapshot,
    pub large_write_repairs: LargeWriteRepairMetricsSnapshot,
    pub small_writes: SmallWriteMetricsSnapshot,
    pub checksum_bytes: u64,
    pub metadata_retries: u64,
    pub cleanup_enqueued: u64,
    pub cleanup_completed: u64,
    pub cleanup_failed: u64,
    pub cleanup_backlog: u64,
}

fn snapshot_matrix(
    values: &[[AtomicU64; OUTCOME_COUNT]; OPERATION_COUNT],
) -> [[u64; OUTCOME_COUNT]; OPERATION_COUNT] {
    std::array::from_fn(|operation| {
        std::array::from_fn(|outcome| values[operation][outcome].load(Ordering::Relaxed))
    })
}

pub struct InFlightRequest<'a>(&'a S3Metrics);

impl Drop for InFlightRequest<'_> {
    fn drop(&mut self) {
        self.0.in_flight.fetch_sub(1, Ordering::Relaxed);
    }
}

pub struct S3Health {
    live: AtomicBool,
    listener: AtomicU8,
    metadata: AtomicU8,
    chunks: AtomicU8,
    authentication: AtomicU8,
    cleanup_backlog_limit: u64,
}

impl S3Health {
    #[must_use]
    pub fn starting(cleanup_backlog_limit: u64) -> Self {
        Self {
            live: AtomicBool::new(true),
            listener: AtomicU8::new(DependencyHealth::Unavailable as u8),
            metadata: AtomicU8::new(DependencyHealth::Unavailable as u8),
            chunks: AtomicU8::new(DependencyHealth::Unavailable as u8),
            authentication: AtomicU8::new(DependencyHealth::Unavailable as u8),
            cleanup_backlog_limit,
        }
    }

    #[must_use]
    pub fn ready(cleanup_backlog_limit: u64) -> Self {
        Self {
            live: AtomicBool::new(true),
            listener: AtomicU8::new(DependencyHealth::Ready as u8),
            metadata: AtomicU8::new(DependencyHealth::Ready as u8),
            chunks: AtomicU8::new(DependencyHealth::Ready as u8),
            authentication: AtomicU8::new(DependencyHealth::Ready as u8),
            cleanup_backlog_limit,
        }
    }

    pub fn set_listener(&self, health: DependencyHealth) {
        self.listener.store(health as u8, Ordering::Release);
    }

    pub fn set_metadata(&self, health: DependencyHealth) {
        self.metadata.store(health as u8, Ordering::Release);
    }

    pub fn set_chunks(&self, health: DependencyHealth) {
        self.chunks.store(health as u8, Ordering::Release);
    }

    pub fn set_authentication(&self, health: DependencyHealth) {
        self.authentication.store(health as u8, Ordering::Release);
    }

    pub fn stop(&self) {
        self.live.store(false, Ordering::Release);
    }

    #[must_use]
    pub fn snapshot(&self, metrics: &S3Metrics, native: Option<&NativeBodyAllocator>) -> S3HealthSnapshot {
        let native_metrics = native.map(NativeBodyAllocator::metrics_snapshot);
        let native_pool = native_metrics.map_or(DependencyHealth::Ready, |snapshot| {
            if snapshot.retained_bytes >= snapshot.budget_bytes {
                DependencyHealth::Busy
            } else {
                DependencyHealth::Ready
            }
        });
        let request = metrics.snapshot();
        let cleanup_backlog = request.cleanup_enqueued.saturating_sub(request.cleanup_completed);
        let cleanup = if cleanup_backlog > self.cleanup_backlog_limit {
            DependencyHealth::Unavailable
        } else {
            DependencyHealth::Ready
        };
        S3HealthSnapshot {
            live: self.live.load(Ordering::Acquire),
            readiness: S3Readiness {
                listener: load_health(&self.listener),
                metadata: load_health(&self.metadata),
                chunks: load_health(&self.chunks),
                native_pool,
                cleanup,
                authentication: load_health(&self.authentication),
            },
            native_retained_bytes: native_metrics.map_or(0, |snapshot| snapshot.retained_bytes),
            native_budget_bytes: native_metrics.map_or(0, |snapshot| snapshot.budget_bytes),
            cleanup_backlog,
            cleanup_backlog_limit: self.cleanup_backlog_limit,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct S3HealthSnapshot {
    pub live: bool,
    pub readiness: S3Readiness,
    pub native_retained_bytes: usize,
    pub native_budget_bytes: usize,
    pub cleanup_backlog: u64,
    pub cleanup_backlog_limit: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct S3Readiness {
    pub listener: DependencyHealth,
    pub metadata: DependencyHealth,
    pub chunks: DependencyHealth,
    pub native_pool: DependencyHealth,
    pub cleanup: DependencyHealth,
    pub authentication: DependencyHealth,
}

impl S3Readiness {
    #[must_use]
    pub const fn is_ready(self) -> bool {
        self.listener.can_admit()
            && self.metadata.can_admit()
            && self.chunks.can_admit()
            && self.native_pool.can_admit()
            && self.cleanup.can_admit()
            && self.authentication.can_admit()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum DependencyHealth {
    Ready,
    Busy,
    Unavailable,
}

impl DependencyHealth {
    #[must_use]
    pub const fn can_admit(self) -> bool {
        !matches!(self, Self::Unavailable)
    }

    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Ready => "ready",
            Self::Busy => "busy",
            Self::Unavailable => "unavailable",
        }
    }
}

fn load_health(value: &AtomicU8) -> DependencyHealth {
    match value.load(Ordering::Acquire) {
        value if value == DependencyHealth::Ready as u8 => DependencyHealth::Ready,
        value if value == DependencyHealth::Busy as u8 => DependencyHealth::Busy,
        _ => DependencyHealth::Unavailable,
    }
}
