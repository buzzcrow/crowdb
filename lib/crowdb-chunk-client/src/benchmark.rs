// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Reusable bounded chunk IO benchmark workloads.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use crowdb_common::metrics::perf::DramBwCounter;
use serde::Serialize;
use tokio::io::{AsyncRead, ReadBuf};
use tokio::task::JoinSet;

use crate::{ChunkIoClient, ChunkIoWriter, LargeWritePolicy, ProtoLocation};

/// Deterministic large-write workload parameters.
#[derive(Debug, Clone)]
pub struct LargeWriteBenchmarkConfig {
    pub object_count: u64,
    /// Admission duration. `None` runs until `object_count` is reached.
    pub duration: Option<Duration>,
    pub object_size: u64,
    pub concurrency: usize,
    pub seed: u8,
    /// Write sessions whose first chunks are allocated before timing starts.
    pub prefetch_chunks: usize,
    pub direct_buffers: bool,
    pub policy: LargeWritePolicy,
}

/// Aggregate workload result; application and CLI presentation independent.
#[derive(Debug, Clone, Serialize)]
pub struct LargeWriteBenchmarkResult {
    pub preparation_secs: f64,
    pub elapsed_secs: f64,
    pub requested_objects: u64,
    pub objects: u64,
    pub errors: u64,
    pub incomplete_objects: u64,
    pub stop_reason: String,
    pub logical_bytes: u64,
    pub physical_bytes: u64,
    pub logical_mib_per_sec: f64,
    pub physical_mib_per_sec: f64,
    pub objects_per_sec: f64,
    pub latency_avg_us: u64,
    pub latency_p50_us: u64,
    pub latency_p99_us: u64,
    pub preparation_stalls: u64,
    pub preparation_stall_us: u64,
    pub source_reads: u64,
    pub source_read_us: u64,
    pub assembly_copies: u64,
    pub assembly_copy_bytes: u64,
    pub assembly_copy_us: u64,
    pub ec_encode_us: u64,
    pub completion_wait_us: u64,
    /// Workload-window DRAM read bandwidth in MiB/s, measured from
    /// after prefetch to all writes complete. `None` when the PMU is
    /// unavailable (non-Linux, missing module, insufficient permissions).
    pub dram_read_mib_s: Option<f64>,
    /// Workload-window DRAM write bandwidth in MiB/s. `None` on AMD
    /// (total-only PMU) or when the PMU is unavailable.
    pub dram_write_mib_s: Option<f64>,
    /// Workload-window aggregate DRAM bandwidth in MiB/s (read + write).
    pub dram_total_mib_s: Option<f64>,
    pub error_messages: Vec<String>,
}

/// Deterministic small-write workload parameters.
#[derive(Debug, Clone)]
pub struct SmallWriteBenchmarkConfig {
    pub object_count: u64,
    /// Admission duration. `None` runs until `object_count` is reached.
    pub duration: Option<Duration>,
    pub object_size: usize,
    pub concurrency: usize,
    pub seed: u8,
}

/// Aggregate small-write result including queue-driven pipeline behavior.
#[derive(Debug, Clone, Serialize)]
pub struct SmallWriteBenchmarkResult {
    pub elapsed_secs: f64,
    pub requested_objects: u64,
    pub objects: u64,
    pub errors: u64,
    pub incomplete_objects: u64,
    pub stop_reason: String,
    pub logical_bytes: u64,
    pub logical_mib_per_sec: f64,
    pub objects_per_sec: f64,
    pub latency_avg_us: u64,
    pub latency_p50_us: u64,
    pub latency_p90_us: u64,
    pub latency_p95_us: u64,
    pub latency_p99_us: u64,
    pub latency_max_us: u64,
    pub batches: u64,
    pub max_batch_objects: u64,
    pub max_batch_bytes: u64,
    pub batch_watchdog_expirations: u64,
    pub aggregate_write_requests: u64,
    pub aggregate_write_objects: u64,
    pub aggregate_write_buffers: u64,
    pub aggregate_write_logical_bytes: u64,
    pub aggregate_write_payload_bytes: u64,
    pub max_objects_per_write_request: u64,
    pub max_buffers_per_write_request: u64,
    pub average_batch_fill_ppm: u64,
    pub max_queue_delay_us: u64,
    pub active_pipelines: u64,
    pub max_active_pipelines: u64,
    pub draining_pipelines: u64,
    pub scale_out: u64,
    pub scale_in: u64,
    pub tail_waste_bytes: u64,
    pub foreground_parity_bytes: u64,
    pub reservation_requests: u64,
    pub reservation_wait_us: u64,
    pub first_reservation_us: u64,
    pub dram_read_mib_s: Option<f64>,
    pub dram_write_mib_s: Option<f64>,
    pub dram_total_mib_s: Option<f64>,
    pub error_messages: Vec<String>,
}

/// Object class selected by the read benchmark.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReadBenchmarkWorkload {
    Small,
    Large,
    Mixed,
}

/// Deterministic read workload and real-write preparation parameters.
#[derive(Debug, Clone)]
pub struct ReadBenchmarkConfig {
    pub request_count: u64,
    /// Admission duration. `None` runs until `request_count` is reached.
    pub duration: Option<Duration>,
    /// Number of reusable prepared objects per selected object class.
    pub dataset_objects: usize,
    pub concurrency: usize,
    pub small_object_size: usize,
    pub large_object_size: u64,
    /// Request-count ratio, not a byte ratio.
    pub mixed_large_percent: u8,
    pub seed: u8,
    pub workload: ReadBenchmarkWorkload,
    pub large_policy: LargeWritePolicy,
}

/// Aggregate read result. Preparation is reported separately from timed IO.
#[derive(Debug, Clone, Serialize)]
pub struct ReadBenchmarkResult {
    pub preparation_secs: f64,
    pub elapsed_secs: f64,
    pub requested_reads: u64,
    pub reads: u64,
    pub small_reads: u64,
    pub large_reads: u64,
    pub errors: u64,
    pub incomplete_reads: u64,
    pub stop_reason: String,
    pub logical_bytes: u64,
    pub logical_mib_per_sec: f64,
    pub reads_per_sec: f64,
    pub latency_avg_us: u64,
    pub latency_p50_us: u64,
    pub latency_p99_us: u64,
    pub dram_read_mib_s: Option<f64>,
    pub dram_write_mib_s: Option<f64>,
    pub dram_total_mib_s: Option<f64>,
    pub error_messages: Vec<String>,
}

#[derive(Default)]
struct WorkerResult {
    objects: u64,
    logical_bytes: u64,
    physical_bytes: u64,
    preparation_stalls: u64,
    preparation_stall_us: u64,
    source_reads: u64,
    source_read_us: u64,
    assembly_copies: u64,
    assembly_copy_bytes: u64,
    assembly_copy_us: u64,
    ec_encode_us: u64,
    completion_wait_us: u64,
    latencies: Vec<u64>,
    errors: u64,
    error_messages: Vec<String>,
}

/// Run concurrent deterministic large writes without allocating an
/// object-sized source buffer.
pub async fn run_large_write_benchmark(
    client: ChunkIoClient,
    config: LargeWriteBenchmarkConfig,
) -> LargeWriteBenchmarkResult {
    let preparation_started = Instant::now();
    let prepared = match prepare_writes(&client, &config).await {
        Ok(prepared) => prepared,
        Err(error) => {
            return failed_before_load(&config, format!("prepare writes: {error}"));
        }
    };
    let preparation_secs = preparation_started.elapsed().as_secs_f64();
    let started = Instant::now();
    let deadline = config.duration.map(|duration| started + duration);
    // Open the DRAM BW counter after prefetch so the workload-window
    // measurement excludes prefetch traffic. The first read_bytes_per_sec
    // call returns bandwidth averaged since this point.
    let mut dram_bw = DramBwCounter::new();
    let next_object = Arc::new(AtomicU64::new(0));
    let mut tasks = JoinSet::new();
    for mut worker_prepared in prepared {
        let client = client.clone();
        let config = config.clone();
        let next_object = next_object.clone();
        tasks.spawn(
            async move { run_worker(client, config, deadline, next_object, &mut worker_prepared).await },
        );
    }
    let mut total = WorkerResult::default();
    while let Some(result) = tasks.join_next().await {
        match result {
            Ok(worker) => merge_worker(&mut total, worker),
            Err(error) => {
                total.errors += 1;
                record_error(
                    &mut total.error_messages,
                    format!("benchmark worker failed: {error}"),
                );
            }
        }
    }
    total.latencies.sort_unstable();
    let elapsed_secs = started.elapsed().as_secs_f64().max(f64::EPSILON);
    let requested_objects = next_object.load(Ordering::Relaxed).min(config.object_count);
    let accounted = total.objects.saturating_add(total.errors);
    let incomplete_objects = requested_objects.saturating_sub(accounted);
    let stop_reason = if total.errors == 0 && incomplete_objects == 0 {
        "complete"
    } else {
        "failed"
    };
    // Read the workload-window DRAM BW. This covers exactly from after
    // prefetch (counter creation) to all writes complete (now).
    let to_mib = |v: f64| v / 1024.0 / 1024.0;
    let (dram_read_mib_s, dram_write_mib_s, dram_total_mib_s) = dram_bw
        .as_mut()
        .and_then(DramBwCounter::read_bytes_per_sec)
        .map_or((None, None, None), |(r, w, total)| {
            (r.map(to_mib), w.map(to_mib), Some(to_mib(total)))
        });
    LargeWriteBenchmarkResult {
        preparation_secs,
        elapsed_secs,
        requested_objects,
        objects: total.objects,
        errors: total.errors,
        incomplete_objects,
        stop_reason: stop_reason.into(),
        logical_bytes: total.logical_bytes,
        physical_bytes: total.physical_bytes,
        logical_mib_per_sec: u64_as_f64(total.logical_bytes) / 1_048_576.0 / elapsed_secs,
        physical_mib_per_sec: u64_as_f64(total.physical_bytes) / 1_048_576.0 / elapsed_secs,
        objects_per_sec: u64_as_f64(total.objects) / elapsed_secs,
        latency_avg_us: if total.latencies.is_empty() {
            0
        } else {
            total.latencies.iter().sum::<u64>() / total.latencies.len() as u64
        },
        latency_p50_us: percentile(&total.latencies, 50),
        latency_p99_us: percentile(&total.latencies, 99),
        preparation_stalls: total.preparation_stalls,
        preparation_stall_us: total.preparation_stall_us,
        source_reads: total.source_reads,
        source_read_us: total.source_read_us,
        assembly_copies: total.assembly_copies,
        assembly_copy_bytes: total.assembly_copy_bytes,
        assembly_copy_us: total.assembly_copy_us,
        ec_encode_us: total.ec_encode_us,
        completion_wait_us: total.completion_wait_us,
        dram_read_mib_s,
        dram_write_mib_s,
        dram_total_mib_s,
        error_messages: total.error_messages,
    }
}

async fn prepare_writes(
    client: &ChunkIoClient,
    config: &LargeWriteBenchmarkConfig,
) -> crate::Result<Vec<VecDeque<crate::PreparedLargeWrite>>> {
    let workers = config.concurrency.max(1);
    let object_count = usize::try_from(config.object_count).unwrap_or(usize::MAX);
    let count = config.prefetch_chunks.max(workers).min(object_count);
    let mut queues: Vec<VecDeque<_>> = (0..workers).map(|_| VecDeque::new()).collect();
    let writes = client
        .prepare_large_writes(count, Some(config.object_size), config.policy.clone())
        .await?;
    for (index, write) in writes.into_iter().enumerate() {
        queues[index % workers].push_back(write);
    }
    Ok(queues)
}

fn failed_before_load(config: &LargeWriteBenchmarkConfig, message: String) -> LargeWriteBenchmarkResult {
    LargeWriteBenchmarkResult {
        preparation_secs: 0.0,
        elapsed_secs: 0.0,
        requested_objects: config.object_count,
        objects: 0,
        errors: 1,
        incomplete_objects: config.object_count.saturating_sub(1),
        stop_reason: "failed".into(),
        logical_bytes: 0,
        physical_bytes: 0,
        logical_mib_per_sec: 0.0,
        physical_mib_per_sec: 0.0,
        objects_per_sec: 0.0,
        latency_avg_us: 0,
        latency_p50_us: 0,
        latency_p99_us: 0,
        preparation_stalls: 0,
        preparation_stall_us: 0,
        source_reads: 0,
        source_read_us: 0,
        assembly_copies: 0,
        assembly_copy_bytes: 0,
        assembly_copy_us: 0,
        ec_encode_us: 0,
        completion_wait_us: 0,
        dram_read_mib_s: None,
        dram_write_mib_s: None,
        dram_total_mib_s: None,
        error_messages: vec![message],
    }
}

fn u64_as_f64(value: u64) -> f64 {
    let high = u32::try_from(value >> 32).unwrap_or(u32::MAX);
    let low = u32::try_from(value & u64::from(u32::MAX)).unwrap_or(u32::MAX);
    f64::from(high).mul_add(4_294_967_296.0, f64::from(low))
}

async fn run_worker(
    client: ChunkIoClient,
    config: LargeWriteBenchmarkConfig,
    deadline: Option<Instant>,
    next_object: Arc<AtomicU64>,
    prepared: &mut VecDeque<crate::PreparedLargeWrite>,
) -> WorkerResult {
    let mut result = WorkerResult::default();
    let random_block = bytes::Bytes::from(random_bytes(config.policy.client.read_buffer_size, config.seed));
    loop {
        if deadline.is_some_and(|deadline| Instant::now() >= deadline) {
            break;
        }
        let object = next_object.fetch_add(1, Ordering::Relaxed);
        if object >= config.object_count {
            break;
        }
        let source = RepeatingBufferReader::new(random_block.clone(), config.object_size);
        let started = Instant::now();
        let write = prepared
            .pop_front()
            .unwrap_or_else(|| client.prepare_large_write(Some(config.object_size), config.policy.clone()));
        prepared.push_back(client.prepare_large_write(Some(config.object_size), config.policy.clone()));
        let write_result = if config.direct_buffers {
            let block_bytes = random_block.len() as u64;
            let blocks = config.object_size.div_ceil(block_bytes);
            write
                .write_buffers((0..blocks).map(|index| {
                    let remaining = config.object_size - index * block_bytes;
                    random_block
                        .slice(..usize::try_from(remaining.min(block_bytes)).unwrap_or(random_block.len()))
                }))
                .await
        } else {
            write.write_stream(source).await
        };
        match write_result {
            Ok(write) => {
                result.objects += 1;
                result.logical_bytes += write.logical_bytes;
                result.physical_bytes += write.physical_bytes;
                result.preparation_stalls += write.preparation_stalls;
                result.preparation_stall_us +=
                    u64::try_from(write.preparation_stall_time.as_micros()).unwrap_or(u64::MAX);
                result.source_reads += write.source_reads;
                result.source_read_us += duration_us(write.source_read_time);
                result.assembly_copies += write.assembly_copies;
                result.assembly_copy_bytes += write.assembly_copy_bytes;
                result.assembly_copy_us += duration_us(write.assembly_copy_time);
                result.ec_encode_us += duration_us(write.ec_encode_time);
                result.completion_wait_us += duration_us(write.completion_wait_time);
                result
                    .latencies
                    .push(u64::try_from(started.elapsed().as_micros()).unwrap_or(u64::MAX));
            }
            Err(error) => {
                result.errors += 1;
                record_error(&mut result.error_messages, format!("object {object}: {error}"));
            }
        }
    }
    while let Some(write) = prepared.pop_front() {
        let _ = write.abort().await;
    }
    result
}

fn merge_worker(total: &mut WorkerResult, worker: WorkerResult) {
    total.objects += worker.objects;
    total.logical_bytes += worker.logical_bytes;
    total.physical_bytes += worker.physical_bytes;
    total.preparation_stalls += worker.preparation_stalls;
    total.preparation_stall_us += worker.preparation_stall_us;
    total.source_reads += worker.source_reads;
    total.source_read_us += worker.source_read_us;
    total.assembly_copies += worker.assembly_copies;
    total.assembly_copy_bytes += worker.assembly_copy_bytes;
    total.assembly_copy_us += worker.assembly_copy_us;
    total.ec_encode_us += worker.ec_encode_us;
    total.completion_wait_us += worker.completion_wait_us;
    total.latencies.extend(worker.latencies);
    total.errors += worker.errors;
    for message in worker.error_messages {
        record_error(&mut total.error_messages, message);
    }
}

#[derive(Default)]
struct SmallWorkerResult {
    objects: u64,
    logical_bytes: u64,
    latencies: Vec<u64>,
    errors: u64,
    error_messages: Vec<String>,
}

/// Run concurrent small writes through the client's shared aggregation pool.
pub async fn run_small_write_benchmark(
    client: ChunkIoClient,
    config: SmallWriteBenchmarkConfig,
) -> SmallWriteBenchmarkResult {
    if config.object_count == 0 || config.object_size == 0 || config.concurrency == 0 {
        return failed_small_before_load(&config, "object count, size, and concurrency must be non-zero");
    }
    let started = Instant::now();
    let deadline = config.duration.map(|duration| started + duration);
    let mut dram_bw = DramBwCounter::new();
    let next_object = Arc::new(AtomicU64::new(0));
    let failed = Arc::new(AtomicBool::new(false));
    let payload = bytes::Bytes::from(random_bytes(config.object_size, config.seed));
    let mut tasks = JoinSet::new();
    for _ in 0..config.concurrency.max(1) {
        let client = client.clone();
        let config = config.clone();
        let next_object = Arc::clone(&next_object);
        let payload = payload.clone();
        let failed = Arc::clone(&failed);
        tasks.spawn(run_small_worker(
            client,
            config,
            deadline,
            next_object,
            payload,
            failed,
        ));
    }
    let mut total = SmallWorkerResult::default();
    while let Some(result) = tasks.join_next().await {
        match result {
            Ok(worker) => merge_small_worker(&mut total, worker),
            Err(error) => {
                total.errors += 1;
                record_error(
                    &mut total.error_messages,
                    format!("benchmark worker failed: {error}"),
                );
            }
        }
    }
    let elapsed_secs = started.elapsed().as_secs_f64().max(f64::EPSILON);
    let dram = sample_dram(&mut dram_bw);
    if let Err(error) = client.shutdown_small_writes().await {
        total.errors += 1;
        record_error(&mut total.error_messages, format!("drain small writes: {error}"));
    }
    total.latencies.sort_unstable();
    let requested_objects = next_object.load(Ordering::Relaxed).min(config.object_count);
    let incomplete_objects = requested_objects.saturating_sub(total.objects.saturating_add(total.errors));
    let metrics = client.small_write_metrics();
    finalize_small_result(
        total,
        requested_objects,
        incomplete_objects,
        elapsed_secs,
        &metrics,
        dram,
    )
}

async fn run_small_worker(
    client: ChunkIoClient,
    config: SmallWriteBenchmarkConfig,
    deadline: Option<Instant>,
    next_object: Arc<AtomicU64>,
    payload: bytes::Bytes,
    failed: Arc<AtomicBool>,
) -> SmallWorkerResult {
    let mut result = SmallWorkerResult::default();
    loop {
        if failed.load(Ordering::Acquire) || deadline.is_some_and(|deadline| Instant::now() >= deadline) {
            break;
        }
        let object = next_object.fetch_add(1, Ordering::Relaxed);
        if object >= config.object_count {
            break;
        }
        let operation_started = Instant::now();
        let write = async {
            let mut writer = client.prepare_small_write(config.object_size).await?;
            writer.on_data(payload.clone()).await?;
            writer.on_finish().await
        }
        .await;
        match write {
            Ok(_) => {
                result.objects += 1;
                result.logical_bytes = result.logical_bytes.saturating_add(config.object_size as u64);
                result.latencies.push(duration_us(operation_started.elapsed()));
            }
            Err(error) => {
                failed.store(true, Ordering::Release);
                result.errors += 1;
                record_error(&mut result.error_messages, format!("object {object}: {error}"));
            }
        }
    }
    result
}

fn merge_small_worker(total: &mut SmallWorkerResult, worker: SmallWorkerResult) {
    total.objects += worker.objects;
    total.logical_bytes += worker.logical_bytes;
    total.latencies.extend(worker.latencies);
    total.errors += worker.errors;
    for message in worker.error_messages {
        record_error(&mut total.error_messages, message);
    }
}

fn finalize_small_result(
    total: SmallWorkerResult,
    requested_objects: u64,
    incomplete_objects: u64,
    elapsed_secs: f64,
    metrics: &crate::SmallWriteMetricsSnapshot,
    dram: (Option<f64>, Option<f64>, Option<f64>),
) -> SmallWriteBenchmarkResult {
    SmallWriteBenchmarkResult {
        elapsed_secs,
        requested_objects,
        objects: total.objects,
        errors: total.errors,
        incomplete_objects,
        stop_reason: if total.errors == 0 && incomplete_objects == 0 {
            "complete".into()
        } else {
            "failed".into()
        },
        logical_bytes: total.logical_bytes,
        logical_mib_per_sec: u64_as_f64(total.logical_bytes) / 1_048_576.0 / elapsed_secs,
        objects_per_sec: u64_as_f64(total.objects) / elapsed_secs,
        latency_avg_us: if total.latencies.is_empty() {
            0
        } else {
            total.latencies.iter().sum::<u64>() / total.latencies.len() as u64
        },
        latency_p50_us: percentile(&total.latencies, 50),
        latency_p90_us: percentile(&total.latencies, 90),
        latency_p95_us: percentile(&total.latencies, 95),
        latency_p99_us: percentile(&total.latencies, 99),
        latency_max_us: total.latencies.last().copied().unwrap_or(0),
        batches: metrics.batches,
        max_batch_objects: metrics.max_batch_objects,
        max_batch_bytes: metrics.max_batch_bytes,
        batch_watchdog_expirations: metrics.batch_watchdog_expirations,
        aggregate_write_requests: metrics.aggregate_write_requests,
        aggregate_write_objects: metrics.aggregate_write_objects,
        aggregate_write_buffers: metrics.aggregate_write_buffers,
        aggregate_write_logical_bytes: metrics.aggregate_write_logical_bytes,
        aggregate_write_payload_bytes: metrics.aggregate_write_payload_bytes,
        max_objects_per_write_request: metrics.max_objects_per_write_request,
        max_buffers_per_write_request: metrics.max_buffers_per_write_request,
        average_batch_fill_ppm: metrics.average_batch_fill_ppm,
        max_queue_delay_us: metrics.max_queue_delay_ns / 1_000,
        active_pipelines: metrics.active_pipelines,
        max_active_pipelines: metrics.max_active_pipelines,
        draining_pipelines: metrics.draining_pipelines,
        scale_out: metrics.scale_out,
        scale_in: metrics.scale_in,
        tail_waste_bytes: metrics.tail_waste_bytes,
        foreground_parity_bytes: metrics.foreground_parity_bytes,
        reservation_requests: metrics.reservation_requests,
        reservation_wait_us: metrics.reservation_wait_ns / 1_000,
        first_reservation_us: metrics.first_reservation_ns / 1_000,
        dram_read_mib_s: dram.0,
        dram_write_mib_s: dram.1,
        dram_total_mib_s: dram.2,
        error_messages: total.error_messages,
    }
}

fn failed_small_before_load(config: &SmallWriteBenchmarkConfig, message: &str) -> SmallWriteBenchmarkResult {
    SmallWriteBenchmarkResult {
        elapsed_secs: 0.0,
        requested_objects: config.object_count,
        objects: 0,
        errors: 1,
        incomplete_objects: config.object_count.saturating_sub(1),
        stop_reason: "failed".into(),
        logical_bytes: 0,
        logical_mib_per_sec: 0.0,
        objects_per_sec: 0.0,
        latency_avg_us: 0,
        latency_p50_us: 0,
        latency_p90_us: 0,
        latency_p95_us: 0,
        latency_p99_us: 0,
        latency_max_us: 0,
        batches: 0,
        max_batch_objects: 0,
        max_batch_bytes: 0,
        batch_watchdog_expirations: 0,
        aggregate_write_requests: 0,
        aggregate_write_objects: 0,
        aggregate_write_buffers: 0,
        aggregate_write_logical_bytes: 0,
        aggregate_write_payload_bytes: 0,
        max_objects_per_write_request: 0,
        max_buffers_per_write_request: 0,
        average_batch_fill_ppm: 0,
        max_queue_delay_us: 0,
        active_pipelines: 0,
        max_active_pipelines: 0,
        draining_pipelines: 0,
        scale_out: 0,
        scale_in: 0,
        tail_waste_bytes: 0,
        foreground_parity_bytes: 0,
        reservation_requests: 0,
        reservation_wait_us: 0,
        first_reservation_us: 0,
        dram_read_mib_s: None,
        dram_write_mib_s: None,
        dram_total_mib_s: None,
        error_messages: vec![message.into()],
    }
}

#[derive(Clone)]
struct PreparedReadObject {
    locations: Vec<ProtoLocation>,
    logical_bytes: u64,
}

struct PreparedReadSet {
    small: Arc<Vec<PreparedReadObject>>,
    large: Arc<Vec<PreparedReadObject>>,
}

#[derive(Default)]
struct ReadWorkerResult {
    reads: u64,
    small_reads: u64,
    large_reads: u64,
    logical_bytes: u64,
    latencies: Vec<u64>,
    errors: u64,
    error_messages: Vec<String>,
}

/// Prepare real writer-produced metadata, then benchmark full-object reads.
pub async fn run_read_benchmark(client: ChunkIoClient, config: ReadBenchmarkConfig) -> ReadBenchmarkResult {
    if config.request_count == 0
        || config.dataset_objects == 0
        || config.concurrency == 0
        || config.small_object_size == 0
        || config.large_object_size == 0
        || config.mixed_large_percent > 100
    {
        return failed_read_before_load(&config, "invalid read workload parameters");
    }
    let preparation_started = Instant::now();
    let prepared = match prepare_read_set(&client, &config).await {
        Ok(prepared) => prepared,
        Err(error) => return failed_read_before_load(&config, &error.to_string()),
    };
    let preparation_secs = preparation_started.elapsed().as_secs_f64();
    let started = Instant::now();
    let deadline = config.duration.map(|duration| started + duration);
    let mut dram_bw = DramBwCounter::new();
    let next_read = Arc::new(AtomicU64::new(0));
    let failed = Arc::new(AtomicBool::new(false));
    let mut tasks = JoinSet::new();
    for _ in 0..config.concurrency.max(1) {
        let client = client.clone();
        let config = config.clone();
        let next_read = Arc::clone(&next_read);
        let small = Arc::clone(&prepared.small);
        let large = Arc::clone(&prepared.large);
        let failed = Arc::clone(&failed);
        tasks.spawn(run_read_worker(
            client, config, deadline, next_read, small, large, failed,
        ));
    }
    let mut total = ReadWorkerResult::default();
    while let Some(result) = tasks.join_next().await {
        match result {
            Ok(worker) => merge_read_worker(&mut total, worker),
            Err(error) => {
                total.errors += 1;
                record_error(
                    &mut total.error_messages,
                    format!("benchmark worker failed: {error}"),
                );
            }
        }
    }
    total.latencies.sort_unstable();
    let elapsed_secs = started.elapsed().as_secs_f64().max(f64::EPSILON);
    let requested_reads = next_read.load(Ordering::Relaxed).min(config.request_count);
    let incomplete_reads = requested_reads.saturating_sub(total.reads.saturating_add(total.errors));
    let (dram_read_mib_s, dram_write_mib_s, dram_total_mib_s) = sample_dram(&mut dram_bw);
    finalize_read_result(
        preparation_secs,
        elapsed_secs,
        requested_reads,
        incomplete_reads,
        total,
        (dram_read_mib_s, dram_write_mib_s, dram_total_mib_s),
    )
}

async fn run_read_worker(
    client: ChunkIoClient,
    config: ReadBenchmarkConfig,
    deadline: Option<Instant>,
    next_read: Arc<AtomicU64>,
    small: Arc<Vec<PreparedReadObject>>,
    large: Arc<Vec<PreparedReadObject>>,
    failed: Arc<AtomicBool>,
) -> ReadWorkerResult {
    let mut result = ReadWorkerResult::default();
    loop {
        if failed.load(Ordering::Acquire) || deadline.is_some_and(|deadline| Instant::now() >= deadline) {
            break;
        }
        let request = next_read.fetch_add(1, Ordering::Relaxed);
        if request >= config.request_count {
            break;
        }
        let use_large = select_large_read(&config, request);
        let objects = if use_large { &large } else { &small };
        let index = usize::try_from(request).unwrap_or(usize::MAX) % objects.len();
        let object = &objects[index];
        let operation_started = Instant::now();
        match client.read_object(&object.locations).await {
            Ok(bytes) if bytes.len() as u64 == object.logical_bytes => {
                result.reads += 1;
                result.logical_bytes += object.logical_bytes;
                if use_large {
                    result.large_reads += 1;
                } else {
                    result.small_reads += 1;
                }
                result.latencies.push(duration_us(operation_started.elapsed()));
            }
            Ok(bytes) => {
                failed.store(true, Ordering::Release);
                result.errors += 1;
                record_error(
                    &mut result.error_messages,
                    format!(
                        "read {request}: length {} != {}",
                        bytes.len(),
                        object.logical_bytes
                    ),
                );
            }
            Err(error) => {
                failed.store(true, Ordering::Release);
                result.errors += 1;
                record_error(&mut result.error_messages, format!("read {request}: {error}"));
            }
        }
    }
    result
}

fn select_large_read(config: &ReadBenchmarkConfig, request: u64) -> bool {
    match config.workload {
        ReadBenchmarkWorkload::Small => false,
        ReadBenchmarkWorkload::Large => true,
        ReadBenchmarkWorkload::Mixed => {
            request.wrapping_mul(61).wrapping_add(u64::from(config.seed)) % 100
                < u64::from(config.mixed_large_percent)
        }
    }
}

fn merge_read_worker(total: &mut ReadWorkerResult, worker: ReadWorkerResult) {
    total.reads += worker.reads;
    total.small_reads += worker.small_reads;
    total.large_reads += worker.large_reads;
    total.logical_bytes += worker.logical_bytes;
    total.latencies.extend(worker.latencies);
    total.errors += worker.errors;
    for message in worker.error_messages {
        record_error(&mut total.error_messages, message);
    }
}

fn finalize_read_result(
    preparation_secs: f64,
    elapsed_secs: f64,
    requested_reads: u64,
    incomplete_reads: u64,
    total: ReadWorkerResult,
    dram: (Option<f64>, Option<f64>, Option<f64>),
) -> ReadBenchmarkResult {
    ReadBenchmarkResult {
        preparation_secs,
        elapsed_secs,
        requested_reads,
        reads: total.reads,
        small_reads: total.small_reads,
        large_reads: total.large_reads,
        errors: total.errors,
        incomplete_reads,
        stop_reason: if total.errors == 0 && incomplete_reads == 0 {
            "complete".into()
        } else {
            "failed".into()
        },
        logical_bytes: total.logical_bytes,
        logical_mib_per_sec: u64_as_f64(total.logical_bytes) / 1_048_576.0 / elapsed_secs,
        reads_per_sec: u64_as_f64(total.reads) / elapsed_secs,
        latency_avg_us: if total.latencies.is_empty() {
            0
        } else {
            total.latencies.iter().sum::<u64>() / total.latencies.len() as u64
        },
        latency_p50_us: percentile(&total.latencies, 50),
        latency_p99_us: percentile(&total.latencies, 99),
        dram_read_mib_s: dram.0,
        dram_write_mib_s: dram.1,
        dram_total_mib_s: dram.2,
        error_messages: total.error_messages,
    }
}

async fn prepare_read_set(
    client: &ChunkIoClient,
    config: &ReadBenchmarkConfig,
) -> crate::Result<PreparedReadSet> {
    let need_small = config.workload != ReadBenchmarkWorkload::Large;
    let need_large = config.workload != ReadBenchmarkWorkload::Small;
    let mut small = Vec::with_capacity(if need_small { config.dataset_objects } else { 0 });
    let mut large = Vec::with_capacity(if need_large { config.dataset_objects } else { 0 });
    if need_small {
        let payload = bytes::Bytes::from(random_bytes(config.small_object_size, config.seed));
        for _ in 0..config.dataset_objects {
            let mut writer = client.prepare_small_write(config.small_object_size).await?;
            writer.on_data(payload.clone()).await?;
            small.push(PreparedReadObject {
                locations: writer.on_finish().await?,
                logical_bytes: config.small_object_size as u64,
            });
        }
        client.shutdown_small_writes().await?;
    }
    if need_large {
        let block_size = config.large_policy.client.read_buffer_size;
        let payload = bytes::Bytes::from(random_bytes(block_size, config.seed.wrapping_add(1)));
        for _ in 0..config.dataset_objects {
            let block_bytes = payload.len() as u64;
            let blocks = config.large_object_size.div_ceil(block_bytes);
            let write = client
                .prepare_large_write(Some(config.large_object_size), config.large_policy.clone())
                .write_buffers((0..blocks).map(|index| {
                    let remaining = config.large_object_size - index * block_bytes;
                    payload.slice(..usize::try_from(remaining.min(block_bytes)).unwrap_or(payload.len()))
                }))
                .await?;
            large.push(PreparedReadObject {
                locations: write.locations,
                logical_bytes: config.large_object_size,
            });
        }
    }
    Ok(PreparedReadSet {
        small: Arc::new(small),
        large: Arc::new(large),
    })
}

fn failed_read_before_load(config: &ReadBenchmarkConfig, message: &str) -> ReadBenchmarkResult {
    ReadBenchmarkResult {
        preparation_secs: 0.0,
        elapsed_secs: 0.0,
        requested_reads: config.request_count,
        reads: 0,
        small_reads: 0,
        large_reads: 0,
        errors: 1,
        incomplete_reads: config.request_count.saturating_sub(1),
        stop_reason: "failed".into(),
        logical_bytes: 0,
        logical_mib_per_sec: 0.0,
        reads_per_sec: 0.0,
        latency_avg_us: 0,
        latency_p50_us: 0,
        latency_p99_us: 0,
        dram_read_mib_s: None,
        dram_write_mib_s: None,
        dram_total_mib_s: None,
        error_messages: vec![format!("prepare reads: {message}")],
    }
}

fn sample_dram(counter: &mut Option<DramBwCounter>) -> (Option<f64>, Option<f64>, Option<f64>) {
    let to_mib = |value: f64| value / 1024.0 / 1024.0;
    counter
        .as_mut()
        .and_then(DramBwCounter::read_bytes_per_sec)
        .map_or((None, None, None), |(read, write, total)| {
            (read.map(to_mib), write.map(to_mib), Some(to_mib(total)))
        })
}

fn duration_us(duration: std::time::Duration) -> u64 {
    u64::try_from(duration.as_micros()).unwrap_or(u64::MAX)
}

fn random_bytes(size: usize, seed: u8) -> Vec<u8> {
    let mut state = u64::from(seed).wrapping_add(0x9e37_79b9_7f4a_7c15);
    let mut bytes = vec![0; size];
    for byte in &mut bytes {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        *byte = state as u8;
    }
    bytes
}

struct RepeatingBufferReader {
    block: bytes::Bytes,
    remaining: u64,
    offset: usize,
}

impl RepeatingBufferReader {
    fn new(block: bytes::Bytes, remaining: u64) -> Self {
        Self {
            block,
            remaining,
            offset: 0,
        }
    }
}

impl AsyncRead for RepeatingBufferReader {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
        output: &mut ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        if self.remaining == 0 || self.block.is_empty() {
            return std::task::Poll::Ready(Ok(()));
        }
        let count = output
            .remaining()
            .min(self.block.len() - self.offset)
            .min(usize::try_from(self.remaining).unwrap_or(usize::MAX));
        output.put_slice(&self.block[self.offset..self.offset + count]);
        self.remaining -= count as u64;
        self.offset = (self.offset + count) % self.block.len();
        std::task::Poll::Ready(Ok(()))
    }
}

fn record_error(messages: &mut Vec<String>, message: String) {
    const MAX_ERROR_MESSAGES: usize = 16;
    if messages.len() < MAX_ERROR_MESSAGES {
        messages.push(message);
    }
}

fn percentile(sorted: &[u64], percentile: usize) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let index = (sorted.len() - 1) * percentile / 100;
    sorted[index]
}
