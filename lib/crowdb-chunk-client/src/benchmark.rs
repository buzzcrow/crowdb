// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Reusable bounded large-write benchmark workload.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use serde::Serialize;
use tokio::io::AsyncReadExt;
use tokio::task::JoinSet;

use crate::{ChunkIoClient, LargeWritePolicy};

/// Deterministic large-write workload parameters.
#[derive(Debug, Clone)]
pub struct LargeWriteBenchmarkConfig {
    pub object_count: u64,
    pub object_size: u64,
    pub concurrency: usize,
    pub seed: u8,
    /// Write sessions whose first chunks are allocated before timing starts.
    pub prepared_write_count: usize,
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
    pub latency_p50_us: u64,
    pub latency_p99_us: u64,
    pub preparation_stalls: u64,
    pub preparation_stall_us: u64,
    pub error_messages: Vec<String>,
}

#[derive(Default)]
struct WorkerResult {
    objects: u64,
    logical_bytes: u64,
    physical_bytes: u64,
    preparation_stalls: u64,
    preparation_stall_us: u64,
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
    let next_object = Arc::new(AtomicU64::new(0));
    let mut tasks = JoinSet::new();
    for mut worker_prepared in prepared {
        let client = client.clone();
        let config = config.clone();
        let next_object = next_object.clone();
        tasks.spawn(async move { run_worker(client, config, next_object, &mut worker_prepared).await });
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
    let accounted = total.objects.saturating_add(total.errors);
    let incomplete_objects = config.object_count.saturating_sub(accounted);
    let stop_reason = if total.errors == 0 && incomplete_objects == 0 {
        "complete"
    } else {
        "failed"
    };
    LargeWriteBenchmarkResult {
        preparation_secs,
        elapsed_secs,
        requested_objects: config.object_count,
        objects: total.objects,
        errors: total.errors,
        incomplete_objects,
        stop_reason: stop_reason.into(),
        logical_bytes: total.logical_bytes,
        physical_bytes: total.physical_bytes,
        logical_mib_per_sec: u64_as_f64(total.logical_bytes) / 1_048_576.0 / elapsed_secs,
        physical_mib_per_sec: u64_as_f64(total.physical_bytes) / 1_048_576.0 / elapsed_secs,
        objects_per_sec: u64_as_f64(total.objects) / elapsed_secs,
        latency_p50_us: percentile(&total.latencies, 50),
        latency_p99_us: percentile(&total.latencies, 99),
        preparation_stalls: total.preparation_stalls,
        preparation_stall_us: total.preparation_stall_us,
        error_messages: total.error_messages,
    }
}

async fn prepare_writes(
    client: &ChunkIoClient,
    config: &LargeWriteBenchmarkConfig,
) -> crate::Result<Vec<VecDeque<crate::PreparedLargeWrite>>> {
    let workers = config.concurrency.max(1);
    let object_count = usize::try_from(config.object_count).unwrap_or(usize::MAX);
    let count = config.prepared_write_count.max(workers).min(object_count);
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
        latency_p50_us: 0,
        latency_p99_us: 0,
        preparation_stalls: 0,
        preparation_stall_us: 0,
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
    next_object: Arc<AtomicU64>,
    prepared: &mut VecDeque<crate::PreparedLargeWrite>,
) -> WorkerResult {
    let mut result = WorkerResult::default();
    loop {
        let object = next_object.fetch_add(1, Ordering::Relaxed);
        if object >= config.object_count {
            break;
        }
        let byte = config.seed.wrapping_add(object as u8);
        let source = tokio::io::repeat(byte).take(config.object_size);
        let started = Instant::now();
        let write = prepared
            .pop_front()
            .unwrap_or_else(|| client.prepare_large_write(Some(config.object_size), config.policy.clone()));
        prepared.push_back(client.prepare_large_write(Some(config.object_size), config.policy.clone()));
        match write.write_stream(source).await {
            Ok(write) => {
                result.objects += 1;
                result.logical_bytes += write.logical_bytes;
                result.physical_bytes += write.physical_bytes;
                result.preparation_stalls += write.preparation_stalls;
                result.preparation_stall_us +=
                    u64::try_from(write.preparation_stall_time.as_micros()).unwrap_or(u64::MAX);
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
    total.latencies.extend(worker.latencies);
    total.errors += worker.errors;
    for message in worker.error_messages {
        record_error(&mut total.error_messages, message);
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
