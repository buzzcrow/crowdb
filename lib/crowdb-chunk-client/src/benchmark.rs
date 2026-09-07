// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Reusable bounded large-write benchmark workload.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use serde::Serialize;
use tokio::io::{AsyncRead, ReadBuf};
use tokio::task::JoinSet;

use crate::{ChunkIoClient, LargeWritePolicy};

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
