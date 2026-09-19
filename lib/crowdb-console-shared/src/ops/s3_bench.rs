// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Bounded S3 workloads over an invocation-owned memory-backed mini-cluster.

use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures::stream::{self, StreamExt};
use reqwest::Method;
use serde::Serialize;

use crate::error::{Error, Result};
use crate::ops::s3::{self, S3HttpClient};

const BENCH_BUCKET: &str = "crowdb-memory-bench";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum S3BenchWorkload {
    Write,
    Read,
    RangeRead,
    List,
    Mix,
}

#[derive(Debug, Clone)]
pub struct S3BenchConfig {
    pub work_dir: PathBuf,
    pub workload: S3BenchWorkload,
    pub object_size: usize,
    pub dataset_objects: usize,
    pub concurrency: usize,
    pub operations: u64,
    pub duration: Duration,
    pub warmup_operations: u64,
    pub seed: u64,
    pub memory_budget_bytes: u64,
    pub list_limit: usize,
    pub mix: MixWeights,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MixWeights {
    weights: [u64; 4],
    total: u64,
}

impl Default for MixWeights {
    fn default() -> Self {
        Self {
            weights: [20, 70, 5, 5],
            total: 100,
        }
    }
}

impl MixWeights {
    /// Parse compact case-insensitive weights such as `w20R70RR5L5`.
    ///
    /// # Errors
    /// Rejects missing, duplicate, zero, unknown, and overflowing terms.
    pub fn parse(value: &str) -> Result<Self> {
        let bytes = value.as_bytes();
        let mut weights = [0_u64; 4];
        let mut seen = [false; 4];
        let mut cursor = 0;
        while cursor < bytes.len() {
            let op = match bytes[cursor].to_ascii_uppercase() {
                b'W' => 0,
                b'R' if bytes
                    .get(cursor + 1)
                    .is_some_and(|b| b.eq_ignore_ascii_case(&b'R')) =>
                {
                    cursor += 1;
                    2
                }
                b'R' => 1,
                b'L' => 3,
                _ => return Err(validation("mix", "unknown operation; use W, R, RR, or L")),
            };
            if seen[op] {
                return Err(validation("mix", "duplicate operation"));
            }
            seen[op] = true;
            cursor += 1;
            let start = cursor;
            while bytes.get(cursor).is_some_and(u8::is_ascii_digit) {
                cursor += 1;
            }
            if start == cursor {
                return Err(validation("mix", "each operation needs a positive weight"));
            }
            let text = std::str::from_utf8(&bytes[start..cursor])
                .map_err(|_| validation("mix", "weight is not UTF-8"))?;
            let weight = text
                .parse::<u64>()
                .map_err(|_| validation("mix", "weight overflow"))?;
            if weight == 0 {
                return Err(validation("mix", "weights must be positive"));
            }
            weights[op] = weight;
        }
        let total = weights
            .iter()
            .try_fold(0_u64, |sum, weight| sum.checked_add(*weight))
            .ok_or_else(|| validation("mix", "total weight overflow"))?;
        if total == 0 {
            return Err(validation("mix", "at least one operation is required"));
        }
        Ok(Self { weights, total })
    }

    #[must_use]
    pub fn select_name(self, random: u64) -> &'static str {
        workload_name(self.select(random))
    }

    fn select(self, random: u64) -> S3BenchWorkload {
        let mut selected = random % self.total;
        for (index, weight) in self.weights.into_iter().enumerate() {
            if selected < weight {
                return match index {
                    0 => S3BenchWorkload::Write,
                    1 => S3BenchWorkload::Read,
                    2 => S3BenchWorkload::RangeRead,
                    _ => S3BenchWorkload::List,
                };
            }
            selected -= weight;
        }
        S3BenchWorkload::List
    }
}

#[derive(Debug, Serialize)]
pub struct S3BenchResult {
    pub workload: &'static str,
    pub duration_ms: u64,
    pub total_operations: u64,
    pub total_errors: u64,
    pub operations_per_second: u64,
    pub warmup_operations: u64,
    pub object_size: usize,
    pub dataset_objects: usize,
    pub memory_budget_bytes: u64,
    pub estimated_dataset_bytes: u64,
    pub peak_resident_bytes: u64,
    pub backing: S3BenchBacking,
    pub failures: S3BenchFailures,
    pub by_operation: S3BenchOperations,
    pub object_bytes_verified: bool,
}

#[derive(Debug, Serialize)]
pub struct S3BenchBacking {
    pub kv: &'static str,
    pub wal: &'static str,
    pub diskio: &'static str,
    pub chunk_kv: &'static str,
}

#[derive(Debug, Default, Serialize)]
pub struct S3BenchFailures {
    pub metadata: u64,
    pub protocol: u64,
    pub transport: u64,
    pub resource: u64,
}

#[derive(Debug, Default, Serialize)]
pub struct S3BenchOperations {
    pub write: Option<S3BenchOperationStats>,
    pub read: Option<S3BenchOperationStats>,
    pub range_read: Option<S3BenchOperationStats>,
    pub list: Option<S3BenchOperationStats>,
}

#[derive(Debug, Serialize)]
pub struct S3BenchOperationStats {
    pub attempts: u64,
    pub successes: u64,
    pub failures: u64,
    pub operations_per_second: u64,
    pub average_us: u64,
    pub p50_us: u64,
    pub p99_us: u64,
}

#[derive(Default)]
struct WorkerStats {
    operations: [OperationAccumulator; 4],
    failures: S3BenchFailures,
    written_operations: Vec<u64>,
}

#[derive(Default)]
struct OperationAccumulator {
    attempts: u64,
    successes: u64,
    latencies_us: Vec<u64>,
}

/// Start an invocation-owned memory cluster, run one workload, and stop it.
///
/// # Errors
/// Returns validation, cluster startup, request-path, or cleanup errors.
pub async fn run(config: S3BenchConfig) -> Result<S3BenchResult> {
    validate_config(&config)?;
    let estimated_dataset_bytes = u64::try_from(config.object_size)
        .ok()
        .and_then(|size| size.checked_mul(u64::try_from(config.dataset_objects).ok()?))
        .ok_or_else(|| validation("dataset", "dataset size overflow"))?;
    if estimated_dataset_bytes > config.memory_budget_bytes / 2 {
        return Err(validation(
            "memory_budget_bytes",
            "prepared dataset exceeds half of the memory budget",
        ));
    }
    s3::start_memory(&config.work_dir, config.memory_budget_bytes).await?;
    let outcome = run_attached(&config, estimated_dataset_bytes).await;
    let stop_result = s3::stop(&config.work_dir);
    let result = outcome?;
    stop_result?;
    Ok(result)
}

async fn run_attached(config: &S3BenchConfig, estimated_dataset_bytes: u64) -> Result<S3BenchResult> {
    let client = S3HttpClient::from_data_dir(&config.work_dir)?;
    client
        .request(Method::PUT, Some(BENCH_BUCKET), None, &[], None, None)
        .await?;
    prepare_dataset(&client, config).await?;
    let warmup_writes = run_warmup(&client, config).await?;

    let sampling = Arc::new(AtomicBool::new(true));
    let peak_resident = Arc::new(AtomicU64::new(cluster_resident_bytes(&config.work_dir)));
    let sampler = tokio::spawn(sample_cluster_resident(
        config.work_dir.clone(),
        Arc::clone(&sampling),
        Arc::clone(&peak_resident),
    ));
    let admitted = Arc::new(AtomicU64::new(0));
    let deadline = Instant::now() + config.duration;
    let start = Instant::now();
    let mut tasks = Vec::with_capacity(config.concurrency);
    for worker in 0..config.concurrency {
        let client = client.clone();
        let admitted = Arc::clone(&admitted);
        let config = config.clone();
        tasks.push(tokio::spawn(async move {
            run_worker(client, config, admitted, deadline, worker).await
        }));
    }
    let mut combined = WorkerStats::default();
    for task in tasks {
        merge_stats(
            &mut combined,
            task.await.map_err(|error| Error::Config(error.to_string()))?,
        );
    }
    let duration_ms = elapsed_ms(start);
    sampling.store(false, Ordering::Release);
    sampler.await.map_err(|error| Error::Config(error.to_string()))?;
    let peak_resident_bytes = peak_resident.load(Ordering::Acquire);
    cleanup(&client, &combined.written_operations, &warmup_writes, config).await?;
    if peak_resident_bytes > config.memory_budget_bytes {
        return Err(validation(
            "memory_budget_bytes",
            &format!(
                "cluster peak resident bytes {peak_resident_bytes} exceeded budget {}",
                config.memory_budget_bytes
            ),
        ));
    }
    Ok(build_result(
        config,
        combined,
        duration_ms,
        estimated_dataset_bytes,
        peak_resident_bytes,
    ))
}

fn validate_config(config: &S3BenchConfig) -> Result<()> {
    if config.object_size == 0 || config.dataset_objects == 0 || config.concurrency == 0 {
        return Err(validation(
            "benchmark",
            "object size, dataset objects, and concurrency must be non-zero",
        ));
    }
    if config.operations == 0 && config.duration.is_zero() {
        return Err(validation("benchmark", "operations or duration must be non-zero"));
    }
    if config.list_limit == 0 {
        return Err(validation("list_limit", "must be non-zero"));
    }
    Ok(())
}

async fn prepare_dataset(client: &S3HttpClient, config: &S3BenchConfig) -> Result<()> {
    if config.workload == S3BenchWorkload::Write {
        return Ok(());
    }
    let body = deterministic_body(config.object_size, config.seed);
    for index in 0..config.dataset_objects {
        client
            .request(
                Method::PUT,
                Some(BENCH_BUCKET),
                Some(&dataset_key(index)),
                &[],
                Some(body.clone()),
                None,
            )
            .await?;
    }
    Ok(())
}

async fn run_warmup(client: &S3HttpClient, config: &S3BenchConfig) -> Result<Vec<u64>> {
    let mut written_operations = Vec::new();
    for index in 0..config.warmup_operations {
        let workload = selected_workload(config.workload, config.mix, config.seed ^ index);
        execute(client, config, workload, index, true).await?;
        if workload == S3BenchWorkload::Write {
            written_operations.push(index);
        }
    }
    Ok(written_operations)
}

async fn run_worker(
    client: S3HttpClient,
    config: S3BenchConfig,
    admitted: Arc<AtomicU64>,
    deadline: Instant,
    worker: usize,
) -> WorkerStats {
    let mut stats = WorkerStats::default();
    let mut random = config.seed ^ u64::try_from(worker + 1).unwrap_or(u64::MAX);
    while Instant::now() < deadline {
        let operation = admitted.fetch_add(1, Ordering::Relaxed);
        if operation >= config.operations {
            break;
        }
        random = xorshift(random);
        let workload = selected_workload(config.workload, config.mix, random);
        if workload == S3BenchWorkload::Write {
            stats.written_operations.push(operation);
        }
        let index = operation_index(workload);
        stats.operations[index].attempts += 1;
        let started = Instant::now();
        match execute(&client, &config, workload, operation, false).await {
            Ok(()) => {
                stats.operations[index].successes += 1;
                stats.operations[index]
                    .latencies_us
                    .push(u64::try_from(started.elapsed().as_micros()).unwrap_or(u64::MAX));
            }
            Err(error) => classify_failure(&mut stats.failures, &error),
        }
    }
    stats
}

async fn execute(
    client: &S3HttpClient,
    config: &S3BenchConfig,
    workload: S3BenchWorkload,
    operation: u64,
    warmup: bool,
) -> Result<()> {
    match workload {
        S3BenchWorkload::Write => {
            let key = if warmup {
                format!("warmup-{operation:020}")
            } else {
                format!("write-{operation:020}")
            };
            client
                .request(
                    Method::PUT,
                    Some(BENCH_BUCKET),
                    Some(&key),
                    &[],
                    Some(deterministic_body(config.object_size, config.seed ^ operation)),
                    None,
                )
                .await?;
        }
        S3BenchWorkload::Read => {
            let key = dataset_key(dataset_index(operation, config.dataset_objects));
            let (_, body) = client
                .request(Method::GET, Some(BENCH_BUCKET), Some(&key), &[], None, None)
                .await?;
            if body.len() != config.object_size {
                return Err(validation("read", "response length differs from prepared object"));
            }
        }
        S3BenchWorkload::RangeRead => {
            let key = dataset_key(dataset_index(operation, config.dataset_objects));
            let start = u64::try_from(config.object_size / 4).unwrap_or(0);
            let length = (config.object_size / 2).max(1);
            let end = start + u64::try_from(length - 1).unwrap_or(0);
            let (_, body) = client
                .request(
                    Method::GET,
                    Some(BENCH_BUCKET),
                    Some(&key),
                    &[],
                    None,
                    Some((start, end)),
                )
                .await?;
            if body.len() != length {
                return Err(validation(
                    "range_read",
                    "response length differs from requested range",
                ));
            }
        }
        S3BenchWorkload::List => {
            let query = [
                ("list-type", "2".to_string()),
                ("prefix", "dataset-".to_string()),
                ("max-keys", config.list_limit.to_string()),
            ];
            let (_, body) = client
                .request(Method::GET, Some(BENCH_BUCKET), None, &query, None, None)
                .await?;
            validate_list(&body)?;
        }
        S3BenchWorkload::Mix => unreachable!("mix is selected before execution"),
    }
    Ok(())
}

async fn cleanup(
    client: &S3HttpClient,
    written_operations: &[u64],
    warmup_writes: &[u64],
    config: &S3BenchConfig,
) -> Result<()> {
    let mut keys =
        Vec::with_capacity(config.dataset_objects + written_operations.len() + warmup_writes.len());
    keys.extend((0..config.dataset_objects).map(dataset_key));
    keys.extend(
        written_operations
            .iter()
            .map(|operation| format!("write-{operation:020}")),
    );
    keys.extend(
        warmup_writes
            .iter()
            .map(|operation| format!("warmup-{operation:020}")),
    );
    let results = stream::iter(keys.into_iter().map(|key| {
        let client = client.clone();
        async move { delete_key(&client, &key).await }
    }))
    .buffer_unordered(config.concurrency)
    .collect::<Vec<_>>()
    .await;
    for result in results {
        result?;
    }
    client
        .request(Method::DELETE, Some(BENCH_BUCKET), None, &[], None, None)
        .await?;
    Ok(())
}

async fn delete_key(client: &S3HttpClient, key: &str) -> Result<()> {
    client
        .request(Method::DELETE, Some(BENCH_BUCKET), Some(key), &[], None, None)
        .await?;
    Ok(())
}

fn validate_list(body: &[u8]) -> Result<()> {
    let xml = std::str::from_utf8(body).map_err(|_| validation("list", "response is not UTF-8 XML"))?;
    let keys = xml_values(xml, "Key");
    if keys.windows(2).any(|pair| pair[0] >= pair[1]) {
        return Err(validation("list", "keys are not strictly ordered"));
    }
    if xml.contains("<IsTruncated>true</IsTruncated>") && xml_values(xml, "NextContinuationToken").is_empty()
    {
        return Err(validation("list", "truncated page has no continuation token"));
    }
    Ok(())
}

fn xml_values<'a>(xml: &'a str, tag: &str) -> Vec<&'a str> {
    let open = format!("<{tag}>");
    let close = format!("</{tag}>");
    let mut values = Vec::new();
    let mut remaining = xml;
    while let Some(start) = remaining.find(&open) {
        remaining = &remaining[start + open.len()..];
        let Some(end) = remaining.find(&close) else { break };
        values.push(&remaining[..end]);
        remaining = &remaining[end + close.len()..];
    }
    values
}

fn build_result(
    config: &S3BenchConfig,
    mut stats: WorkerStats,
    duration_ms: u64,
    estimated_dataset_bytes: u64,
    peak_resident_bytes: u64,
) -> S3BenchResult {
    let total_operations = stats.operations.iter().map(|op| op.attempts).sum();
    let total_errors = stats.operations.iter().map(|op| op.attempts - op.successes).sum();
    let elapsed_ms = duration_ms.max(1);
    let mut by_operation = S3BenchOperations::default();
    for (index, accumulator) in stats.operations.iter_mut().enumerate() {
        if accumulator.attempts == 0 {
            continue;
        }
        let value = operation_stats(accumulator, elapsed_ms);
        match index {
            0 => by_operation.write = Some(value),
            1 => by_operation.read = Some(value),
            2 => by_operation.range_read = Some(value),
            _ => by_operation.list = Some(value),
        }
    }
    S3BenchResult {
        workload: workload_name(config.workload),
        duration_ms,
        total_operations,
        total_errors,
        operations_per_second: total_operations.saturating_mul(1000) / elapsed_ms,
        warmup_operations: config.warmup_operations,
        object_size: config.object_size,
        dataset_objects: config.dataset_objects,
        memory_budget_bytes: config.memory_budget_bytes,
        estimated_dataset_bytes,
        peak_resident_bytes,
        backing: S3BenchBacking {
            kv: "mem-block",
            wal: "mem-block",
            diskio: "mem",
            chunk_kv: "mem",
        },
        failures: stats.failures,
        by_operation,
        object_bytes_verified: false,
    }
}

fn operation_stats(accumulator: &mut OperationAccumulator, duration_ms: u64) -> S3BenchOperationStats {
    accumulator.latencies_us.sort_unstable();
    let average = if accumulator.latencies_us.is_empty() {
        0
    } else {
        accumulator.latencies_us.iter().sum::<u64>()
            / u64::try_from(accumulator.latencies_us.len()).unwrap_or(u64::MAX)
    };
    S3BenchOperationStats {
        attempts: accumulator.attempts,
        successes: accumulator.successes,
        failures: accumulator.attempts - accumulator.successes,
        operations_per_second: accumulator.successes.saturating_mul(1000) / duration_ms.max(1),
        average_us: average,
        p50_us: percentile(&accumulator.latencies_us, 50),
        p99_us: percentile(&accumulator.latencies_us, 99),
    }
}

fn merge_stats(combined: &mut WorkerStats, mut worker: WorkerStats) {
    for (target, source) in combined.operations.iter_mut().zip(worker.operations.iter_mut()) {
        target.attempts += source.attempts;
        target.successes += source.successes;
        target.latencies_us.append(&mut source.latencies_us);
    }
    combined.failures.metadata += worker.failures.metadata;
    combined.failures.protocol += worker.failures.protocol;
    combined.failures.transport += worker.failures.transport;
    combined.failures.resource += worker.failures.resource;
    combined.written_operations.append(&mut worker.written_operations);
}

fn classify_failure(failures: &mut S3BenchFailures, error: &Error) {
    match error {
        Error::Validation { .. } => failures.protocol += 1,
        Error::UpstreamRpc { status, .. } if status.starts_with("HTTP ") => failures.metadata += 1,
        Error::Io(_) => failures.resource += 1,
        _ => failures.transport += 1,
    }
}

fn selected_workload(workload: S3BenchWorkload, mix: MixWeights, random: u64) -> S3BenchWorkload {
    if workload == S3BenchWorkload::Mix {
        mix.select(random)
    } else {
        workload
    }
}

fn operation_index(workload: S3BenchWorkload) -> usize {
    match workload {
        S3BenchWorkload::Write => 0,
        S3BenchWorkload::Read => 1,
        S3BenchWorkload::RangeRead => 2,
        S3BenchWorkload::List => 3,
        S3BenchWorkload::Mix => unreachable!("mix is selected before accounting"),
    }
}

fn workload_name(workload: S3BenchWorkload) -> &'static str {
    match workload {
        S3BenchWorkload::Write => "write",
        S3BenchWorkload::Read => "read",
        S3BenchWorkload::RangeRead => "range-read",
        S3BenchWorkload::List => "list",
        S3BenchWorkload::Mix => "mix",
    }
}

fn deterministic_body(size: usize, seed: u64) -> Vec<u8> {
    (0..size)
        .map(|index| {
            u8::try_from(seed.wrapping_add(u64::try_from(index).unwrap_or(u64::MAX)) & 0xff).unwrap_or(0)
        })
        .collect()
}

fn dataset_key(index: usize) -> String {
    format!("dataset-{index:020}")
}

fn dataset_index(operation: u64, count: usize) -> usize {
    usize::try_from(operation % u64::try_from(count).unwrap_or(u64::MAX)).unwrap_or(0)
}

fn xorshift(mut value: u64) -> u64 {
    if value == 0 {
        value = 0x9E37_79B9_7F4A_7C15;
    }
    value ^= value << 13;
    value ^= value >> 7;
    value ^ (value << 17)
}

fn percentile(sorted: &[u64], percentile: usize) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    sorted[(sorted.len() - 1) * percentile / 100]
}

fn elapsed_ms(start: Instant) -> u64 {
    u64::try_from(start.elapsed().as_millis()).unwrap_or(u64::MAX)
}

fn cluster_resident_bytes(data_dir: &std::path::Path) -> u64 {
    s3::load(data_dir).map_or(0, |(config, _)| {
        config
            .servers
            .iter()
            .filter_map(|server| server.pid)
            .filter_map(process_resident_bytes)
            .sum()
    })
}

async fn sample_cluster_resident(data_dir: PathBuf, sampling: Arc<AtomicBool>, peak: Arc<AtomicU64>) {
    while sampling.load(Ordering::Acquire) {
        peak.fetch_max(cluster_resident_bytes(&data_dir), Ordering::AcqRel);
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    peak.fetch_max(cluster_resident_bytes(&data_dir), Ordering::AcqRel);
}

fn process_resident_bytes(pid: u32) -> Option<u64> {
    let status = std::fs::read_to_string(format!("/proc/{pid}/status")).ok()?;
    let line = status.lines().find(|line| line.starts_with("VmRSS:"))?;
    let kib = line.split_ascii_whitespace().nth(1)?.parse::<u64>().ok()?;
    kib.checked_mul(1024)
}

fn validation(field: &str, message: &str) -> Error {
    Error::Validation {
        field: field.into(),
        message: message.into(),
    }
}
