// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use crowdb_chunk_client::STREAM_CHUNK_BYTES;
use crowdb_chunk_stream::{
    ChunkStream, ProductionStreamRuntime, ReadHint, Result, StreamBinding, StreamBindingState, StreamName,
    StreamRegistry,
};

use super::config::{BenchConfig, Workload};
use super::result::{BenchResult, TaskResult};

pub async fn run(runtime: &ProductionStreamRuntime, config: &BenchConfig) -> Result<BenchResult> {
    let stream = create_stream(runtime).await?;
    if config.workload != Workload::Append {
        prepare(&stream, config.dataset_bytes).await?;
    }
    let rss_start = rss_kib();
    let sampling = Arc::new(AtomicBool::new(true));
    let peak = Arc::new(AtomicU64::new(rss_start));
    let sampler = tokio::spawn(sample_rss(Arc::clone(&sampling), Arc::clone(&peak)));
    let started = Instant::now();
    let task = match config.workload {
        Workload::Append => append(Arc::new(stream.clone()), config).await,
        Workload::RandomRead => random_read(Arc::new(stream.clone()), config).await,
        Workload::Replay => replay(Arc::new(stream.clone()), config).await,
        Workload::Gc => gc(&stream).await,
    };
    let elapsed = started.elapsed();
    sampling.store(false, Ordering::Release);
    let _ = sampler.await;
    let rss_end = rss_kib();
    Ok(BenchResult::from_task(
        config.workload,
        elapsed,
        rss_start,
        rss_end,
        peak.load(Ordering::Acquire).max(rss_end),
        stream.metrics(),
        task,
    ))
}

async fn create_stream(runtime: &ProductionStreamRuntime) -> Result<ChunkStream> {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let stream_name = StreamName {
        high: u64::try_from(now >> 64).unwrap_or_default(),
        low: u64::try_from(now & u128::from(u64::MAX)).unwrap_or_default() ^ u64::from(std::process::id()),
    };
    runtime
        .registry()
        .create(StreamBinding {
            stream_name,
            metadata_group_id: 1,
            binding_generation: 1,
            state: StreamBindingState::Active,
            owner_kind: Some("production-benchmark".into()),
        })
        .await?;
    runtime.create_registered(stream_name, 0, 1).await
}

async fn prepare(stream: &ChunkStream, bytes: u64) -> Result<()> {
    const PREPARE_BYTES: usize = 4 * 1024 * 1024;
    let buffer = Bytes::from(vec![0x5a; PREPARE_BYTES]);
    let mut written = 0_u64;
    while written < bytes {
        let length = usize::try_from((bytes - written).min(PREPARE_BYTES as u64)).unwrap_or(PREPARE_BYTES);
        stream.append(&[buffer.slice(..length)]).await?;
        written += length as u64;
    }
    Ok(())
}

async fn append(stream: Arc<ChunkStream>, config: &BenchConfig) -> TaskResult {
    let deadline = Instant::now() + Duration::from_secs(config.duration_secs);
    let admitted = Arc::new(AtomicU64::new(0));
    let payload = Bytes::from(vec![0x6b; config.object_size]);
    let mut tasks = Vec::with_capacity(config.concurrency);
    for _ in 0..config.concurrency {
        tasks.push(tokio::spawn(append_task(
            Arc::clone(&stream),
            Arc::clone(&admitted),
            payload.clone(),
            config.operations,
            deadline,
        )));
    }
    collect(tasks).await
}

async fn append_task(
    stream: Arc<ChunkStream>,
    admitted: Arc<AtomicU64>,
    payload: Bytes,
    limit: u64,
    deadline: Instant,
) -> TaskResult {
    let mut result = TaskResult::default();
    while Instant::now() < deadline && admitted.fetch_add(1, Ordering::Relaxed) < limit {
        let started = Instant::now();
        match stream.append(std::slice::from_ref(&payload)).await {
            Ok(range) => {
                result.operations += 1;
                result.bytes += range.end - range.begin;
                record_latency(&mut result, started);
            }
            Err(error) => {
                eprintln!("append failed: {error}");
                result.errors += 1;
                break;
            }
        }
    }
    result
}

async fn random_read(stream: Arc<ChunkStream>, config: &BenchConfig) -> TaskResult {
    if config.object_size as u64 > config.dataset_bytes {
        return TaskResult {
            errors: 1,
            ..TaskResult::default()
        };
    }
    let deadline = Instant::now() + Duration::from_secs(config.duration_secs);
    let admitted = Arc::new(AtomicU64::new(0));
    let mut tasks = Vec::with_capacity(config.concurrency);
    for task_id in 0..config.concurrency {
        tasks.push(tokio::spawn(random_read_task(
            Arc::clone(&stream),
            Arc::clone(&admitted),
            config.clone(),
            task_id as u64 + 1,
            deadline,
        )));
    }
    collect(tasks).await
}

async fn random_read_task(
    stream: Arc<ChunkStream>,
    admitted: Arc<AtomicU64>,
    config: BenchConfig,
    mut random: u64,
    deadline: Instant,
) -> TaskResult {
    let mut result = TaskResult::default();
    let span = config.dataset_bytes - config.object_size as u64 + 1;
    while Instant::now() < deadline && admitted.fetch_add(1, Ordering::Relaxed) < config.operations {
        random = xorshift(random);
        let started = Instant::now();
        match stream.read_at(random % span, config.object_size).await {
            Ok(bytes) if bytes.len() == config.object_size => {
                result.operations += 1;
                result.bytes += bytes.len() as u64;
                record_latency(&mut result, started);
            }
            Ok(_) => {
                result.errors += 1;
                break;
            }
            Err(error) => {
                eprintln!("random read failed: {error}");
                result.errors += 1;
                break;
            }
        }
    }
    result
}

async fn replay(stream: Arc<ChunkStream>, config: &BenchConfig) -> TaskResult {
    let deadline = Instant::now() + Duration::from_secs(config.duration_secs);
    let admitted = Arc::new(AtomicU64::new(0));
    let mut tasks = Vec::with_capacity(config.concurrency);
    for _ in 0..config.concurrency {
        tasks.push(tokio::spawn(replay_task(
            Arc::clone(&stream),
            Arc::clone(&admitted),
            config.operations,
            deadline,
        )));
    }
    collect(tasks).await
}

async fn replay_task(
    stream: Arc<ChunkStream>,
    admitted: Arc<AtomicU64>,
    limit: u64,
    deadline: Instant,
) -> TaskResult {
    let mut result = TaskResult::default();
    while Instant::now() < deadline && admitted.fetch_add(1, Ordering::Relaxed) < limit {
        let started = Instant::now();
        let mut reader = match stream.reader(0, ReadHint::ToEnd) {
            Ok(reader) => reader,
            Err(error) => {
                eprintln!("open replay reader failed: {error}");
                result.errors += 1;
                break;
            }
        };
        let mut bytes = 0_u64;
        loop {
            match reader.next().await {
                Ok(Some(window)) => bytes += window.len() as u64,
                Ok(None) => break,
                Err(error) => {
                    eprintln!("replay failed: {error}");
                    result.errors += 1;
                    return result;
                }
            }
        }
        result.operations += 1;
        result.bytes += bytes;
        record_latency(&mut result, started);
    }
    result
}

async fn gc(stream: &ChunkStream) -> TaskResult {
    let trim_offset = stream.tail() / STREAM_CHUNK_BYTES * STREAM_CHUNK_BYTES;
    if trim_offset == 0 || trim_offset == stream.tail() {
        return TaskResult {
            errors: 1,
            ..TaskResult::default()
        };
    }
    let started = Instant::now();
    match stream.trim_prefix(trim_offset).await {
        Ok(bytes) => TaskResult {
            operations: 1,
            bytes,
            latencies_us: vec![elapsed_us(started)],
            ..TaskResult::default()
        },
        Err(error) => {
            eprintln!("stream GC failed: {error}");
            TaskResult {
                errors: 1,
                ..TaskResult::default()
            }
        }
    }
}

async fn collect(tasks: Vec<tokio::task::JoinHandle<TaskResult>>) -> TaskResult {
    let mut total = TaskResult::default();
    for task in tasks {
        match task.await {
            Ok(result) => total.merge(result),
            Err(error) => {
                eprintln!("benchmark task failed: {error}");
                total.errors += 1;
            }
        }
    }
    total
}

fn record_latency(result: &mut TaskResult, started: Instant) {
    result.latencies_us.push(elapsed_us(started));
}

fn elapsed_us(started: Instant) -> u64 {
    started.elapsed().as_micros().try_into().unwrap_or(u64::MAX)
}

fn xorshift(mut value: u64) -> u64 {
    value ^= value << 13;
    value ^= value >> 7;
    value ^ (value << 17)
}

async fn sample_rss(running: Arc<AtomicBool>, peak: Arc<AtomicU64>) {
    while running.load(Ordering::Acquire) {
        peak.fetch_max(rss_kib(), Ordering::Relaxed);
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

fn rss_kib() -> u64 {
    std::fs::read_to_string("/proc/self/status")
        .ok()
        .and_then(|status| {
            status.lines().find_map(|line| {
                line.strip_prefix("VmRSS:")?
                    .split_whitespace()
                    .next()?
                    .parse()
                    .ok()
            })
        })
        .unwrap_or(0)
}
