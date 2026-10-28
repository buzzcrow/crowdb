// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Continuous closed-loop load with one shared monotonic measurement window.
use clap::Args;
use crowdb_common::metrics::histogram::{HistogramData, LatencyHistogram};
use serde::Serialize;
use serde_json::{json, Value};
use std::{
    future::Future,
    path::PathBuf,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::{
    task::JoinSet,
    time::{sleep_until, Instant},
};

#[derive(Args, Debug, Clone, Serialize)]
pub struct LoadOptions {
    #[arg(long, default_value_t = 5, global = true)]
    pub warmup_secs: u64,
    #[arg(long, default_value_t = 20, global = true)]
    pub duration_secs: u64,
    #[arg(long, default_value_t = 5, global = true)]
    pub tail_secs: u64,
    #[arg(long, default_value_t = 30, global = true)]
    pub drain_timeout_secs: u64,
    #[arg(long, default_value_t = 1, global = true)]
    pub loader_num: usize,
    #[arg(long, global = true)]
    pub output: Option<PathBuf>,
}

impl LoadOptions {
    pub fn validate(&self) -> Result<(), String> {
        if self.duration_secs == 0
            || self.duration_secs > 86400
            || self.warmup_secs > 3600
            || self.tail_secs > 3600
            || self.drain_timeout_secs == 0
            || self.drain_timeout_secs > 3600
            || self.loader_num == 0
            || self.loader_num > 4096
        {
            return Err(
                "invalid load window or concurrency (measurement 1..86400s, concurrency 1..4096)".into(),
            );
        }
        Ok(())
    }
}

#[derive(Clone, Copy)]
pub struct Window {
    pub start: Instant,
    pub measure: Instant,
    pub tail: Instant,
    pub stop: Instant,
}
impl Window {
    pub fn new(options: &LoadOptions) -> Self {
        let start = Instant::now();
        let measure = start + Duration::from_secs(options.warmup_secs);
        let tail = measure + Duration::from_secs(options.duration_secs);
        Self {
            start,
            measure,
            tail,
            stop: tail + Duration::from_secs(options.tail_secs),
        }
    }
    pub fn phase(self, time: Instant) -> usize {
        if time < self.measure {
            0
        } else if time < self.tail {
            1
        } else {
            2
        }
    }
}

struct Counts {
    admitted: AtomicU64,
    completed: AtomicU64,
    failed: AtomicU64,
}
impl Counts {
    fn new() -> Self {
        Self {
            admitted: AtomicU64::new(0),
            completed: AtomicU64::new(0),
            failed: AtomicU64::new(0),
        }
    }
    fn json(&self) -> Value {
        let admitted = self.admitted.load(Ordering::Relaxed);
        let completed = self.completed.load(Ordering::Relaxed);
        let failed = self.failed.load(Ordering::Relaxed);
        json!({"admitted": admitted, "completed": completed, "failed": failed,
               "incomplete": admitted - completed - failed})
    }
}
struct Worker {
    phases: [Counts; 3],
    measured: LatencyHistogram,
    bytes: AtomicU64,
    crossing: AtomicU64,
}
impl Worker {
    fn new() -> Self {
        Self {
            phases: std::array::from_fn(|_| Counts::new()),
            measured: LatencyHistogram::new("bench.latency".into()),
            bytes: AtomicU64::new(0),
            crossing: AtomicU64::new(0),
        }
    }
}

pub struct LoadResult {
    pub report: Value,
    pub errors: Vec<String>,
}

/// Stop and join all workers if a resource guard reports a failure.
pub async fn run_until<F, Fut, Stop>(
    options: &LoadOptions,
    window: Window,
    operation: F,
    stop: Stop,
) -> LoadResult
where
    F: Fn(usize, u64) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<u64, String>> + Send + 'static,
    Stop: Future<Output = String>,
{
    tokio::pin!(stop);
    let operation = Arc::new(operation);
    let workers: Vec<_> = (0..options.loader_num).map(|_| Arc::new(Worker::new())).collect();
    let mut tasks = JoinSet::new();
    for (index, worker) in workers.iter().enumerate() {
        let worker = worker.clone();
        let operation = operation.clone();
        tasks.spawn(async move {
            let mut sequence = 0;
            let mut first_error = None;
            loop {
                let admitted = Instant::now();
                if admitted >= window.stop {
                    break;
                }
                let phase = window.phase(admitted);
                worker.phases[phase].admitted.fetch_add(1, Ordering::Relaxed);
                let outcome = operation(index, sequence).await;
                let completed = Instant::now();
                if window.phase(completed) != phase || completed >= window.stop {
                    worker.crossing.fetch_add(1, Ordering::Relaxed);
                }
                match outcome {
                    Ok(bytes) => {
                        worker.phases[phase].completed.fetch_add(1, Ordering::Relaxed);
                        if phase == 1 && completed < window.tail {
                            worker.measured.observe(
                                u64::try_from((completed - admitted).as_nanos()).unwrap_or(u64::MAX),
                            );
                            worker.bytes.fetch_add(bytes, Ordering::Relaxed);
                        }
                    }
                    Err(error) => {
                        worker.phases[phase].failed.fetch_add(1, Ordering::Relaxed);
                        if first_error.is_none() {
                            first_error = Some(error);
                        }
                        // Yield even for immediately rejected operations.
                        tokio::task::yield_now().await;
                    }
                }
                sequence += 1;
            }
            first_error
        });
    }
    let mut errors = Vec::new();
    let deadline = window.stop + Duration::from_secs(options.drain_timeout_secs);
    loop {
        tokio::select! {
            reason = &mut stop => {
                errors.push(reason);
                tasks.abort_all();
                while tasks.join_next().await.is_some() {}
                break;
            },
            result = tasks.join_next() => match result {
                None => break,
                Some(Ok(Some(error))) => errors.push(error),
                Some(Err(error)) => errors.push(format!("load worker: {error}")),
                Some(Ok(None)) => {},
            },
            () = sleep_until(deadline) => {
                errors.push("drain timeout; unfinished operations have unknown outcomes".into());
                tasks.abort_all();
                while tasks.join_next().await.is_some() {}
                break;
            }
        }
    }
    let report = summarize(options, window, &workers);
    if report["failed"].as_u64().unwrap_or(0) > 0 && errors.is_empty() {
        errors.push("operation failures in an aborted worker".into());
    }
    LoadResult { report, errors }
}

#[allow(clippy::cast_precision_loss)]
fn summarize(options: &LoadOptions, window: Window, workers: &[Arc<Worker>]) -> Value {
    let mut histogram = HistogramData::default();
    let mut phases = [[0_u64; 4]; 3];
    let mut bytes = 0;
    let mut crossing = 0;
    for worker in workers {
        histogram.merge(&worker.measured.capture_total());
        bytes += worker.bytes.load(Ordering::Relaxed);
        crossing += worker.crossing.load(Ordering::Relaxed);
        for (sum, counts) in phases.iter_mut().zip(&worker.phases) {
            let counts = counts.json();
            for (i, field) in ["admitted", "completed", "failed", "incomplete"]
                .iter()
                .enumerate()
            {
                sum[i] += counts[field].as_u64().unwrap_or(0);
            }
        }
    }
    let h = histogram.snapshot(0);
    let phases_json: Vec<_> = phases.iter().zip(["warmup", "measurement", "tail"]).map(|(v, name)|
        json!({"phase":name,"admitted":v[0],"completed":v[1],"failed":v[2],"incomplete":v[3]})).collect();
    json!({"phases":phases_json,"measured_operations":h.count,"measured_bytes":bytes,
        "ops_per_sec":h.count as f64 / options.duration_secs as f64,
        "bytes_per_sec":bytes as f64 / options.duration_secs as f64,
        "failed":phases.iter().map(|p|p[2]).sum::<u64>(),
        "incomplete":phases.iter().map(|p|p[3]).sum::<u64>(),
        "boundary_crossings":crossing,"duration_secs":options.duration_secs,
        "elapsed_secs_including_drain":window.start.elapsed().as_secs_f64(),
        "selection":"successful requests admitted and completed within measurement; crossings excluded",
        "latency_ns":{"count":h.count,"avg":h.avg,"min":h.min,"max":h.max,"p50":h.p50,"p99":h.p99,"p999":h.p999}})
}
