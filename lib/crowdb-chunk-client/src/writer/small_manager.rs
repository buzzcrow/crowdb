// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Sole owner of elastic small-write pipeline membership.

use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::{mpsc, oneshot, Semaphore};

use crate::{IoError, Result};

use super::small_pipeline::{self, ManagedPipeline};
use super::small_pool::{SmallPoolRuntime, SmallWritePool};

pub(crate) enum ManagerCommand {
    Shutdown(oneshot::Sender<Result<()>>),
}

pub(crate) async fn start(pool: Arc<SmallWritePool>) -> Result<Arc<SmallPoolRuntime>> {
    let (manager_tx, manager_rx) = mpsc::unbounded_channel();
    let runtime = Arc::new(SmallPoolRuntime {
        policy: Arc::clone(&pool.policy),
        allocator: Arc::clone(&pool.allocator),
        disk_writer: Arc::clone(&pool.disk_writer),
        routes: arc_swap::ArcSwap::from_pointee(Vec::new()),
        metrics: Arc::clone(&pool.metrics),
        origin: Instant::now(),
        closed: std::sync::atomic::AtomicBool::new(false),
        route_nonce: std::sync::atomic::AtomicU64::new(0),
        manager_tx,
        budget: Arc::new(Semaphore::new(pool.policy.memory_budget)),
    });
    let mut pipelines = Vec::with_capacity(pool.policy.min_pipelines);
    for id in 0..pool.policy.min_pipelines {
        match small_pipeline::spawn(Arc::clone(&runtime), id as u64).await {
            Ok(pipeline) => pipelines.push(pipeline),
            Err(error) => {
                for pipeline in &pipelines {
                    pipeline.begin_retire();
                }
                let _ = join_all(pipelines).await;
                return Err(error);
            }
        }
    }
    publish(&runtime, &pipelines);
    tokio::spawn(run(Arc::clone(&runtime), pipelines, manager_rx));
    Ok(runtime)
}

async fn run(
    runtime: Arc<SmallPoolRuntime>,
    mut pipelines: Vec<ManagedPipeline>,
    mut commands: mpsc::UnboundedReceiver<ManagerCommand>,
) {
    let mut ticker = tokio::time::interval(runtime.policy.control_interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut last_change = Instant::now()
        .checked_sub(runtime.policy.cooldown)
        .unwrap_or_else(Instant::now);
    let mut next_id = pipelines.len() as u64;
    loop {
        tokio::select! {
            command = commands.recv() => {
                let Some(ManagerCommand::Shutdown(done)) = command else { break; };
                runtime.publish(&[]);
                runtime.metrics.draining_pipelines.store(pipelines.len() as u64, Ordering::Relaxed);
                for pipeline in &pipelines {
                    pipeline.begin_retire();
                }
                let result = join_all(pipelines).await;
                runtime.metrics.draining_pipelines.store(0, Ordering::Relaxed);
                let _ = done.send(result);
                return;
            }
            _ = ticker.tick() => {
                reap_finished(&runtime, &mut pipelines).await;
                while pipelines.len() < runtime.policy.min_pipelines {
                    match small_pipeline::spawn(Arc::clone(&runtime), next_id).await {
                        Ok(pipeline) => {
                            next_id = next_id.wrapping_add(1);
                            pipelines.push(pipeline);
                            last_change = Instant::now();
                        }
                        Err(_) => break,
                    }
                }
                publish(&runtime, &pipelines);
                if last_change.elapsed() < runtime.policy.cooldown {
                    continue;
                }
                if should_scale_out(&runtime, &pipelines) && pipelines.len() < runtime.policy.max_pipelines {
                    if let Ok(pipeline) = small_pipeline::spawn(Arc::clone(&runtime), next_id).await {
                        next_id = next_id.wrapping_add(1);
                        pipelines.push(pipeline);
                        publish(&runtime, &pipelines);
                        runtime.metrics.scale_out.fetch_add(1, Ordering::Relaxed);
                        last_change = Instant::now();
                    }
                    continue;
                }
                if let Some(index) = scale_in_candidate(&runtime, &pipelines) {
                    let pipeline = pipelines.remove(index);
                    publish(&runtime, &pipelines);
                    runtime.metrics.draining_pipelines.fetch_add(1, Ordering::Relaxed);
                    pipeline.begin_retire();
                    let _ = pipeline.join.await;
                    runtime.metrics.draining_pipelines.fetch_sub(1, Ordering::Relaxed);
                    runtime.metrics.scale_in.fetch_add(1, Ordering::Relaxed);
                    last_change = Instant::now();
                }
            }
        }
    }
}

fn publish(runtime: &SmallPoolRuntime, pipelines: &[ManagedPipeline]) {
    let routes = pipelines
        .iter()
        .map(|pipeline| Arc::clone(&pipeline.route))
        .collect::<Vec<_>>();
    runtime.publish(&routes);
}

async fn reap_finished(runtime: &SmallPoolRuntime, pipelines: &mut Vec<ManagedPipeline>) {
    let mut index = 0;
    let mut changed = false;
    while index < pipelines.len() {
        if pipelines[index].join.is_finished() {
            let pipeline = pipelines.remove(index);
            let _ = pipeline.join.await;
            changed = true;
        } else {
            index += 1;
        }
    }
    if changed {
        publish(runtime, pipelines);
    }
}

fn should_scale_out(runtime: &SmallPoolRuntime, pipelines: &[ManagedPipeline]) -> bool {
    if pipelines.is_empty()
        || pipelines
            .iter()
            .any(|pipeline| !pipeline.route.busy.load(Ordering::Acquire))
    {
        return false;
    }
    let now = runtime.now_ms();
    let delay = duration_ms(runtime.policy.scale_out_delay);
    pipelines.iter().any(|pipeline| {
        let oldest = pipeline.route.oldest_enqueue_ms.load(Ordering::Relaxed);
        oldest != 0 && now.saturating_sub(oldest) >= delay
    })
}

fn scale_in_candidate(runtime: &SmallPoolRuntime, pipelines: &[ManagedPipeline]) -> Option<usize> {
    if pipelines.len() <= runtime.policy.min_pipelines {
        return None;
    }
    let now = runtime.now_ms();
    let delay = duration_ms(runtime.policy.scale_in_delay);
    pipelines.iter().position(|pipeline| {
        pipeline.route.queued_bytes.load(Ordering::Relaxed) == 0
            && !pipeline.route.busy.load(Ordering::Acquire)
            && now.saturating_sub(pipeline.route.last_active_ms.load(Ordering::Relaxed)) >= delay
    })
}

async fn join_all(pipelines: Vec<ManagedPipeline>) -> Result<()> {
    let mut first_error = None;
    for pipeline in pipelines {
        match pipeline.join.await {
            Ok(Err(error)) if first_error.is_none() => first_error = Some(error),
            Err(error) if first_error.is_none() => {
                first_error = Some(IoError::Internal(format!("small-write worker failed: {error}")));
            }
            Ok(Ok(()) | Err(_)) | Err(_) => {}
        }
    }
    first_error.map_or(Ok(()), Err)
}

fn duration_ms(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}
