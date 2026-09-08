// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Sole owner of elastic small-write pipeline membership.

use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Instant;

use tokio::sync::{mpsc, oneshot, Semaphore};

use crate::negative_list::FailedDiskList;
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
        failed_disks: Arc::new(FailedDiskList::new(pool.policy.failed_disk_ttl)),
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
    let mut next_id = pipelines.len() as u64;
    loop {
        tokio::select! {
            command = commands.recv() => {
                let Some(ManagerCommand::Shutdown(done)) = command else { break; };
                runtime.publish(&[]);
                runtime.metrics.draining_pipelines.set(pipelines.len() as u64);
                for pipeline in &pipelines {
                    pipeline.begin_retire();
                }
                let result = join_all(pipelines).await;
                runtime.metrics.draining_pipelines.set(0);
                let _ = done.send(result);
                return;
            }
            _ = ticker.tick() => {
                let mut failed_pipelines = reap_finished(&runtime, &mut pipelines).await;
                while pipelines.len() < runtime.policy.min_pipelines {
                    match small_pipeline::spawn(Arc::clone(&runtime), next_id).await {
                        Ok(pipeline) => {
                            next_id = next_id.wrapping_add(1);
                            pipelines.push(pipeline);
                            if failed_pipelines > 0 {
                                runtime.metrics.pipeline_replacements.fetch_add(1, Ordering::Relaxed);
                                failed_pipelines -= 1;
                            }
                        }
                        Err(_) => break,
                    }
                }
                publish(&runtime, &pipelines);
                if should_scale_out(&runtime, &pipelines) && pipelines.len() < runtime.policy.max_pipelines {
                    if let Ok(pipeline) = small_pipeline::spawn(Arc::clone(&runtime), next_id).await {
                        next_id = next_id.wrapping_add(1);
                        pipelines.push(pipeline);
                        publish(&runtime, &pipelines);
                        runtime.metrics.scale_out.fetch_add(1, Ordering::Relaxed);
                    }
                    continue;
                }
                if let Some(index) = scale_in_candidate(&pipelines, runtime.policy.min_pipelines) {
                    let pipeline = pipelines.remove(index);
                    publish(&runtime, &pipelines);
                    runtime.metrics.draining_pipelines.inc();
                    pipeline.begin_retire();
                    let _ = pipeline.join.await;
                    runtime.metrics.draining_pipelines.dec();
                    runtime.metrics.scale_in.fetch_add(1, Ordering::Relaxed);
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

async fn reap_finished(runtime: &SmallPoolRuntime, pipelines: &mut Vec<ManagedPipeline>) -> usize {
    let mut index = 0;
    let mut changed = false;
    let mut failed = 0;
    while index < pipelines.len() {
        if pipelines[index].join.is_finished() {
            let pipeline = pipelines.remove(index);
            if !matches!(pipeline.join.await, Ok(Ok(()))) {
                failed += 1;
            }
            changed = true;
        } else {
            index += 1;
        }
    }
    if changed {
        publish(runtime, pipelines);
    }
    failed
}

fn should_scale_out(runtime: &SmallPoolRuntime, pipelines: &[ManagedPipeline]) -> bool {
    pipelines.iter().any(|pipeline| {
        pipeline.route.queued_bytes.load(Ordering::Relaxed) >= runtime.policy.scale_out_queue_bytes as u64
            || pipeline.route.queued_objects.load(Ordering::Relaxed)
                >= runtime.policy.scale_out_queue_objects as u64
    })
}

fn scale_in_candidate(pipelines: &[ManagedPipeline], min_pipelines: usize) -> Option<usize> {
    if pipelines.len() <= min_pipelines
        || pipelines.iter().any(|pipeline| {
            pipeline.route.queued_objects.load(Ordering::Relaxed) != 0
                || pipeline.route.busy.load(Ordering::Acquire)
                || pipeline.route.conversion_active.load(Ordering::Acquire)
        })
    {
        return None;
    }
    Some(pipelines.len() - 1)
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
