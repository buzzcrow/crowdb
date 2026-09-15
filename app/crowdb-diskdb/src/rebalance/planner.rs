// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! One-job-at-a-time imbalance planner with serial zone traversal.

use std::collections::HashSet;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use arc_swap::ArcSwap;
use crowdb_protocol::common::DiskId;
use crowdb_protocol::diskdb::rpc::{CommitState, RelocationJournalPhase, Segment};

use crate::bg_task::{BackgroundTask, BgCtx, CycleFut, Trigger};
use crate::ddb_config::DdbConfig;
use crate::model::disk_group::DdbDiskGroup;

use super::RelocationWorker;

pub trait RebalanceZonePacer: Send + Sync + 'static {
    fn sleep(&self, duration: Duration) -> Pin<Box<dyn Future<Output = ()> + Send + '_>>;
}

struct TokioRebalanceZonePacer;

impl RebalanceZonePacer for TokioRebalanceZonePacer {
    fn sleep(&self, duration: Duration) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
        Box::pin(tokio::time::sleep(duration))
    }
}

pub struct RebalancePlannerTask {
    worker: Arc<RelocationWorker>,
    config: Arc<ArcSwap<DdbConfig>>,
    pacer: Arc<dyn RebalanceZonePacer>,
}

impl RebalancePlannerTask {
    #[must_use]
    pub fn new(worker: Arc<RelocationWorker>, config: Arc<ArcSwap<DdbConfig>>) -> Self {
        Self {
            worker,
            config,
            pacer: Arc::new(TokioRebalanceZonePacer),
        }
    }

    #[cfg(feature = "test-util")]
    #[must_use]
    pub fn with_pacer_for_tests(mut self, pacer: Arc<dyn RebalanceZonePacer>) -> Self {
        self.pacer = pacer;
        self
    }

    #[cfg(feature = "test-util")]
    pub async fn run_once_for_tests(&self, ctx: &BgCtx) {
        self.run_once(ctx).await;
    }

    async fn run_once(&self, ctx: &BgCtx) {
        if !ctx.container.lifecycle_phase().allows_mutating_rpcs() {
            return;
        }
        let mut disk_group_ids = ctx.container.disk_group_ids();
        disk_group_ids.sort_unstable();
        let mut started = 0;
        for disk_group_id in disk_group_ids {
            let Some(dg) = ctx.container.get_disk_group(disk_group_id) else {
                continue;
            };
            if self.resume_active(ctx, &dg).await {
                continue;
            }
            let settings = self.config.load().rebalance.clone();
            if !settings.enabled || started >= settings.max_jobs_per_cycle {
                continue;
            }
            let Some((source_disk, target_disk)) =
                select_imbalanced_pair(&dg, settings.imbalance_threshold_pct)
            else {
                continue;
            };
            let Some(source) = self.find_source(ctx, &dg, source_disk).await else {
                continue;
            };
            match self.worker.reserve(ctx, &dg, source, target_disk).await {
                Ok((key, mut journal)) => {
                    started += 1;
                    if let Err(error) = self.worker.resume(ctx, &dg, &key, &mut journal).await {
                        ctx.metrics.rebalance_errors_total.inc();
                        tracing::warn!(disk_group_id, %error, "relocation job did not advance");
                    }
                }
                Err(error) => {
                    ctx.metrics.rebalance_errors_total.inc();
                    tracing::warn!(disk_group_id, %error, "relocation target reservation failed");
                }
            }
        }
        self.refresh_plan_metrics(ctx).await;
    }

    async fn resume_active(&self, ctx: &BgCtx, dg: &Arc<DdbDiskGroup>) -> bool {
        let journals = match ctx.kv.list_relocation_journals(dg.bind()).await {
            Ok(journals) => journals,
            Err(error) => {
                ctx.metrics.rebalance_errors_total.inc();
                tracing::warn!(disk_group_id = dg.disk_group_id, %error, "could not list relocation journals");
                return true;
            }
        };
        let mut active = false;
        for (key, mut journal) in journals {
            if journal.target_disk_group_id != dg.disk_group_id || terminal(journal.phase) {
                continue;
            }
            active = true;
            if let Err(error) = self.worker.resume(ctx, dg, &key, &mut journal).await {
                ctx.metrics.rebalance_errors_total.inc();
                tracing::warn!(disk_group_id = dg.disk_group_id, %error, "relocation resume failed");
            }
        }
        active
    }

    async fn refresh_plan_metrics(&self, ctx: &BgCtx) {
        let mut count = 0u64;
        let mut blocks = 0u64;
        for disk_group_id in ctx.container.disk_group_ids() {
            let Some(dg) = ctx.container.get_disk_group(disk_group_id) else {
                continue;
            };
            let Ok(journals) = ctx.kv.list_relocation_journals(dg.bind()).await else {
                continue;
            };
            for (_, journal) in journals {
                if journal.target_disk_group_id != dg.disk_group_id || terminal(journal.phase) {
                    continue;
                }
                count = count.saturating_add(1);
                blocks =
                    blocks.saturating_add(u64::from(journal.source.map_or(0, |source| source.unit_count)));
            }
        }
        ctx.metrics.rebalance_plan_count.set(count);
        ctx.metrics.rebalance_planned_blocks.set(blocks);
    }

    async fn find_source(&self, ctx: &BgCtx, dg: &Arc<DdbDiskGroup>, source_disk: DiskId) -> Option<Segment> {
        let disk = dg
            .disks
            .read()
            .unwrap()
            .iter()
            .find(|disk| disk.disk_id == source_disk)
            .cloned()?;
        let mut zones = disk.zones.load().as_ref().clone();
        zones.sort_unstable_by_key(|zone| zone.zone_index);
        for zone in zones {
            let records = ctx
                .kv
                .read_zone_records(dg.bind(), &source_disk, zone.zone_index)
                .await;
            let delay = self.config.load().rebalance.zone_delay_secs;
            if delay != 0 {
                self.pacer.sleep(Duration::from_secs(u64::from(delay))).await;
            }
            let Ok(records) = records else {
                continue;
            };
            let freed: HashSet<_> = records
                .free
                .iter()
                .map(|record| (record.key.unit_offset, record.key.allocation_ts))
                .collect();
            if let Some(record) = records.busy.into_iter().find(|record| {
                record.value.commit_state == CommitState::Committed as i32
                    && record.value.owner_chunk.is_some()
                    && !freed.contains(&(record.key.unit_offset, record.value.allocation_ts))
            }) {
                return Some(Segment {
                    disk_id: Some(source_disk),
                    zone_index: record.key.zone_index,
                    unit_offset: record.key.unit_offset,
                    unit_count: record.value.unit_count,
                    owner_chunk: record.value.owner_chunk,
                    allocation_ts: record.value.allocation_ts,
                });
            }
        }
        None
    }
}

impl BackgroundTask for RebalancePlannerTask {
    fn run_cycle<'a>(&'a self, ctx: &'a BgCtx) -> CycleFut<'a> {
        Box::pin(async move {
            self.run_once(ctx).await;
            Ok(())
        })
    }

    fn trigger(&self) -> Trigger {
        let config = Arc::clone(&self.config);
        Trigger::TimerFn(Box::new(move || {
            Duration::from_secs(u64::from(config.load().rebalance.plan_interval_secs))
        }))
    }

    fn name(&self) -> &'static str {
        "rebalance-planner"
    }
}

fn select_imbalanced_pair(dg: &DdbDiskGroup, threshold: u32) -> Option<(DiskId, DiskId)> {
    let allocatable: HashSet<_> = dg
        .disks
        .read()
        .unwrap()
        .iter()
        .filter(|disk| disk.allocatable())
        .map(|disk| disk.disk_id)
        .collect();
    let mut usages: Vec<_> = dg
        .aggregate_usage()
        .disks
        .into_iter()
        .filter(|usage| allocatable.contains(&usage.disk_id) && usage.capacity_bytes != 0)
        .map(|usage| {
            (
                usage.disk_id,
                usage.busy_bytes.saturating_mul(100) / usage.capacity_bytes,
            )
        })
        .collect();
    if usages.len() < 2 {
        return None;
    }
    usages.sort_unstable_by_key(|(disk_id, used_pct)| (*used_pct, disk_id.high, disk_id.low));
    let target = usages.first().copied()?;
    let source = usages.last().copied()?;
    (source.1.saturating_sub(target.1) >= u64::from(threshold)).then_some((source.0, target.0))
}

fn terminal(raw: i32) -> bool {
    matches!(
        RelocationJournalPhase::try_from(raw),
        Ok(RelocationJournalPhase::SourceFreed | RelocationJournalPhase::Discarded)
    )
}
