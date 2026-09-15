// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Pacing-aware reconciliation for exceptional tentative `BusyBlock`s.

use std::collections::HashSet;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use arc_swap::ArcSwap;
use crowdb_chunkdb_client::ChunkdbClient;
use crowdb_protocol::chunkdb::rpc::{QuerySegmentOwnerRequest, SegmentOwnerDisposition};
use crowdb_protocol::diskdb::rpc::{BusyBlockValue, CommitState, Segment};
use crowdb_protocol::key::TentativeOwnerGraceKey;

use crate::bg_task::{BackgroundTask, BgCtx, CycleFut, Trigger};
use crate::ddb_config::DdbConfig;
use crate::model::alloc::{commit_blocks, free_blocks};

/// Independent, deliberately paced owner reconciliation task.
pub struct BusyBlockOwnerScanner {
    owner: Arc<dyn SegmentOwnerQuery>,
    config: Arc<ArcSwap<DdbConfig>>,
    pacer: Arc<dyn ZonePacer>,
}

impl BusyBlockOwnerScanner {
    #[must_use]
    pub fn new(owner: Arc<dyn SegmentOwnerQuery>, config: Arc<ArcSwap<DdbConfig>>) -> Self {
        Self {
            owner,
            config,
            pacer: Arc::new(TokioZonePacer),
        }
    }

    #[cfg(feature = "test-util")]
    pub async fn run_once_for_tests(&self, ctx: &BgCtx) {
        self.scan_once(ctx).await;
    }

    #[cfg(feature = "test-util")]
    #[must_use]
    pub fn with_pacer_for_tests(mut self, pacer: Arc<dyn ZonePacer>) -> Self {
        self.pacer = pacer;
        self
    }

    async fn scan_once(&self, ctx: &BgCtx) {
        if !ctx.container.lifecycle_phase().allows_mutating_rpcs() {
            return;
        }
        let mut disk_groups = ctx.container.disk_group_ids();
        disk_groups.sort_unstable();
        for disk_group_id in disk_groups {
            let Some(dg) = ctx.container.get_disk_group(disk_group_id) else {
                continue;
            };
            let mut disks = dg.disks.read().unwrap().clone();
            disks.sort_unstable_by_key(|disk| (disk.disk_id.high, disk.disk_id.low));
            for disk in disks {
                let mut zones = disk.zones.load().as_ref().clone();
                zones.sort_unstable_by_key(|zone| zone.zone_index);
                for zone in zones {
                    self.scan_zone(ctx, &dg, disk.disk_id, zone.zone_index).await;
                    let delay = self.config.load().scanner.tentative_owner_zone_delay_secs;
                    if delay != 0 {
                        self.pacer.sleep(Duration::from_secs(u64::from(delay))).await;
                    }
                }
            }
        }
    }

    async fn scan_zone(
        &self,
        ctx: &BgCtx,
        dg: &Arc<crate::model::disk_group::DdbDiskGroup>,
        disk_id: crowdb_protocol::common::DiskId,
        zone_index: u32,
    ) {
        let bind = dg.bind();
        let Ok(records) = ctx.kv.read_zone_records(bind, &disk_id, zone_index).await else {
            tracing::warn!(
                ?disk_id,
                zone_index,
                "tentative owner scan could not read zone records"
            );
            return;
        };
        let freed: HashSet<_> = records
            .free
            .iter()
            .map(|record| {
                (
                    record.key.disk_id,
                    record.key.zone_index,
                    record.key.unit_offset,
                    record.key.allocation_ts,
                )
            })
            .collect();
        for record in records.busy {
            if record.value.commit_state != CommitState::Tentative as i32 {
                continue;
            }
            if freed.contains(&(
                record.key.disk_id,
                record.key.zone_index,
                record.key.unit_offset,
                record.value.allocation_ts,
            )) {
                continue;
            }
            self.reconcile(
                ctx,
                dg,
                record.key.disk_id,
                record.key.zone_index,
                record.key.unit_offset,
                record.value,
            )
            .await;
        }
    }

    async fn reconcile(
        &self,
        ctx: &BgCtx,
        dg: &Arc<crate::model::disk_group::DdbDiskGroup>,
        disk_id: crowdb_protocol::common::DiskId,
        zone_index: u32,
        unit_offset: u64,
        busy: BusyBlockValue,
    ) {
        let Some(chunk_id) = busy.owner_chunk else {
            tracing::warn!(
                ?disk_id,
                zone_index,
                unit_offset,
                "tentative BusyBlock has no owner; retaining"
            );
            return;
        };
        let segment = Segment {
            disk_id: Some(disk_id),
            zone_index,
            unit_offset,
            unit_count: busy.unit_count,
            owner_chunk: Some(chunk_id),
            allocation_ts: busy.allocation_ts,
        };
        let grace_key = TentativeOwnerGraceKey {
            disk_id,
            zone_index,
            unit_offset,
            allocation_ts: busy.allocation_ts,
        };
        let disposition = self
            .owner
            .query(QuerySegmentOwnerRequest {
                chunk_id: Some(chunk_id),
                segment: Some(segment),
            })
            .await;
        match disposition {
            Some(SegmentOwnerDisposition::Referenced) => {
                ctx.metrics.tentative_owner_referenced.inc();
                if commit_blocks(dg, &[segment], &ctx.kv, &ctx.metrics).await.is_ok() {
                    let _ = ctx.kv.delete_tentative_owner_grace(dg.bind(), &grace_key).await;
                }
            }
            Some(SegmentOwnerDisposition::TaskPending) => {
                ctx.metrics.tentative_owner_task_pending.inc();
                let _ = ctx.kv.delete_tentative_owner_grace(dg.bind(), &grace_key).await;
            }
            Some(SegmentOwnerDisposition::Absent) => {
                ctx.metrics.tentative_owner_absent.inc();
                self.free_after_grace(ctx, dg, segment, grace_key).await;
            }
            None => {
                ctx.metrics.tentative_owner_transient.inc();
                tracing::warn!(
                    ?disk_id,
                    zone_index,
                    unit_offset,
                    "tentative owner query failed; retaining"
                );
            }
        }
    }

    async fn free_after_grace(
        &self,
        ctx: &BgCtx,
        dg: &Arc<crate::model::disk_group::DdbDiskGroup>,
        segment: Segment,
        grace_key: TentativeOwnerGraceKey,
    ) {
        let now_secs = now_secs();
        let first_absent = match ctx.kv.get_tentative_owner_grace(dg.bind(), &grace_key).await {
            Ok(Some(value)) => value,
            Ok(None) => {
                if ctx
                    .kv
                    .put_tentative_owner_grace(dg.bind(), &grace_key, now_secs)
                    .await
                    .is_err()
                {
                    tracing::warn!("could not persist tentative owner grace; retaining");
                }
                return;
            }
            Err(_) => return,
        };
        let grace = self.config.load().scanner.tentative_owner_grace_secs;
        if now_secs.saturating_sub(first_absent) < grace {
            return;
        }
        if free_blocks(dg, &[segment], &ctx.kv).await.is_ok() {
            let _ = ctx.kv.delete_tentative_owner_grace(dg.bind(), &grace_key).await;
        }
    }
}

/// Narrow owner-query boundary used by the production client and scanner tests.
pub trait SegmentOwnerQuery: Send + Sync + 'static {
    fn query<'a>(
        &'a self,
        request: QuerySegmentOwnerRequest,
    ) -> Pin<Box<dyn Future<Output = Option<SegmentOwnerDisposition>> + Send + 'a>>;
}

/// Awaited pacing boundary after each visited zone.
pub trait ZonePacer: Send + Sync + 'static {
    fn sleep(&self, duration: Duration) -> Pin<Box<dyn Future<Output = ()> + Send + '_>>;
}

struct TokioZonePacer;

impl ZonePacer for TokioZonePacer {
    fn sleep(&self, duration: Duration) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
        Box::pin(tokio::time::sleep(duration))
    }
}

impl SegmentOwnerQuery for ChunkdbClient {
    fn query<'a>(
        &'a self,
        request: QuerySegmentOwnerRequest,
    ) -> Pin<Box<dyn Future<Output = Option<SegmentOwnerDisposition>> + Send + 'a>> {
        Box::pin(async move {
            self.query_segment_owner(request)
                .await
                .ok()
                .and_then(|response| SegmentOwnerDisposition::try_from(response.disposition).ok())
        })
    }
}

impl BackgroundTask for BusyBlockOwnerScanner {
    fn run_cycle<'a>(&'a self, ctx: &'a BgCtx) -> CycleFut<'a> {
        Box::pin(async move {
            self.scan_once(ctx).await;
            Ok(())
        })
    }

    fn trigger(&self) -> Trigger {
        let config = Arc::clone(&self.config);
        Trigger::TimerFn(Box::new(move || {
            Duration::from_secs(u64::from(
                config.load().scanner.tentative_owner_scan_interval_secs,
            ))
        }))
    }

    fn name(&self) -> &'static str {
        "busy-block-owner-scanner"
    }
}

fn now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |duration| duration.as_secs())
}
