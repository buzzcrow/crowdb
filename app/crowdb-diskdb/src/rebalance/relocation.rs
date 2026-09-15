// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Restart-safe reserve, copy, owner handoff, confirm, and source-free flow.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use arc_swap::ArcSwapOption;
use bytes::Bytes;
use crowdb_chunkdb_client::ChunkdbClient;
use crowdb_diskdb_client::DiskdbClient;
use crowdb_diskio_client::{DiskId as IoDiskId, DiskioClient, DiskioClientConfig, Durability, SegmentTarget};
use crowdb_kv_client::{HardwareClient, ServiceRegistryClient};
use crowdb_protocol::chunk_task::relocation_operation_id;
use crowdb_protocol::chunkdb::rpc::{RelocateSegmentHandoffRequest, RelocationHandoffDisposition};
use crowdb_protocol::common::DiskId;
use crowdb_protocol::diskdb::rpc::{
    CommitState, FreeBlocksRequest, RelocationJournalPhase, RelocationJournalValue, Segment,
    RELOCATION_JOURNAL_SCHEMA_VERSION,
};
use crowdb_protocol::key::RelocationJournalKey;

use crate::bg_task::BgCtx;
use crate::model::alloc::{allocate_blocks, commit_blocks, free_blocks};
use crate::model::disk_group::DdbDiskGroup;

#[derive(Debug, thiserror::Error)]
pub enum RelocationWorkerError {
    #[error("relocation source is invalid or no longer live")]
    InvalidSource,
    #[error("relocation target disk is not an eligible peer")]
    InvalidTarget,
    #[error("relocation journal is invalid: {0}")]
    InvalidJournal(String),
    #[error("relocation allocation failed: {0}")]
    Allocation(String),
    #[error("relocation persistence failed: {0}")]
    Persistence(String),
    #[error("relocation I/O failed: {0}")]
    Io(String),
    #[error("relocation owner request failed: {0}")]
    Owner(String),
    #[error("relocation owner rejected the handoff")]
    Rejected,
    #[error("relocation finalization failed: {0}")]
    Finalize(String),
}

pub trait RelocationOwner: Send + Sync + 'static {
    fn handoff<'a>(
        &'a self,
        request: RelocateSegmentHandoffRequest,
    ) -> Pin<Box<dyn Future<Output = Result<RelocationHandoffDisposition, String>> + Send + 'a>>;
}

pub trait RelocationIo: Send + Sync + 'static {
    fn copy_and_fsync<'a>(
        &'a self,
        source: Segment,
        target: Segment,
        unit_size: u32,
    ) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>>;
}

pub trait RelocationSourceFree: Send + Sync + 'static {
    fn free_source<'a>(
        &'a self,
        source: Segment,
    ) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>>;
}

pub struct RelocationWorker {
    owner: Arc<dyn RelocationOwner>,
    io: Arc<dyn RelocationIo>,
    source_free: Option<Arc<dyn RelocationSourceFree>>,
}

impl RelocationWorker {
    #[must_use]
    pub fn new(owner: Arc<dyn RelocationOwner>, io: Arc<dyn RelocationIo>) -> Self {
        Self {
            owner,
            io,
            source_free: None,
        }
    }

    #[must_use]
    pub fn with_source_free(mut self, source_free: Arc<dyn RelocationSourceFree>) -> Self {
        self.source_free = Some(source_free);
        self
    }

    /// Reserve a tentative target on one selected peer disk and persist the
    /// `Reserved` checkpoint before any data I/O.
    pub async fn reserve(
        &self,
        ctx: &BgCtx,
        dg: &Arc<DdbDiskGroup>,
        source: Segment,
        target_disk_id: DiskId,
    ) -> Result<(RelocationJournalKey, RelocationJournalValue), RelocationWorkerError> {
        let source_disk = source.disk_id.ok_or(RelocationWorkerError::InvalidSource)?;
        let Some((busy, _)) = ctx
            .kv
            .get_busy(dg.bind(), &source_disk, source.zone_index, source.unit_offset)
            .await
            .map_err(|error| RelocationWorkerError::Persistence(error.to_string()))?
        else {
            return Err(RelocationWorkerError::InvalidSource);
        };
        if busy.owner_chunk != source.owner_chunk
            || busy.unit_count != source.unit_count
            || busy.allocation_ts != source.allocation_ts
            || busy.commit_state != CommitState::Committed as i32
        {
            return Err(RelocationWorkerError::InvalidSource);
        }
        let key = journal_key(&source)?;
        if let Some(existing) = ctx
            .kv
            .get_relocation_journal(dg.bind(), &key)
            .await
            .map_err(|error| RelocationWorkerError::Persistence(error.to_string()))?
        {
            return Ok((key, existing));
        }
        let owner_chunk = source.owner_chunk.ok_or(RelocationWorkerError::InvalidSource)?;
        let disks = dg.disks.read().unwrap().clone();
        if source_disk == target_disk_id || !disks.iter().any(|disk| disk.disk_id == target_disk_id) {
            return Err(RelocationWorkerError::InvalidTarget);
        }
        let excluded: Vec<_> = disks
            .iter()
            .filter_map(|disk| (disk.disk_id != target_disk_id).then_some(disk.disk_id))
            .collect();
        let config = ctx.config.load();
        let mut targets = allocate_blocks(
            dg,
            source.unit_count,
            1,
            &excluded,
            false,
            &owner_chunk,
            busy.unit_size,
            &ctx.kv,
            config.storage.cas_retry_limit,
            config.storage.zone_rotate_count,
            &ctx.metrics,
        )
        .await
        .map_err(|error| RelocationWorkerError::Allocation(format!("{error:?}")))?;
        let target = targets.pop().ok_or(RelocationWorkerError::InvalidTarget)?;
        if target.disk_id != Some(target_disk_id) {
            free_blocks(dg, &[target], &ctx.kv)
                .await
                .map_err(|error| RelocationWorkerError::Finalize(error.to_string()))?;
            return Err(RelocationWorkerError::InvalidTarget);
        }
        let now = now_ms();
        let value = RelocationJournalValue {
            schema_version: RELOCATION_JOURNAL_SCHEMA_VERSION,
            operation_id: relocation_operation_id(&source),
            owner_chunk: Some(owner_chunk),
            source: Some(source),
            target: Some(target),
            target_disk_group_id: dg.disk_group_id,
            unit_size: busy.unit_size,
            phase: RelocationJournalPhase::Reserved.into(),
            created_at_ms: now,
            updated_at_ms: now,
            last_error: String::new(),
        };
        ctx.kv
            .put_relocation_journal(dg.bind(), &key, &value)
            .await
            .map_err(|error| RelocationWorkerError::Persistence(error.to_string()))?;
        Ok((key, value))
    }

    /// Persist a relocation around a tentative target that was allocated on
    /// this disk-group through the normal allocation RPC.
    pub async fn adopt(
        &self,
        ctx: &BgCtx,
        target_dg: &Arc<DdbDiskGroup>,
        source: Segment,
        target: Segment,
    ) -> Result<(RelocationJournalKey, RelocationJournalValue), RelocationWorkerError> {
        let key = journal_key(&source)?;
        if let Some(existing) = ctx
            .kv
            .get_relocation_journal(target_dg.bind(), &key)
            .await
            .map_err(|error| RelocationWorkerError::Persistence(error.to_string()))?
        {
            if existing.source == Some(source) && existing.target == Some(target) {
                return Ok((key, existing));
            }
            return Err(RelocationWorkerError::InvalidJournal(
                "source identity is already paired with another target".into(),
            ));
        }
        if source.owner_chunk.is_none()
            || source.owner_chunk != target.owner_chunk
            || source.unit_count == 0
            || source.unit_count != target.unit_count
        {
            return Err(RelocationWorkerError::InvalidTarget);
        }
        let target_disk = target.disk_id.ok_or(RelocationWorkerError::InvalidTarget)?;
        if !target_dg
            .disks
            .read()
            .unwrap()
            .iter()
            .any(|disk| disk.disk_id == target_disk)
        {
            return Err(RelocationWorkerError::InvalidTarget);
        }
        let Some((busy, _)) = ctx
            .kv
            .get_busy(
                target_dg.bind(),
                &target_disk,
                target.zone_index,
                target.unit_offset,
            )
            .await
            .map_err(|error| RelocationWorkerError::Persistence(error.to_string()))?
        else {
            return Err(RelocationWorkerError::InvalidTarget);
        };
        if busy.owner_chunk != target.owner_chunk
            || busy.unit_count != target.unit_count
            || busy.allocation_ts != target.allocation_ts
            || busy.commit_state != CommitState::Tentative as i32
        {
            return Err(RelocationWorkerError::InvalidTarget);
        }
        let now = now_ms();
        let value = RelocationJournalValue {
            schema_version: RELOCATION_JOURNAL_SCHEMA_VERSION,
            operation_id: relocation_operation_id(&source),
            owner_chunk: source.owner_chunk,
            source: Some(source),
            target: Some(target),
            target_disk_group_id: target_dg.disk_group_id,
            unit_size: busy.unit_size,
            phase: RelocationJournalPhase::Reserved.into(),
            created_at_ms: now,
            updated_at_ms: now,
            last_error: String::new(),
        };
        ctx.kv
            .put_relocation_journal(target_dg.bind(), &key, &value)
            .await
            .map_err(|error| RelocationWorkerError::Persistence(error.to_string()))?;
        Ok((key, value))
    }

    /// Resume one journal from its durable phase. `Accepted` stops the cycle;
    /// a later call polls the owner again, while all other successful phases
    /// continue to the next durability boundary.
    pub async fn resume(
        &self,
        ctx: &BgCtx,
        dg: &Arc<DdbDiskGroup>,
        key: &RelocationJournalKey,
        value: &mut RelocationJournalValue,
    ) -> Result<(), RelocationWorkerError> {
        validate_journal(key, value)?;
        loop {
            match phase(value)? {
                RelocationJournalPhase::Reserved => {
                    self.io
                        .copy_and_fsync(source(value)?, target(value)?, value.unit_size)
                        .await
                        .map_err(RelocationWorkerError::Io)?;
                    self.checkpoint(ctx, dg, key, value, RelocationJournalPhase::Copied)
                        .await?;
                }
                RelocationJournalPhase::Copied | RelocationJournalPhase::Accepted => {
                    let disposition = self
                        .owner
                        .handoff(RelocateSegmentHandoffRequest {
                            operation_id: value.operation_id,
                            chunk_id: value.owner_chunk,
                            source: value.source,
                            target: value.target,
                        })
                        .await
                        .map_err(RelocationWorkerError::Owner)?;
                    match disposition {
                        RelocationHandoffDisposition::Accepted => {
                            self.checkpoint(ctx, dg, key, value, RelocationJournalPhase::Accepted)
                                .await?;
                            return Ok(());
                        }
                        RelocationHandoffDisposition::Published => {
                            self.checkpoint(ctx, dg, key, value, RelocationJournalPhase::Published)
                                .await?;
                        }
                        RelocationHandoffDisposition::Stale => {
                            free_blocks(dg, &[target(value)?], &ctx.kv)
                                .await
                                .map_err(|error| RelocationWorkerError::Finalize(error.to_string()))?;
                            self.checkpoint(ctx, dg, key, value, RelocationJournalPhase::Discarded)
                                .await?;
                            return Ok(());
                        }
                        RelocationHandoffDisposition::Rejected => {
                            value.last_error = "owner rejected relocation handoff".into();
                            value.updated_at_ms = now_ms();
                            ctx.kv
                                .put_relocation_journal(dg.bind(), key, value)
                                .await
                                .map_err(|error| RelocationWorkerError::Persistence(error.to_string()))?;
                            return Err(RelocationWorkerError::Rejected);
                        }
                    }
                }
                RelocationJournalPhase::Published => {
                    commit_blocks(dg, &[target(value)?], &ctx.kv, &ctx.metrics)
                        .await
                        .map_err(|error| RelocationWorkerError::Finalize(error.to_string()))?;
                    self.checkpoint(ctx, dg, key, value, RelocationJournalPhase::TargetConfirmed)
                        .await?;
                }
                RelocationJournalPhase::TargetConfirmed => {
                    let source = source(value)?;
                    let source_dg = source.disk_id.and_then(|disk_id| {
                        ctx.container
                            .disk_group_ids()
                            .into_iter()
                            .find_map(|disk_group_id| {
                                let group = ctx.container.get_disk_group(disk_group_id)?;
                                let owns_source = group
                                    .disks
                                    .read()
                                    .unwrap()
                                    .iter()
                                    .any(|disk| disk.disk_id == disk_id);
                                owns_source.then_some(group)
                            })
                    });
                    if let Some(source_dg) = source_dg {
                        free_blocks(&source_dg, &[source], &ctx.kv)
                            .await
                            .map_err(|error| RelocationWorkerError::Finalize(error.to_string()))?;
                    } else {
                        self.source_free
                            .as_ref()
                            .ok_or_else(|| {
                                RelocationWorkerError::Finalize(
                                    "cross-domain source finalizer is unavailable".into(),
                                )
                            })?
                            .free_source(source)
                            .await
                            .map_err(RelocationWorkerError::Finalize)?;
                    }
                    self.checkpoint(ctx, dg, key, value, RelocationJournalPhase::SourceFreed)
                        .await?;
                    ctx.metrics.rebalance_moves_total.inc();
                }
                RelocationJournalPhase::SourceFreed | RelocationJournalPhase::Discarded => return Ok(()),
            }
        }
    }

    async fn checkpoint(
        &self,
        ctx: &BgCtx,
        dg: &Arc<DdbDiskGroup>,
        key: &RelocationJournalKey,
        value: &mut RelocationJournalValue,
        next: RelocationJournalPhase,
    ) -> Result<(), RelocationWorkerError> {
        value.phase = next.into();
        value.updated_at_ms = now_ms();
        value.last_error.clear();
        ctx.kv
            .put_relocation_journal(dg.bind(), key, value)
            .await
            .map_err(|error| RelocationWorkerError::Persistence(error.to_string()))
    }
}

pub struct DiskioRelocationIo {
    client: ArcSwapOption<DiskioClient>,
    service: ServiceRegistryClient,
    hardware: HardwareClient,
}

impl DiskioRelocationIo {
    #[must_use]
    pub fn new(service: ServiceRegistryClient, hardware: HardwareClient) -> Self {
        Self {
            client: ArcSwapOption::empty(),
            service,
            hardware,
        }
    }

    pub async fn refresh(&self) -> Result<(), RelocationWorkerError> {
        let client = self.client().await?;
        client.refresh().await.map(|_| ()).map_err(|error| {
            self.client.store(None);
            RelocationWorkerError::Io(error.to_string())
        })
    }

    async fn client(&self) -> Result<Arc<DiskioClient>, RelocationWorkerError> {
        if let Some(client) = self.client.load_full() {
            return Ok(client);
        }
        let client = Arc::new(
            DiskioClient::connect_with_clients(
                self.service.clone(),
                self.hardware.clone(),
                DiskioClientConfig::default(),
            )
            .await
            .map_err(|error| RelocationWorkerError::Io(error.to_string()))?,
        );
        self.client.store(Some(Arc::clone(&client)));
        Ok(client)
    }
}

impl RelocationIo for DiskioRelocationIo {
    fn copy_and_fsync<'a>(
        &'a self,
        source: Segment,
        target: Segment,
        unit_size: u32,
    ) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>> {
        Box::pin(async move {
            let client = self.client().await.map_err(|error| error.to_string())?;
            let source_target = io_target(source, unit_size)?;
            let target_target = io_target(target, unit_size)?;
            let length = u32::try_from(source_target.capacity())
                .map_err(|_| "relocation size exceeds u32".to_string())?;
            let options = client.normal_options().priority();
            let bytes: Bytes = client
                .read(source_target, 0, length, options)
                .await
                .map_err(|error| error.to_string())?;
            client
                .write(target_target, 0, bytes, Durability::Buffered, options)
                .await
                .map_err(|error| error.to_string())?;
            client
                .fsync(io_disk(target)?, options)
                .await
                .map_err(|error| error.to_string())
        })
    }
}

impl RelocationOwner for ChunkdbClient {
    fn handoff<'a>(
        &'a self,
        request: RelocateSegmentHandoffRequest,
    ) -> Pin<Box<dyn Future<Output = Result<RelocationHandoffDisposition, String>> + Send + 'a>> {
        Box::pin(async move {
            let response = self
                .relocate_segment_handoff(request)
                .await
                .map_err(|error| error.to_string())?;
            RelocationHandoffDisposition::try_from(response.disposition)
                .map_err(|()| "owner returned an unknown relocation disposition".to_string())
        })
    }
}

impl RelocationSourceFree for DiskdbClient {
    fn free_source<'a>(
        &'a self,
        source: Segment,
    ) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>> {
        Box::pin(async move {
            let response = self
                .free_blocks(FreeBlocksRequest {
                    segments: vec![source],
                })
                .await
                .map_err(|error| error.to_string())?;
            if response.freed_count == 1 && response.failures.is_empty() {
                Ok(())
            } else {
                Err("source DiskDB did not durably accept the exact free".into())
            }
        })
    }
}

fn journal_key(source: &Segment) -> Result<RelocationJournalKey, RelocationWorkerError> {
    Ok(RelocationJournalKey {
        disk_id: source.disk_id.ok_or(RelocationWorkerError::InvalidSource)?,
        zone_index: source.zone_index,
        unit_offset: source.unit_offset,
        allocation_ts: source.allocation_ts,
    })
}

fn validate_journal(
    key: &RelocationJournalKey,
    value: &RelocationJournalValue,
) -> Result<(), RelocationWorkerError> {
    let source = source(value)?;
    let target = target(value)?;
    if value.schema_version != RELOCATION_JOURNAL_SCHEMA_VERSION
        || value.operation_id != relocation_operation_id(&source)
        || value.owner_chunk.is_none()
        || source.owner_chunk != value.owner_chunk
        || target.owner_chunk != value.owner_chunk
        || value.target_disk_group_id == 0
        || source.unit_count == 0
        || source.unit_count != target.unit_count
        || value.unit_size == 0
        || *key != journal_key(&source)?
    {
        return Err(RelocationWorkerError::InvalidJournal(
            "identity or geometry mismatch".into(),
        ));
    }
    Ok(())
}

fn phase(value: &RelocationJournalValue) -> Result<RelocationJournalPhase, RelocationWorkerError> {
    RelocationJournalPhase::try_from(value.phase)
        .map_err(|()| RelocationWorkerError::InvalidJournal("unknown phase".into()))
}

fn source(value: &RelocationJournalValue) -> Result<Segment, RelocationWorkerError> {
    value
        .source
        .ok_or_else(|| RelocationWorkerError::InvalidJournal("missing source".into()))
}

fn target(value: &RelocationJournalValue) -> Result<Segment, RelocationWorkerError> {
    value
        .target
        .ok_or_else(|| RelocationWorkerError::InvalidJournal("missing target".into()))
}

fn io_target(segment: Segment, unit_size: u32) -> Result<SegmentTarget, String> {
    SegmentTarget::new(
        io_disk(segment)?,
        segment.zone_index,
        segment.unit_offset,
        segment.unit_count,
        unit_size,
    )
    .map_err(|error| error.to_string())
}

fn io_disk(segment: Segment) -> Result<IoDiskId, String> {
    segment
        .disk_id
        .map(|disk| IoDiskId::new(disk.high, disk.low))
        .ok_or_else(|| "segment has no disk ID".to_string())
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |duration| {
            u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
        })
}
