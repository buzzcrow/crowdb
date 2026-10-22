// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Shared TCP/RDMA workload parameters and explicit native configuration.

use std::path::PathBuf;

use crowdb_rpc_ffi::RdmaConfig;
use serde::Serialize;

use clap::ValueEnum;

#[derive(clap::Args, Debug, Serialize)]
pub struct RpcArgs {
    /// Emit the structured result on stdout.
    #[arg(long)]
    pub json: bool,
    /// Require RDMA and disable TCP fallback.
    #[arg(long)]
    pub rdma: bool,
    /// Measured completion window, excluding warmup and drain.
    #[arg(long, visible_alias = "measure-secs", default_value_t = 10)]
    pub duration_secs: u64,
    #[arg(long, default_value_t = 1)]
    pub warmup_secs: u64,
    /// One outstanding RPC per coroutine loader.
    #[arg(long, visible_alias = "loader-num", default_value_t = 1)]
    pub loader_count: u32,
    /// Independent TCP connections or RDMA queue pairs.
    #[arg(long, visible_aliases = ["connections", "qpair-count"], default_value_t = 1)]
    pub connection_count: u32,
    #[arg(long, visible_alias = "value-size", default_value_t = 128)]
    pub payload_bytes: usize,
    #[arg(long, default_value_t = 1)]
    pub io_engines: u32,
    #[arg(long, default_value_t = 1)]
    pub io_workers: u32,
    #[arg(long, value_enum, default_value_t = BenchMode::Coroutine)]
    pub mode: BenchMode,
    #[arg(long)]
    pub server_port: u16,
    #[arg(long, default_value = "127.0.0.1")]
    pub server_host: String,
    #[arg(long)]
    pub enable_nagle: bool,
    #[arg(long, default_value_t = 1)]
    pub metrics_interval: u64,
    #[arg(long, default_value_t = 5)]
    pub request_timeout_secs: u64,
    /// Owned canonical JSON file; stdout remains human readable.
    #[arg(long)]
    pub output: Option<PathBuf>,
    #[arg(long, default_value = "manual")]
    pub case_id: String,
    #[arg(long, default_value_t = 1)]
    pub repeat_index: u32,
    #[command(flatten)]
    #[serde(flatten)]
    pub queues: QueueArgs,
}

#[derive(clap::Args, Debug, Serialize)]
pub struct QueueArgs {
    #[arg(long, default_value = "")]
    pub device: String,
    #[arg(long, default_value = "")]
    pub source_address: String,
    #[arg(long, default_value = "")]
    pub gid: String,
    #[arg(long, default_value_t = 1)]
    pub hca_port: u32,
    #[arg(long, default_value_t = 1)]
    pub cq_count: u32,
    /// Zero derives enough entries for every QP's SQ and posted RQ.
    #[arg(long, default_value_t = 0)]
    pub cq_depth: u32,
    #[arg(long, default_value_t = 1024)]
    pub sq_depth: u32,
    #[arg(long, default_value_t = 1024)]
    pub rq_depth: u32,
    #[arg(long, default_value_t = 1024)]
    pub posted_receive_target: u32,
    #[arg(long, default_value_t = 1024)]
    pub wire_slot_capacity: u32,
    /// Zero derives `sq_depth * wire_slot_capacity` per QP.
    #[arg(long, default_value_t = 0)]
    pub send_arena_bytes: u64,
    /// Zero derives `posted_receive_target * wire_slot_capacity` per QP.
    #[arg(long, default_value_t = 0)]
    pub receive_arena_bytes: u64,
    #[arg(long, default_value_t = 0)]
    pub registered_memory_budget_bytes: u64,
    #[arg(long, default_value_t = 256)]
    pub poll_batch: u32,
    #[arg(long, default_value_t = 32)]
    pub verbs_poll_batch: u32,
    #[arg(long, default_value_t = 256)]
    pub poll_empty_budget: u32,
    #[arg(long, default_value_t = 4096)]
    pub send_queue_capacity: u32,
    #[arg(long)]
    pub aggregation_enabled: bool,
    /// Zero uses `wire_slot_capacity`; peer admission may lower this further.
    #[arg(long, default_value_t = 0)]
    pub aggregation_max_bytes: u32,
    #[arg(long, default_value_t = 256)]
    pub aggregation_max_frames: u32,
}

impl RpcArgs {
    pub(super) fn resolve(&mut self, rdma: bool) -> Result<RdmaConfig, String> {
        if self.duration_secs == 0
            || self.duration_secs > 3600
            || self.warmup_secs > 3600
            || self.loader_count == 0
            || self.loader_count > (1 << 20)
            || self.connection_count == 0
            || self.connection_count > self.loader_count
            || self.io_engines == 0
            || self.io_engines > 32
            || self.io_workers == 0
            || self.io_workers > 32
            || self.io_workers % self.io_engines != 0
            || self.request_timeout_secs == 0
            || self.request_timeout_secs > 60
            || self.payload_bytes > (1 << 20)
        {
            return Err("invalid duration, loader/connection count, workers, timeout or payload size".into());
        }
        let q = &mut self.queues;
        if q.cq_count == 0 || q.cq_count > 256 {
            return Err("cq_count must be in 1..=256".into());
        }
        if q.cq_depth == 0 {
            q.cq_depth = self
                .connection_count
                .div_ceil(q.cq_count)
                .checked_mul(
                    q.sq_depth
                        .checked_add(q.posted_receive_target)
                        .ok_or("queue entry overflow")?,
                )
                .ok_or("cq_depth overflow")?;
        }
        if q.send_arena_bytes == 0 {
            q.send_arena_bytes = u64::from(q.sq_depth) * u64::from(q.wire_slot_capacity);
        }
        if q.receive_arena_bytes == 0 {
            q.receive_arena_bytes = u64::from(q.posted_receive_target) * u64::from(q.wire_slot_capacity);
        }
        if q.aggregation_max_bytes == 0 {
            q.aggregation_max_bytes = q.wire_slot_capacity;
        }
        Ok(RdmaConfig {
            enabled: rdma,
            allow_tcp_fallback: false,
            listen_enabled: false,
            device: q.device.clone(),
            source_address: q.source_address.clone(),
            gid: q.gid.clone(),
            port: q.hca_port,
            cq_count: q.cq_count,
            cq_depth: q.cq_depth,
            sq_depth: q.sq_depth,
            rq_depth: q.rq_depth,
            posted_receive_target: q.posted_receive_target,
            wire_slot_capacity: q.wire_slot_capacity,
            wire_layout: crowdb_rpc_ffi::RdmaWireLayout::Contiguous,
            send_arena_bytes: q.send_arena_bytes,
            receive_arena_bytes: q.receive_arena_bytes,
            registered_memory_budget_bytes: q.registered_memory_budget_bytes,
            poll_batch: q.poll_batch,
            verbs_poll_batch: q.verbs_poll_batch,
            poll_empty_budget: q.poll_empty_budget,
            send_queue_capacity: q.send_queue_capacity,
            aggregation_enabled: q.aggregation_enabled,
            aggregation_max_bytes: q.aggregation_max_bytes,
            aggregation_max_frames: q.aggregation_max_frames,
            ..RdmaConfig::default()
        })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum BenchMode {
    Coroutine,
    Tokio,
}
