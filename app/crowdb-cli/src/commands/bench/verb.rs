// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! `bench` CLI verb definitions + dispatch. The heavy workload logic
//! lives in the per-workload sub-modules; this file only routes.

use std::process::ExitCode;

use clap::{Subcommand, ValueEnum};

use crate::Cli;

/// Workloads attached to a selected application cluster.
#[derive(Subcommand, Debug)]
pub enum BenchVerb {
    /// Load an already running three-container Paxos-KV deployment.
    PaxosKv(super::paxos_kv::args::PaxosKvArgs),
    /// Distributed disk-block allocation benchmark.
    #[command(subcommand)]
    Diskdb(DiskdbBenchVerb),
    /// Distributed chunk lifecycle benchmark.
    #[command(subcommand)]
    Chunkdb(ChunkdbBenchVerb),
    /// End-to-end chunk data IO benchmark.
    #[command(subcommand)]
    Chunkio(ChunkioBenchVerb),
    /// Authenticated S3 HTTP workloads on the selected cluster.
    #[command(subcommand)]
    S3(S3BenchVerb),
}

#[derive(Subcommand, Debug)]
pub enum S3BenchVerb {
    Write(S3Args),
    Read(S3Args),
    RangeRead(S3Args),
    List(S3Args),
    Mix(S3Args),
}

#[derive(clap::Args, Debug, Clone)]
pub struct S3Args {
    #[arg(long, default_value_t = 4 * 1024)]
    pub object_size: usize,
    #[arg(long, default_value_t = 128)]
    pub dataset_objects: usize,
    #[arg(long, default_value_t = 32)]
    pub concurrency: usize,
    /// Maximum measured operations; duration may stop admission first.
    #[arg(long, default_value_t = 60_000)]
    pub operations: u64,
    #[arg(long, default_value_t = 20)]
    pub duration_secs: u64,
    #[arg(long, default_value_t = 100)]
    pub warmup_operations: u64,
    #[arg(long, default_value_t = 1)]
    pub seed: u64,
    #[arg(long, default_value_t = 2 * 1024 * 1024 * 1024)]
    pub max_dataset_bytes: u64,
    #[arg(long, default_value_t = 100)]
    pub list_limit: usize,
    #[arg(long, default_value = "w20R70RR5L5")]
    pub ratio: String,
    /// Also write the JSON result to this path.
    #[arg(long)]
    pub output: Option<std::path::PathBuf>,
}

#[derive(Subcommand, Debug)]
pub enum ChunkioBenchVerb {
    /// Stream deterministic large objects through `ChunkDB` and `DiskIO`.
    Write(ChunkioArgs),
    /// Aggregate deterministic small objects through the shared write pool.
    WriteSmall(ChunkioSmallWriteArgs),
    /// Read small objects from real writer-produced locations.
    ReadSmall(ChunkioReadArgs),
    /// Read large objects from real writer-produced locations.
    ReadLarge(ChunkioReadArgs),
    /// Run a deterministic request mix of small and large object reads.
    ReadMix(ChunkioReadArgs),
}

#[derive(clap::Args, Debug, Clone)]
pub struct ChunkioArgs {
    /// Maximum admitted objects; duration is the normal stopping condition.
    #[arg(long, default_value_t = u64::MAX)]
    pub objects: u64,
    /// Stop admitting new objects after this many seconds, then drain writes.
    #[arg(long, default_value_t = 20)]
    pub duration_secs: u64,
    #[arg(long, default_value_t = 16 * 1024 * 1024)]
    pub object_size: u64,
    #[arg(long, default_value_t = 1)]
    pub concurrency: usize,
    /// Connections kept per discovered `DiskIO` endpoint.
    #[arg(long, default_value_t = 4)]
    pub diskio_connections: usize,
    /// RPC I/O workers serving `DiskIO` responses in this client.
    #[arg(long, default_value_t = 4)]
    pub diskio_rpc_workers: u32,
    #[arg(long, default_value_t = 1024 * 1024)]
    pub block_size: usize,
    #[arg(long, default_value_t = 1024 * 1024 * 1024)]
    pub chunk_size: u64,
    #[arg(long, default_value_t = 4)]
    pub data_num: usize,
    #[arg(long, default_value_t = 1)]
    pub code_num: usize,
    /// Use large mirror strips instead of EC, with the selected copy count.
    #[arg(long, value_parser = clap::value_parser!(u32).range(1..=5))]
    pub mirror_copies: Option<u32>,
    #[arg(long, default_value_t = 1)]
    pub seed: u8,
    #[arg(long, default_value_t = 1)]
    pub metrics_interval: u64,
    /// First chunks to prepare before benchmark timing starts.
    #[arg(long, default_value_t = 10)]
    pub prefetch_chunks: usize,
    /// Strips appended ahead within each active chunk.
    #[arg(long, default_value_t = 2)]
    pub prefetch_strips_per_chunk: usize,
    /// Send owned `Bytes` blocks directly, bypassing stream fetch assembly.
    #[arg(long, default_value_t = false)]
    pub direct_buffers: bool,
}

#[derive(clap::Args, Debug, Clone)]
pub struct ChunkioSmallWriteArgs {
    /// Maximum admitted objects; duration is the normal stopping condition.
    #[arg(long, default_value_t = u64::MAX)]
    pub objects: u64,
    /// Stop admitting new objects after this many seconds, then drain writes.
    #[arg(long, default_value_t = 20)]
    pub duration_secs: u64,
    /// Small object size in bytes (maximum 1 MiB).
    #[arg(long, default_value_t = 8 * 1024)]
    pub object_size: usize,
    #[arg(long, default_value_t = 32)]
    pub concurrency: usize,
    /// Connections kept per discovered `DiskIO` endpoint.
    #[arg(long, default_value_t = 4)]
    pub diskio_connections: usize,
    /// RPC I/O workers serving `DiskIO` responses in this client.
    #[arg(long, default_value_t = 4)]
    pub diskio_rpc_workers: u32,
    /// Maximum queue-driven pipelines; hard limit is 32.
    #[arg(long, default_value_t = 32)]
    pub max_pipelines: usize,
    /// Scale out when queued bytes reach this threshold.
    #[arg(long, default_value_t = 4 * 1024 * 1024)]
    pub scale_out_queue_bytes: usize,
    /// Scale out when queued object count reaches this threshold.
    #[arg(long, default_value_t = 128)]
    pub scale_out_queue_objects: usize,
    #[arg(long, default_value_t = 1024 * 1024)]
    pub max_batch_bytes: usize,
    #[arg(long, default_value_t = 1024)]
    pub max_batch_objects: usize,
    #[arg(long, default_value_t = 1)]
    pub seed: u8,
    #[arg(long, default_value_t = 1)]
    pub metrics_interval: u64,
}

#[derive(clap::Args, Debug, Clone)]
pub struct ChunkioReadArgs {
    /// Maximum admitted reads; duration is the normal stopping condition.
    #[arg(long, default_value_t = u64::MAX)]
    pub requests: u64,
    /// Stop admitting reads after this many seconds, then drain them.
    #[arg(long, default_value_t = 20)]
    pub duration_secs: u64,
    /// Reusable objects prepared per selected object class before timing.
    #[arg(long, default_value_t = 32)]
    pub dataset_objects: usize,
    #[arg(long, default_value_t = 32)]
    pub concurrency: usize,
    /// Connections kept per discovered `DiskIO` endpoint.
    #[arg(long, default_value_t = 4)]
    pub diskio_connections: usize,
    /// RPC I/O workers serving `DiskIO` responses in this client.
    #[arg(long, default_value_t = 4)]
    pub diskio_rpc_workers: u32,
    #[arg(long, default_value_t = 8 * 1024)]
    pub small_object_size: usize,
    #[arg(long, default_value_t = 16 * 1024 * 1024)]
    pub large_object_size: u64,
    /// Large-object percentage by request count for `read-mix`.
    #[arg(long, default_value_t = 50)]
    pub mixed_large_percent: u8,
    #[arg(long, default_value_t = 1024 * 1024)]
    pub block_size: usize,
    #[arg(long, default_value_t = 1024 * 1024 * 1024)]
    pub chunk_size: u64,
    #[arg(long, default_value_t = 4)]
    pub data_num: usize,
    #[arg(long, default_value_t = 1)]
    pub code_num: usize,
    /// Use large mirror strips instead of EC, with the selected copy count.
    #[arg(long, value_parser = clap::value_parser!(u32).range(1..=5))]
    pub mirror_copies: Option<u32>,
    #[arg(long, default_value_t = 1)]
    pub seed: u8,
    #[arg(long, default_value_t = 1)]
    pub metrics_interval: u64,
}

#[derive(Subcommand, Debug)]
pub enum ChunkdbBenchVerb {
    /// Allocate chunks until the time limit.
    Allocate(ChunkdbArgs),
    /// Run a deterministic lifecycle operation mix.
    Mix(ChunkdbArgs),
}

#[derive(clap::Args, Debug, Clone)]
pub struct ChunkdbArgs {
    #[arg(long, default_value_t = 10)]
    pub duration_secs: u64,
    #[arg(long, default_value_t = 4)]
    pub concurrency: usize,
    /// Connections kept per `ChunkDB` endpoint.
    #[arg(long, default_value_t = 1)]
    pub chunkdb_connections: usize,
    /// crowdb-rpc I/O workers used by the `ChunkDB` client transport.
    #[arg(long, default_value_t = 2)]
    pub chunkdb_client_rpc_workers: u32,
    #[arg(long, default_value_t = 1)]
    pub strip_count: u32,
    #[arg(long, default_value_t = 1024)]
    pub write_granularity_kb: u32,
    #[arg(long, value_enum, default_value_t = ChunkdbStripMode::Mirror)]
    pub strip_type: ChunkdbStripMode,
    #[arg(long, default_value_t = 3)]
    pub copy_count: u32,
    #[arg(long, default_value_t = 4)]
    pub data_num: u32,
    #[arg(long, default_value_t = 2)]
    pub code_num: u32,
    #[arg(long, default_value_t = 1)]
    pub seed: u64,
    /// Metrics flush interval in seconds. 0 disables the metrics log.
    #[arg(long, default_value_t = 1)]
    pub metrics_interval: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
pub enum ChunkdbStripMode {
    Mirror,
    Ec,
}

#[derive(Subcommand, Debug)]
pub enum DiskdbBenchVerb {
    /// Allocate until capacity exhaustion or the time limit.
    Allocate(DiskdbArgs),
    /// Run deterministic 70% allocate and 30% free traffic.
    Mix(DiskdbArgs),
}

#[derive(clap::Args, Debug, Clone)]
pub struct DiskdbArgs {
    /// Run duration in seconds.
    #[arg(long, default_value_t = 10)]
    pub duration_secs: u64,
    /// Concurrent workload tasks.
    #[arg(long, default_value_t = 4)]
    pub concurrency: usize,
    /// Connections kept per `DiskDB` endpoint.
    #[arg(long, default_value_t = 1)]
    pub diskdb_connections: usize,
    /// crowdb-rpc I/O workers used by the `DiskDB` client transport.
    #[arg(long, default_value_t = 2)]
    pub diskdb_client_rpc_workers: u32,
    /// Allocation size in disk units.
    #[arg(long, default_value_t = 1)]
    pub unit_count: u32,
    /// Blocks requested by each allocation RPC.
    #[arg(long, default_value_t = 1)]
    pub blocks_per_request: u32,
    /// Expected unit size, used for space verification.
    #[arg(long, default_value_t = 1_048_576)]
    pub unit_size_bytes: u64,
    /// Deterministic workload seed.
    #[arg(long, default_value_t = 1)]
    pub seed: u64,
    /// Metrics flush interval in seconds. 0 disables the metrics log.
    #[arg(long, default_value_t = 1)]
    pub metrics_interval: u64,
}

pub async fn run_bench_verb(cli: &Cli, verb: BenchVerb) -> ExitCode {
    match verb {
        BenchVerb::Diskdb(verb) => super::disk::db::run(cli, verb).await,
        BenchVerb::Chunkdb(verb) => super::chunk::run(cli, verb).await,
        BenchVerb::Chunkio(verb) => super::io::run(cli, verb).await,
        BenchVerb::S3(verb) => super::s3::run(cli, verb).await,
        BenchVerb::PaxosKv(args) => super::paxos_kv::run(cli, args).await,
    }
}
