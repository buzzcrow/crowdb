// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Thin CLI adapter for the library-owned chunk IO benchmarks.

use std::process::ExitCode;
use std::sync::Arc;
use std::time::Duration;

use crowdb_chunk_client::{
    run_large_write_benchmark, run_read_benchmark, run_small_write_benchmark, ChunkClientConfig,
    ChunkIoClient, ChunkIoClientConfig, LargeWriteBenchmarkConfig, LargeWriteBenchmarkResult,
    LargeWritePolicy, ReadBenchmarkConfig, ReadBenchmarkResult, ReadBenchmarkWorkload,
    SmallWriteBenchmarkConfig, SmallWriteBenchmarkResult, SmallWritePolicy,
};
use crowdb_common::ec::EcScheme;

use super::metrics::BenchMetrics;
use super::verb::{ChunkioArgs, ChunkioBenchVerb, ChunkioReadArgs, ChunkioSmallWriteArgs};
use crate::Cli;

pub async fn run(cli: &Cli, verb: ChunkioBenchVerb) -> ExitCode {
    match verb {
        ChunkioBenchVerb::Write(args) => run_large_write(cli, args).await,
        ChunkioBenchVerb::WriteSmall(args) => run_small_write(cli, args).await,
        ChunkioBenchVerb::ReadSmall(args) => run_read(cli, args, ReadBenchmarkWorkload::Small).await,
        ChunkioBenchVerb::ReadLarge(args) => run_read(cli, args, ReadBenchmarkWorkload::Large).await,
        ChunkioBenchVerb::ReadMix(args) => run_read(cli, args, ReadBenchmarkWorkload::Mixed).await,
    }
}

async fn connect(
    cli: &Cli,
    small_write: SmallWritePolicy,
    diskio_connections_per_endpoint: usize,
    diskio_rpc_workers: u32,
) -> Result<ChunkIoClient, ExitCode> {
    let config = crate::commands::load_config(cli)?;
    let mut seeds = vec![format!("http://{}:{}", cli.sysmd_ip, cli.sysmd_port)];
    for server in config
        .servers
        .iter()
        .filter(|server| server.service_type == crowdb_console_shared::config::ServiceType::Kv)
    {
        if !seeds.contains(&server.url) {
            seeds.push(server.url.clone());
        }
    }
    ChunkIoClient::connect(ChunkIoClientConfig {
        management_seeds: seeds,
        diskio_connections_per_endpoint,
        diskio_rpc_workers,
        small_write,
    })
    .await
    .map_err(|error| {
        eprintln!("chunkio discovery failed: {error}");
        ExitCode::FAILURE
    })
}

fn large_policy(
    object_chunk_size: u64,
    block_size: usize,
    data_num: usize,
    code_num: usize,
) -> LargeWritePolicy {
    LargeWritePolicy {
        ec_scheme: EcScheme::new(data_num, code_num),
        client: Arc::new(ChunkClientConfig {
            max_chunk_size: object_chunk_size,
            read_buffer_size: block_size,
            max_cached_buffer: block_size.saturating_mul(data_num),
            ..ChunkClientConfig::default()
        }),
    }
}

async fn run_large_write(cli: &Cli, args: ChunkioArgs) -> ExitCode {
    if args.objects == 0
        || args.duration_secs == 0
        || args.object_size == 0
        || args.concurrency == 0
        || args.diskio_connections == 0
        || args.diskio_rpc_workers == 0
        || args.block_size == 0
        || args.data_num == 0
        || args.code_num == 0
    {
        eprintln!("chunkio benchmark values must be non-zero");
        return ExitCode::from(2);
    }
    let client = match connect(
        cli,
        SmallWritePolicy::default(),
        args.diskio_connections,
        args.diskio_rpc_workers,
    )
    .await
    {
        Ok(client) => client,
        Err(code) => return code,
    };
    let mut policy = large_policy(args.chunk_size, args.block_size, args.data_num, args.code_num);
    Arc::make_mut(&mut policy.client).prefetch_strips_per_chunk = args.prefetch_strips_per_chunk;
    let mut metrics = BenchMetrics::new(&cli.log_dir, args.metrics_interval);
    let client = client.with_metrics(&metrics.chunk_io);
    metrics.start();
    let result = run_large_write_benchmark(
        client,
        LargeWriteBenchmarkConfig {
            object_count: args.objects,
            duration: Some(Duration::from_secs(args.duration_secs)),
            object_size: args.object_size,
            concurrency: args.concurrency,
            seed: args.seed,
            prefetch_chunks: args.prefetch_chunks,
            direct_buffers: args.direct_buffers,
            policy,
        },
    )
    .await;
    metrics.stop().await;
    if !output(cli, &result, || print_large_write(&args, &result)) {
        return ExitCode::FAILURE;
    }
    success(result.errors, result.incomplete_objects)
}

async fn run_small_write(cli: &Cli, args: ChunkioSmallWriteArgs) -> ExitCode {
    if args.objects == 0
        || args.duration_secs == 0
        || args.object_size == 0
        || args.object_size > 1024 * 1024
        || args.concurrency == 0
        || args.diskio_connections == 0
        || args.diskio_rpc_workers == 0
        || args.max_pipelines == 0
        || args.max_pipelines > 32
        || args.scale_out_queue_bytes == 0
        || args.scale_out_queue_objects == 0
        || args.max_batch_bytes == 0
        || args.max_batch_objects == 0
    {
        eprintln!("invalid chunkio small-write benchmark values");
        return ExitCode::from(2);
    }
    let small_write = SmallWritePolicy {
        min_pipelines: 1,
        max_pipelines: args.max_pipelines,
        scale_out_queue_bytes: args.scale_out_queue_bytes,
        scale_out_queue_objects: args.scale_out_queue_objects,
        max_batch_bytes: args.max_batch_bytes,
        max_batch_objects: args.max_batch_objects,
        conversion_enabled: !args.mirror_only,
        ..SmallWritePolicy::default()
    };
    if let Err(error) = small_write.validate() {
        eprintln!("invalid chunkio small-write policy: {error}");
        return ExitCode::from(2);
    }
    let client = match connect(cli, small_write, args.diskio_connections, args.diskio_rpc_workers).await {
        Ok(client) => client,
        Err(code) => return code,
    };
    let mut metrics = BenchMetrics::new(&cli.log_dir, args.metrics_interval);
    let client = client.with_metrics(&metrics.chunk_io);
    metrics.start();
    let result = run_small_write_benchmark(
        client,
        SmallWriteBenchmarkConfig {
            object_count: args.objects,
            duration: Some(Duration::from_secs(args.duration_secs)),
            object_size: args.object_size,
            concurrency: args.concurrency,
            seed: args.seed,
        },
    )
    .await;
    metrics.stop().await;
    if !output(cli, &result, || print_small_write(&args, &result)) {
        return ExitCode::FAILURE;
    }
    success(result.errors, result.incomplete_objects)
}

async fn run_read(cli: &Cli, args: ChunkioReadArgs, workload: ReadBenchmarkWorkload) -> ExitCode {
    if args.requests == 0
        || args.duration_secs == 0
        || args.dataset_objects == 0
        || args.concurrency == 0
        || args.diskio_connections == 0
        || args.diskio_rpc_workers == 0
        || args.small_object_size == 0
        || args.small_object_size > 1024 * 1024
        || args.large_object_size == 0
        || args.block_size == 0
        || args.data_num == 0
        || args.code_num == 0
        || args.mixed_large_percent > 100
    {
        eprintln!("invalid chunkio read benchmark values");
        return ExitCode::from(2);
    }
    let client = match connect(
        cli,
        SmallWritePolicy::default(),
        args.diskio_connections,
        args.diskio_rpc_workers,
    )
    .await
    {
        Ok(client) => client,
        Err(code) => return code,
    };
    let mut metrics = BenchMetrics::new(&cli.log_dir, args.metrics_interval);
    let client = client.with_metrics(&metrics.chunk_io);
    metrics.start();
    let result = run_read_benchmark(
        client,
        ReadBenchmarkConfig {
            request_count: args.requests,
            duration: Some(Duration::from_secs(args.duration_secs)),
            dataset_objects: args.dataset_objects,
            concurrency: args.concurrency,
            small_object_size: args.small_object_size,
            large_object_size: args.large_object_size,
            mixed_large_percent: args.mixed_large_percent,
            seed: args.seed,
            workload,
            large_policy: large_policy(args.chunk_size, args.block_size, args.data_num, args.code_num),
        },
    )
    .await;
    metrics.stop().await;
    if !output(cli, &result, || print_read(workload, &args, &result)) {
        return ExitCode::FAILURE;
    }
    success(result.errors, result.incomplete_reads)
}

fn output<T: serde::Serialize>(cli: &Cli, result: &T, print_text: impl FnOnce()) -> bool {
    if cli.json {
        match serde_json::to_string_pretty(result) {
            Ok(json) => {
                println!("{json}");
                true
            }
            Err(error) => {
                eprintln!("encode chunkio result: {error}");
                false
            }
        }
    } else {
        print_text();
        true
    }
}

fn success(errors: u64, incomplete: u64) -> ExitCode {
    if errors == 0 && incomplete == 0 {
        ExitCode::SUCCESS
    } else {
        ExitCode::FAILURE
    }
}

fn dram(value: Option<f64>) -> String {
    value.map_or_else(|| "unsupported".to_string(), |value| format!("{value:.1}"))
}

fn print_large_write(args: &ChunkioArgs, result: &LargeWriteBenchmarkResult) {
    println!(
        "chunkio write: requested={} object_size={} prefetch_chunks={} prepare_s={:.3} objects={} errors={} incomplete={} stop={} objects_s={:.2} logical_mib_s={:.1} physical_mib_s={:.1} p50_us={} p99_us={} prep_stalls={} prep_stall_us={} dram_read_mib_s={} dram_write_mib_s={} dram_total_mib_s={}",
        result.requested_objects, args.object_size, args.prefetch_chunks, result.preparation_secs,
        result.objects, result.errors, result.incomplete_objects, result.stop_reason,
        result.objects_per_sec, result.logical_mib_per_sec, result.physical_mib_per_sec,
        result.latency_p50_us, result.latency_p99_us, result.preparation_stalls,
        result.preparation_stall_us, dram(result.dram_read_mib_s),
        dram(result.dram_write_mib_s), dram(result.dram_total_mib_s),
    );
    println!(
        "chunkio flow: source_reads={} source_read_us={} assembly_copies={} assembly_copy_bytes={} assembly_copy_us={} ec_encode_us={} completion_wait_us={}",
        result.source_reads, result.source_read_us, result.assembly_copies,
        result.assembly_copy_bytes, result.assembly_copy_us, result.ec_encode_us,
        result.completion_wait_us,
    );
    print_errors("write", &result.error_messages);
}

fn print_small_write(args: &ChunkioSmallWriteArgs, result: &SmallWriteBenchmarkResult) {
    println!(
        "chunkio write-small: requested={} object_size={} objects={} errors={} incomplete={} stop={} objects_s={:.2} logical_mib_s={:.1} p50_us={} p90_us={} p95_us={} p99_us={} max_us={} batches={} max_batch_objects={} max_batch_bytes={} batch_watchdog_expirations={} aggregate_write_requests={} aggregate_write_objects={} aggregate_write_buffers={} aggregate_write_logical_bytes={} aggregate_write_payload_bytes={} max_objects_per_write_request={} max_buffers_per_write_request={} avg_batch_fill_ppm={} max_queue_delay_us={} active_pipelines={} max_active_pipelines={} draining_pipelines={} scale_out={} scale_in={} tail_waste_bytes={} foreground_parity_bytes={} reservation_requests={} reservation_wait_us={} first_reservation_us={} dram_read_mib_s={} dram_write_mib_s={} dram_total_mib_s={}",
        result.requested_objects, args.object_size, result.objects, result.errors,
        result.incomplete_objects, result.stop_reason, result.objects_per_sec,
        result.logical_mib_per_sec, result.latency_p50_us, result.latency_p90_us,
        result.latency_p95_us, result.latency_p99_us, result.latency_max_us,
        result.batches, result.max_batch_objects, result.max_batch_bytes,
        result.batch_watchdog_expirations,
        result.aggregate_write_requests, result.aggregate_write_objects,
        result.aggregate_write_buffers, result.aggregate_write_logical_bytes,
        result.aggregate_write_payload_bytes, result.max_objects_per_write_request,
        result.max_buffers_per_write_request,
        result.average_batch_fill_ppm, result.max_queue_delay_us, result.active_pipelines,
        result.max_active_pipelines, result.draining_pipelines, result.scale_out,
        result.scale_in, result.tail_waste_bytes, result.foreground_parity_bytes,
        result.reservation_requests, result.reservation_wait_us,
        result.first_reservation_us,
        dram(result.dram_read_mib_s), dram(result.dram_write_mib_s),
        dram(result.dram_total_mib_s),
    );
    print_errors("write-small", &result.error_messages);
}

fn print_read(workload: ReadBenchmarkWorkload, args: &ChunkioReadArgs, result: &ReadBenchmarkResult) {
    let name = match workload {
        ReadBenchmarkWorkload::Small => "read-small",
        ReadBenchmarkWorkload::Large => "read-large",
        ReadBenchmarkWorkload::Mixed => "read-mix",
    };
    println!(
        "chunkio {name}: requested={} dataset_objects={} prepare_s={:.3} reads={} small_reads={} large_reads={} errors={} incomplete={} stop={} reads_s={:.2} logical_mib_s={:.1} logical_bytes={} p50_us={} p99_us={} dram_read_mib_s={} dram_write_mib_s={} dram_total_mib_s={}",
        result.requested_reads, args.dataset_objects, result.preparation_secs, result.reads,
        result.small_reads, result.large_reads, result.errors, result.incomplete_reads,
        result.stop_reason, result.reads_per_sec, result.logical_mib_per_sec,
        result.logical_bytes, result.latency_p50_us, result.latency_p99_us,
        dram(result.dram_read_mib_s), dram(result.dram_write_mib_s),
        dram(result.dram_total_mib_s),
    );
    print_errors(name, &result.error_messages);
}

fn print_errors(workload: &str, messages: &[String]) {
    for message in messages {
        eprintln!("chunkio {workload} error: {message}");
    }
}
