// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Thin CLI adapter for the shared memory-backed S3 benchmark.

use std::process::ExitCode;
use std::time::Duration;

use crowdb_console_shared::ops::s3_bench::{self, MixWeights, S3BenchConfig, S3BenchWorkload};

use super::verb::{S3Args, S3BenchVerb};
use crate::Cli;

pub async fn run(_cli: &Cli, verb: S3BenchVerb) -> ExitCode {
    let (workload, args) = match verb {
        S3BenchVerb::Write(args) => (S3BenchWorkload::Write, args),
        S3BenchVerb::Read(args) => (S3BenchWorkload::Read, args),
        S3BenchVerb::RangeRead(args) => (S3BenchWorkload::RangeRead, args),
        S3BenchVerb::List(args) => (S3BenchWorkload::List, args),
        S3BenchVerb::Mix(args) => (S3BenchWorkload::Mix, args),
    };
    match execute(workload, args).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("error: {error}");
            ExitCode::from(2)
        }
    }
}

async fn execute(workload: S3BenchWorkload, args: S3Args) -> crowdb_console_shared::error::Result<()> {
    let mix = MixWeights::parse(&args.ratio)?;
    let output = args.output.clone();
    let result = s3_bench::run(S3BenchConfig {
        work_dir: args.work_dir,
        workload,
        object_size: args.object_size,
        dataset_objects: args.dataset_objects,
        concurrency: args.concurrency,
        operations: args.operations,
        duration: Duration::from_secs(args.duration_secs),
        warmup_operations: args.warmup_operations,
        seed: args.seed,
        memory_budget_bytes: args.memory_budget_bytes,
        list_limit: args.list_limit,
        mix,
    })
    .await?;
    let json = serde_json::to_vec_pretty(&result)
        .map_err(|error| crowdb_console_shared::error::Error::Config(error.to_string()))?;
    if let Some(path) = output {
        std::fs::write(path, &json)?;
    }
    println!("=== S3 benchmark ===");
    println!(
        "  workload: {}  operations: {}  errors: {}  duration: {}ms  ops/s: {}",
        result.workload,
        result.total_operations,
        result.total_errors,
        result.duration_ms,
        result.operations_per_second
    );
    println!(
        "  object_size: {}  dataset_objects: {}  peak_resident: {} / {} bytes",
        result.object_size, result.dataset_objects, result.peak_resident_bytes, result.memory_budget_bytes
    );
    println!(
        "  backing: kv={} wal={} diskio={} chunk-kv={}",
        result.backing.kv, result.backing.wal, result.backing.diskio, result.backing.chunk_kv
    );
    if result.total_errors > 0 {
        println!(
            "  failures: metadata={} protocol={} transport={} resource={}",
            result.failures.metadata,
            result.failures.protocol,
            result.failures.transport,
            result.failures.resource
        );
    }
    for (name, stats) in [
        ("write", result.by_operation.write.as_ref()),
        ("read", result.by_operation.read.as_ref()),
        ("range-read", result.by_operation.range_read.as_ref()),
        ("list", result.by_operation.list.as_ref()),
    ] {
        if let Some(stats) = stats {
            println!(
                "  {name}: attempts={} successes={} failures={} avg={}us p50={}us p99={}us",
                stats.attempts, stats.successes, stats.failures, stats.average_us, stats.p50_us, stats.p99_us
            );
        }
    }
    Ok(())
}
