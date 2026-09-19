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
    println!("{}", String::from_utf8_lossy(&json));
    Ok(())
}
