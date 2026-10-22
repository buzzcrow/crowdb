// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Shared timed echo benchmark for explicit TCP and strict RDMA transports.

mod control;
mod report;
mod runtime;
mod status;
mod workload;

use super::args::{BenchMode, RpcArgs};

use std::process::ExitCode;
use std::sync::Arc;
use std::time::Duration;

use crowdb_common::logging::open_named_log;
use crowdb_common::metrics::{MetricsRegistry, MetricsRunner};
use crowdb_rpc_ffi::RdmaConfig;
use serde_json::{json, Value};

use std::path::Path;
use workload::EchoWorkload;

pub async fn run(log_dir: &Path, mut args: RpcArgs) -> ExitCode {
    let rdma = args.rdma;
    let config = args.resolve(rdma);
    let mut report = report::base(&args, rdma);
    match config {
        Ok(config) => {
            if let Err(error) = run_case(log_dir, &args, config, rdma, &mut report).await {
                report::fail(&mut report, &error);
            }
        }
        Err(error) => report::fail(&mut report, &error),
    }
    report["client_exit"] = json!(u8::from(report["valid"] != true));
    if let Some(path) = &args.output {
        let result = serde_json::to_vec_pretty(&report)
            .map_err(std::io::Error::other)
            .and_then(|bytes| std::fs::write(path, bytes));
        if let Err(error) = result {
            report::fail(&mut report, &format!("write result {}: {error}", path.display()));
            report["report_errors"] = json!(1);
        }
    }
    if args.json {
        println!("{report}");
    } else {
        println!(
            "bench {}: completed={} TPS={} avg_ns={} p90_ns={} p99_ns={} errors={} valid={}",
            if rdma { "rdma" } else { "rpc" },
            report["completed_in_window"],
            report["rpc_tps"],
            report["avg_ns"],
            report["p90_ns"],
            report["p99_ns"],
            report["errors_total"],
            report["valid"]
        );
    }
    if report["valid"] == true {
        ExitCode::SUCCESS
    } else {
        eprintln!("error: invalid benchmark result: {report}");
        ExitCode::FAILURE
    }
}

async fn run_case(
    log_dir: &Path,
    args: &RpcArgs,
    config: RdmaConfig,
    rdma: bool,
    report: &mut Value,
) -> Result<(), String> {
    let session = runtime::Session::new(args, &config)?;
    let connections = session
        .conns
        .iter()
        .map(|conn| {
            let bytes = conn.transport_status_json().map_err(|e| e.to_string())?;
            serde_json::from_slice(&bytes).map_err(|e| e.to_string())
        })
        .collect::<Result<Vec<_>, String>>()?;
    let owners = report::paths(report, &connections, rdma)?;
    runtime::validate_wire_bound(args, &report["client_connections"], rdma)?;
    report["server_before"] = control::remote_status(args).await?;
    report["client_before"] = control::local_status(&session.server)?;
    status::validate(&report["client_before"], rdma)?;
    status::validate(&report["server_before"], rdma)?;
    let before = session.client.counters();
    let mut runner = metrics_runner(log_dir, args)?;
    let handler = create_workload(args, &owners, runner.as_ref());
    if let Some(runner) = &mut runner {
        runner.start();
    }
    let native = runtime::drive(args, &session, handler.clone()).await?;
    session.client.stop_reaper();
    if let Some(runner) = &mut runner {
        runner.stop().await;
    }
    let mut counters = session.client.counters();
    counters.submit_fail = counters.submit_fail.saturating_sub(before.submit_fail);
    counters.submit_retry = counters.submit_retry.saturating_sub(before.submit_retry);
    counters.resp_missed = counters.resp_missed.saturating_sub(before.resp_missed);
    counters.reaped = counters.reaped.saturating_sub(before.reaped);
    report["client_after"] = control::local_status(&session.server)?;
    report["server_after"] = control::remote_status(args).await?;
    report::finish(report, args, &handler, &native, &counters);
    status::validate(&report["client_after"], rdma)?;
    status::validate(&report["server_after"], rdma)?;
    if session.conns.iter().any(|conn| !conn.is_open()) {
        return Err("workload connection closed before final status collection".into());
    }
    if args.mode == BenchMode::Tokio {
        report["latency_scope"] =
            json!("Rust pre-call to awaited response; Tokio scheduling included; validation excluded");
    }
    Ok(())
}

fn create_workload(args: &RpcArgs, owners: &[usize], runner: Option<&MetricsRunner>) -> Arc<EchoWorkload> {
    let create = |registry: &mut MetricsRegistry| {
        Arc::new(EchoWorkload::new(
            args.loader_count,
            owners,
            args.payload_bytes,
            Duration::from_secs(args.warmup_secs),
            Duration::from_secs(args.duration_secs),
            registry,
        ))
    };
    if let Some(runner) = runner {
        create(&mut runner.registry().lock().expect("metrics registry"))
    } else {
        create(&mut MetricsRegistry::new())
    }
}

fn metrics_runner(log_dir: &Path, args: &RpcArgs) -> Result<Option<MetricsRunner>, String> {
    if args.metrics_interval == 0 {
        return Ok(None);
    }
    let file = open_named_log(log_dir, "rpc-test-metrics", 50, 1)?;
    let mut runner = MetricsRunner::new(file, args.metrics_interval);
    runner.set_cpp_flush(|writer, seconds, stamp, width, count_w, tps_w| {
        let width = width.max(crowdb_rpc_ffi::cpp_global_metrics_max_name_len());
        if let Some(text) =
            crowdb_rpc_ffi::flush_cpp_global_metrics(seconds, stamp, "cpp-rpc", width, count_w, tps_w)
        {
            let _ = std::io::Write::write_all(writer, text.as_bytes());
        }
    });
    Ok(Some(runner))
}
