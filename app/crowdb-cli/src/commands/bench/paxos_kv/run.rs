// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::super::workload::{self, Window};
use super::{
    args::{Operation, PaxosKvArgs},
    client, collect, dataset, deployment,
};
use crate::Cli;
use crowdb_common::metrics::system::SystemCollector;
use serde_json::{json, Value};
use std::{process::ExitCode, sync::Arc, time::Duration};

pub async fn run(_cli: &Cli, args: PaxosKvArgs) -> ExitCode {
    let output = args.load.output.clone();
    let result = execute(Arc::new(args)).await;
    let (report, success) = match result {
        Ok(r) => {
            let ok = r["passed"] == true;
            (r, ok)
        }
        Err(e) => {
            eprintln!("bench paxos-kv failed: {e}");
            (json!({"passed":false,"error":e}), false)
        }
    };
    let mut report = report;
    if report.get("load").is_some() {
        report["history_line"] = json!(format!(
            "# {} {} {} ops/s={} latency_ns={} passed={}",
            chrono::Utc::now().format("%Y-%m-%d"),
            report["config"]["label"].as_str().unwrap_or("kv"),
            report["deployment_kind"].as_str().unwrap_or("host-container"),
            report["load"]["ops_per_sec"],
            report["load"]["latency_ns"],
            success
        ));
    }
    let text = serde_json::to_string_pretty(&report).expect("JSON value");
    if let Some(path) = output {
        if let Err(error) = std::fs::write(path, &text) {
            eprintln!("write benchmark report: {error}");
            return ExitCode::from(2);
        }
    }
    println!("{text}");
    if success {
        ExitCode::SUCCESS
    } else {
        ExitCode::from(2)
    }
}

fn validate(args: &PaxosKvArgs) -> Result<(), String> {
    args.load.validate()?;
    let size = if args.mixed_values { 16384 } else { args.value_size };
    if args.group == 0
        || args.key_space == 0
        || args.key_space > 10_000_000
        || size == 0
        || size > 1024 * 1024
        || args.dataset_bytes().map_or(true, |n| n > args.max_dataset_bytes)
        || args.connections == 0
        || args.rpc_workers == 0
        || args.send_queue_capacity == 0
        || args.scan_limit == 0
    {
        return Err("invalid group, dataset budget, value size or client capacity".into());
    }
    if matches!(args.operation, Operation::Scan)
        && args
            .key_space
            .min(u64::from(args.scan_limit))
            .checked_mul(size as u64 + 21)
            .and_then(|n| n.checked_mul(args.load.loader_num as u64))
            .map_or(true, |n| n > args.max_inflight_bytes)
    {
        return Err("scan responses exceed --max-inflight-bytes".into());
    }
    if args.require_snapshot && (!matches!(args.operation, Operation::Write) || args.value_size < 16384) {
        return Err("snapshot sentinel requires a write workload with values >=16KiB".into());
    }
    Ok(())
}

async fn execute(args: Arc<PaxosKvArgs>) -> Result<Value, String> {
    validate(&args)?;
    let containers = deployment::inspect(&args).await?;
    deployment::validate_budget(&args, &containers)?;
    let (client, topology) = client::connect(&args).await?;
    let client = Arc::new(client);
    eprintln!("bench paxos-kv: checking deterministic dataset before timing");
    prepare_dataset(client.clone(), args.clone(), &containers).await?;
    if matches!(args.operation, Operation::Prepare) {
        return Ok(json!({"passed":true,"operation":"prepare","keys":args.key_space}));
    }
    if matches!(args.operation, Operation::Scan) {
        dataset::scan(
            &client,
            &args,
            crowdb_paxos_kv_client::ReadMode::Linearizable,
            true,
        )
        .await?;
    }
    let before = collect::sample(&args, &containers).await?;
    let window = Window::new(&args.load);
    let operation_args = args.clone();
    let operation_client = client.clone();
    let load = workload::run_until(
        &args.load,
        window,
        move |worker, sequence| {
            let args = operation_args.clone();
            let client = operation_client.clone();
            async move {
                // Stateless SplitMix64 avoids shared RNG contention and repeating adjacent keys.
                let mut n = args
                    .seed
                    .wrapping_add((worker as u64) << 32)
                    .wrapping_add(sequence)
                    .wrapping_add(0x9e37_79b9_7f4a_7c15);
                n = (n ^ (n >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
                n = (n ^ (n >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
                let id = (n ^ (n >> 31)) % args.key_space;
                dataset::operation(&client, &args, id).await
            }
        },
        deployment::memory_guard(&containers),
    );
    let measurement = async {
        tokio::time::sleep_until(window.measure).await;
        let mut system = SystemCollector::new();
        let start = collect::sample(&args, &containers).await?;
        tokio::time::sleep_until(window.tail).await;
        let cli_system = system.collect();
        let end = collect::sample(&args, &containers).await?;
        Ok::<_, String>(
            json!({"delta":collect::deltas(&start,&end)?,"before":start,"after":end,"cli_system":cli_system,
            "note":"resource snapshots carry actual sampling timestamps; latency uses exact monotonic window; host TCP/DRAM are shared host metrics"}),
        )
    };
    eprintln!(
        "bench paxos-kv: warmup={}s measurement={}s tail={}s",
        args.load.warmup_secs, args.load.duration_secs, args.load.tail_secs
    );
    let (mut result, measurement) = tokio::join!(load, measurement);
    let metrics = match measurement {
        Ok(v) => v,
        Err(e) => {
            result.errors.push(e);
            Value::Null
        }
    };
    let (after, sentinel) = finish_collection(&args, &containers, &before, &mut result).await;
    verify_dataset(client, args.clone(), &mut result).await;
    let evidence: Vec<_> = containers.iter().map(|c| &c.evidence).collect();
    Ok(
        json!({"schema_version":1,"cli_version":env!("CARGO_PKG_VERSION"),"debug_build":cfg!(debug_assertions),"at":chrono::Utc::now(),"passed":result.errors.is_empty(),"errors":result.errors,
        "kind":if args.load.duration_secs>=20 {"benchmark"}else{"smoke"},"config":&*args,"load":result.report,
        "deployment_kind":if args.server_pids.is_empty() {"host-container"} else {"native-process"},
        "containers":if args.server_pids.is_empty() {json!(evidence)} else {json!([])},
        "processes":if args.server_pids.is_empty() {json!([])} else {json!(evidence)},"initial_topology":topology,"measurement_resources":metrics,
        "load_before":before,"load_after":after,"sentinel":sentinel,
        "dataset_policy":"bounded deterministic overwrite dataset; preparation and full verification outside timing",
        "host_cpu":std::fs::read_to_string("/proc/cpuinfo").ok().and_then(|s|s.lines().find(|l|l.starts_with("model name")).map(str::to_owned))}),
    )
}

async fn verify_dataset(
    client: Arc<crowdb_paxos_kv_client::PaxosKvClient>,
    args: Arc<PaxosKvArgs>,
    result: &mut workload::LoadResult,
) {
    eprintln!("bench paxos-kv: verifying all dataset bytes outside the timing window");
    let verification = tokio::time::timeout(
        Duration::from_secs(300),
        dataset::verify(client.clone(), args.clone()),
    )
    .await;
    match verification {
        Ok(Ok(())) => {}
        Ok(Err(e)) => result.errors.push(e),
        Err(_) => result.errors.push("verification timeout".into()),
    }
    if matches!(args.operation, Operation::Scan) {
        if let Err(error) = dataset::scan(
            &client,
            &args,
            crowdb_paxos_kv_client::ReadMode::Linearizable,
            true,
        )
        .await
        {
            result.errors.push(error);
        }
    }
}

async fn prepare_dataset(
    client: Arc<crowdb_paxos_kv_client::PaxosKvClient>,
    args: Arc<PaxosKvArgs>,
    containers: &[deployment::ServerProcess],
) -> Result<(), String> {
    let prepare = matches!(args.operation, Operation::Prepare | Operation::Write);
    let preparation = async {
        if prepare {
            dataset::prepare(client.clone(), args.clone()).await
        } else {
            dataset::verify(client.clone(), args.clone()).await
        }
    };
    tokio::select! {
        result = preparation => result?,
        error = deployment::memory_guard(containers) => return Err(error),
    }
    Ok(())
}

async fn finish_collection(
    args: &PaxosKvArgs,
    containers: &[deployment::ServerProcess],
    before: &Value,
    result: &mut workload::LoadResult,
) -> (Option<Value>, Value) {
    let after = collect::sample(args, containers).await;
    if let Ok(after) = after.as_ref() {
        if let Err(error) = collect::deltas(before, after) {
            result.errors.push(error);
        }
    }
    match deployment::inspect(args).await {
        Ok(current)
            if current.iter().zip(containers).all(|(a, b)| {
                a.evidence["id"] == b.evidence["id"] && a.evidence["started_at"] == b.evidence["started_at"]
            }) => {}
        Ok(_) => result
            .errors
            .push("server process incarnation changed during load".into()),
        Err(error) => result.errors.push(error),
    }
    if result.report["measured_operations"] == 0 {
        result
            .errors
            .push("no successful operations in measurement window".into());
    }
    let sentinel = match after.as_ref() {
        Ok(after) => match collect::sentinel(args.store, args.group, before, after) {
            Ok(v) => {
                if args.require_snapshot && v["passed"] != true {
                    result.errors.push("snapshot sentinel failed".into());
                }
                v
            }
            Err(e) => {
                if args.require_snapshot {
                    result.errors.push(e.clone());
                }
                json!({"unsupported":e})
            }
        },
        Err(e) => {
            result.errors.push(e.clone());
            Value::Null
        }
    };
    (after.ok(), sentinel)
}
