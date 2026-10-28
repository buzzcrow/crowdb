// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#[path = "../src/commands/bench/paxos_kv/counters.rs"]
mod counters;
#[path = "../src/commands/bench/paxos_kv/deployment/memory.rs"]
mod memory;
#[path = "../src/commands/bench/workload.rs"]
mod workload;
use serde_json::{json, Value};
use std::time::Duration;
use workload::{LoadOptions, Window};

#[test]
fn cgroup_memory_limit_accepts_unlimited_without_hiding_invalid_values() {
    assert_eq!(memory::parse_limit("max\n").unwrap(), None);
    assert_eq!(
        memory::parse_limit("21474836480\n").unwrap(),
        Some(21_474_836_480)
    );
    assert!(memory::parse_limit("").is_err());
    assert!(memory::parse_limit("unknown").is_err());
    assert!(memory::parse_limit("-1").is_err());
}

fn options() -> LoadOptions {
    LoadOptions {
        warmup_secs: 1,
        duration_secs: 1,
        tail_secs: 1,
        drain_timeout_secs: 1,
        loader_num: 2,
        output: None,
    }
}

#[tokio::test]
async fn continuous_load_separates_boundaries_and_drains() {
    let options = options();
    options.validate().unwrap();
    let result = workload::run_until(
        &options,
        Window::new(&options),
        |_, _| async {
            tokio::time::sleep(Duration::from_millis(120)).await;
            Ok(64)
        },
        std::future::pending(),
    )
    .await;
    assert!(result.errors.is_empty(), "{:?}", result.errors);
    assert_eq!(result.report["incomplete"], 0);
    assert!(result.report["boundary_crossings"].as_u64().unwrap() >= 4);
    let middle = &result.report["phases"][1];
    assert!(result.report["measured_operations"].as_u64().unwrap() < middle["completed"].as_u64().unwrap());
    for phase in result.report["phases"].as_array().unwrap() {
        assert_eq!(phase["admitted"], phase["completed"]);
    }
}

#[tokio::test]
async fn failures_in_warmup_and_tail_invalidate_run() {
    let options = options();
    let window = Window::new(&options);
    let result = workload::run_until(
        &options,
        window,
        move |_, _| async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            if window.phase(tokio::time::Instant::now()) == 1 {
                Ok(1)
            } else {
                Err("injected phase error".into())
            }
        },
        std::future::pending(),
    )
    .await;
    assert!(!result.errors.is_empty());
    assert!(result.report["phases"][0]["failed"].as_u64().unwrap() > 0);
    assert!(result.report["phases"][2]["failed"].as_u64().unwrap() > 0);
}

#[tokio::test]
async fn drain_timeout_preserves_incomplete_and_joins_workers() {
    let mut options = options();
    options.warmup_secs = 0;
    options.tail_secs = 0;
    let result = workload::run_until(
        &options,
        Window::new(&options),
        |_, _| async { std::future::pending::<Result<u64, String>>().await },
        std::future::pending(),
    )
    .await;
    assert_eq!(result.report["incomplete"], 2);
    assert!(result.errors.iter().any(|e| e.contains("drain timeout")));
}

#[tokio::test]
async fn worker_panics_are_failures() {
    let options = options();
    let result = workload::run_until(
        &options,
        Window::new(&options),
        |_, _| async {
            panic!("injected worker panic");
            #[allow(unreachable_code)]
            Ok(0)
        },
        std::future::pending(),
    )
    .await;
    assert_eq!(result.errors.len(), 2);
    assert_eq!(result.report["incomplete"], 2);
}

fn snapshot(elections: u64, completed: u64, failed: u64) -> Value {
    let points:Vec<_>=[("paxos.elections.c",elections),("maintenance.snapshot.success.c",completed),("maintenance.snapshot.failure.c",failed)]
        .into_iter().map(|(suffix,value)|json!({"name":format!("s.0.g.1.{suffix}"),"fields":[{"key":"total","value":value}]})).collect();
    json!({"metrics":[{"metrics":points},{"metrics":points},{"metrics":points}]})
}
#[test]
fn snapshot_sentinel_uses_deltas_and_rejects_missing_or_reset_counters() {
    let before = snapshot(2, 10, 1);
    assert_eq!(
        counters::sentinel(0, 1, &before, &snapshot(2, 11, 1)).unwrap()["passed"],
        true
    );
    for after in [snapshot(2, 10, 1), snapshot(3, 11, 1), snapshot(2, 11, 2)] {
        assert_eq!(
            counters::sentinel(0, 1, &before, &after).unwrap()["passed"],
            false
        );
    }
    assert!(counters::sentinel(0, 1, &before, &snapshot(1, 11, 1)).is_err());
    assert!(counters::sentinel(0, 1, &before, &json!({})).is_err());
    assert!(counters::deltas(&json!({}), &json!({})).is_err());
}

#[test]
fn container_bench_rejects_missing_deployment_and_console_target() {
    for (extra, message) in [
        (vec![], "three distinct --container"),
        (
            vec!["--cluster", "missing"],
            "bench paxos-kv uses container --endpoint",
        ),
    ] {
        let dir = crowdb_test_harness::test_dirs::tempdir_in_test_data("kv-bench-contract");
        let output = std::process::Command::new(env!("CARGO_BIN_EXE_crowdb-cli"))
            .env_remove("CROWDB_CLUSTER")
            .env("CROWDB_RUNTIME_ROOT", dir.path())
            .args(extra)
            .args(["bench", "paxos-kv", "read"])
            .output()
            .unwrap();
        assert_eq!(output.status.code(), Some(2));
        assert!(
            String::from_utf8_lossy(&output.stderr).contains(message),
            "{output:?}"
        );
    }
}

#[tokio::test]
async fn resource_guard_cancels_inflight_work_and_fails_the_run() {
    let options = options();
    let result = workload::run_until(
        &options,
        Window::new(&options),
        |_, _| async { std::future::pending::<Result<u64, String>>().await },
        async {
            tokio::time::sleep(Duration::from_millis(30)).await;
            "memory guard stopped load".to_owned()
        },
    )
    .await;
    assert_eq!(result.errors, ["memory guard stopped load"]);
    assert_eq!(result.report["incomplete"], 2);
    assert_eq!(result.report["measured_operations"], 0);
    assert!(result.report["elapsed_secs_including_drain"].as_f64().unwrap() < 1.0);
}

#[test]
fn native_bench_rejects_duplicate_pids_and_mixed_deployments() {
    for (extra, message) in [
        (
            vec!["--server-pid", "1", "--server-pid", "1", "--server-pid", "1"],
            "three distinct --server-pid",
        ),
        (
            vec!["--server-pid", "1", "--container", "node1"],
            "cannot be used with",
        ),
    ] {
        let dir = crowdb_test_harness::test_dirs::tempdir_in_test_data("kv-bench-native-contract");
        let output = std::process::Command::new(env!("CARGO_BIN_EXE_crowdb-cli"))
            .env_remove("CROWDB_CLUSTER")
            .env("CROWDB_RUNTIME_ROOT", dir.path())
            .args(["bench", "paxos-kv", "read"])
            .args(extra)
            .output()
            .unwrap();
        assert_eq!(output.status.code(), Some(2));
        assert!(
            String::from_utf8_lossy(&output.stderr).contains(message),
            "{output:?}"
        );
    }
}
