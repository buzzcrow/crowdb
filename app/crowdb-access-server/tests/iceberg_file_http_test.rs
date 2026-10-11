// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[path = "common/iceberg_stack.rs"]
#[allow(dead_code)]
mod common;
#[path = "common/iceberg_process.rs"]
#[allow(dead_code)]
mod process;

use common::{now_ms, TestIcebergStack};
use crowdb_access_iceberg::{
    catalog::{CatalogRepository, ClearBounds, ManagementPrivilege},
    key::OperationId,
    operation::{ManagementAction, ManagementRequest, RequestIdentity},
};

async fn setup() -> (TestIcebergStack, process::TestIcebergProcess) {
    let stack = TestIcebergStack::start().await;
    let repository = CatalogRepository::new(
        stack.store().await,
        ClearBounds {
            delegated_access_ms: 900_000,
            ..ClearBounds::default()
        },
    )
    .unwrap();
    repository
        .execute(
            ManagementRequest {
                identity: RequestIdentity {
                    operation: OperationId::random(),
                    issued_ms: now_ms(),
                },
                principal: "manager".into(),
                action: ManagementAction::Initialize,
                expected_epoch: 0,
                display_name: "file-http".into(),
                confirmation: None,
                capabilities: None,
            },
            ManagementPrivilege::Manage,
            now_ms(),
        )
        .await
        .unwrap();
    common::activate(&repository).await;
    let process = process::TestIcebergProcess::start(&stack.cluster.mgmt_endpoints).await;
    (stack, process)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "manual TPC-H and TPC-DS loader stress on the small cluster"]
async fn tpc_loader_parallel_stress() {
    let (_stack, process) = setup().await;
    let python = std::env::var("CROWDB_TPC_LOADER_PYTHON")
        .expect("set CROWDB_TPC_LOADER_PYTHON to the installed TPC loader environment");
    let endpoint = format!("http://{}", process.address);
    let mut commands = Vec::new();
    for (benchmark, workers) in [("tpch", "8"), ("tpcds", "24")] {
        let mut command = tokio::process::Command::new("pixi");
        command
            .args([
                "run",
                "-e",
                "iceberg-e2e",
                "--",
                &python,
                "-m",
                "crowdb_tpc_loader",
                "load",
                "--benchmark",
                benchmark,
                "--sf",
                "1",
                "--namespace",
                benchmark,
                "--upload-workers",
                workers,
                "--no-download",
                "--keep-files",
            ])
            .env("ICEBERG_URI", &endpoint)
            .env("ICEBERG_TOKEN", "w".repeat(32))
            .current_dir(env!("CARGO_MANIFEST_DIR"));
        commands.push(command);
    }
    let mut tpcds = commands.pop().unwrap();
    let mut tpch = commands.pop().unwrap();
    let (tpch_result, tpcds_result) = tokio::join!(tpch.output(), tpcds.output());
    let mut failures = Vec::new();
    for (benchmark, output) in [("tpch", tpch_result.unwrap()), ("tpcds", tpcds_result.unwrap())] {
        println!("{benchmark} status: {}", output.status);
        println!("{benchmark} stdout: {}", String::from_utf8_lossy(&output.stdout));
        println!("{benchmark} stderr: {}", String::from_utf8_lossy(&output.stderr));
        if !output.status.success() {
            failures.push(benchmark);
        }
    }
    assert!(failures.is_empty(), "failed loaders: {failures:?}");
}
