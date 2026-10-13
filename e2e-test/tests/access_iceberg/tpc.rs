// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Explicit manual TPC host loaders against the packaged storage chain.

use std::path::PathBuf;
use std::time::Duration;

fn loader(benchmark: &str, workers: &str, endpoint: &str) -> tokio::process::Command {
    let root = PathBuf::from(std::env::var("CROWDB_E2E_ARTIFACTS").unwrap());
    let manifest = std::env::var("CROWDB_E2E_TPC_MANIFEST").expect("locked TPC client prerequisites");
    let mut command = tokio::process::Command::new("pixi");
    command
        .args([
            "run",
            "--locked",
            "--manifest-path",
            &manifest,
            "python",
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
            "--work-dir",
        ])
        .arg(root.join(benchmark))
        .arg("--report-file")
        .arg(root.join(format!("{benchmark}-report.json")))
        .env("ICEBERG_URI", endpoint)
        .env("ICEBERG_TOKEN", "w".repeat(32))
        .current_dir(root);
    command
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "manual SF1 TPC loaders require the locked host environment and cached generators"]
async fn parallel_loader_stress() {
    let addresses = crate::common::management::addresses(14);
    let endpoint = format!("http://{}:7502", addresses[12]);
    let mut tpch = loader("tpch", "8", &endpoint);
    let mut tpcds = loader("tpcds", "24", &endpoint);
    let (tpch, tpcds) = tokio::join!(tpch.output(), tpcds.output());
    let mut failures = Vec::new();
    for (benchmark, output) in [("tpch", tpch.unwrap()), ("tpcds", tpcds.unwrap())] {
        println!("{benchmark} status: {}", output.status);
        println!("{benchmark} stdout: {}", String::from_utf8_lossy(&output.stdout));
        println!("{benchmark} stderr: {}", String::from_utf8_lossy(&output.stderr));
        if !output.status.success() {
            failures.push(benchmark);
        }
    }
    assert!(failures.is_empty(), "failed loaders: {failures:?}");
    let http = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()
        .unwrap();
    for (benchmark, tables) in [("tpch", 8), ("tpcds", 24)] {
        let inventory: serde_json::Value = http
            .get(format!("{endpoint}/v1/namespaces/{benchmark}/tables"))
            .bearer_auth("w".repeat(32))
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(
            inventory["identifiers"].as_array().unwrap().len(),
            tables,
            "{inventory}"
        );
    }
}
