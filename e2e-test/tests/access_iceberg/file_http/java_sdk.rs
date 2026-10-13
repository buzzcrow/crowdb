// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::setup::{setup, setup_with_bounds};
use crowdb_access_iceberg::catalog::ClearBounds;
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Maven and the pinned Apache Iceberg Java dependencies"]
async fn official_java_s3_fileio_uploads_and_reads_native_files() {
    use base64::Engine as _;
    use crowdb_access_iceberg::wire::LoadCredentialsResponse;
    use std::process::Command;

    let (_stack, _process, client, table) = setup().await;
    let response = serde_json::to_vec(&LoadCredentialsResponse::from(client.credentials)).unwrap();
    let configuration = format!(
        "endpoint=http://{}\ncredentials={}\nlocation={}\n",
        client.address,
        base64::engine::general_purpose::STANDARD.encode(response),
        table
            .file("placeholder")
            .unwrap()
            .to_string()
            .trim_end_matches("placeholder"),
    );
    let status = tokio::task::spawn_blocking(move || {
        let maven = std::env::var_os("CROWDB_ICEBERG_E2E_MVN").unwrap_or_else(|| "mvn".into());
        Command::new("timeout")
            .arg("600")
            .arg("pixi")
            .args(["run", "-e", "iceberg-e2e"])
            .arg(maven)
            .args(["--batch-mode", "--no-transfer-progress", "-f"])
            .arg(
                std::path::PathBuf::from(std::env::var("CROWDB_E2E_CLIENT_ROOT").unwrap())
                    .join("iceberg-java/pom.xml"),
            )
            // SDK worker pools live for the JVM lifetime. Run the client in its
            // own JVM instead of Maven's interrupt-driven embedded thread group.
            .args(["compile", "exec:exec", "-Dexec.executable=java"])
            .arg("-Dexec.args=-classpath %classpath TestIcebergFileIO")
            .env("CROWDB_TEST_FILEIO_PROPERTIES", configuration)
            .status()
            .unwrap()
    })
    .await
    .unwrap();
    assert!(status.success(), "official Apache Iceberg S3FileIO failed");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires native storage services, Maven and pinned Apache Iceberg dependencies"]
async fn official_java_catalog_commits_native_parquet_snapshots_and_staged_tables() {
    let (stack, process, _, _) = setup_with_bounds(ClearBounds {
        request_ms: 300_000,
        delegated_access_ms: 900_000,
        ..ClearBounds::default()
    })
    .await;
    let endpoint = format!("http://{}", process.address());
    run_catalog_sdk(endpoint, "data").await;
    process.stop().await;
    stack.restart_catalog().await;
    let restarted = super::super::frontend::TestFrontend::start(13).await;
    run_catalog_sdk(format!("http://{}", restarted.address()), "verify").await;
}

async fn run_catalog_sdk(endpoint: String, mode: &'static str) {
    run_sdk(endpoint, "TestIcebergCatalogWrites", mode).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires native storage services, Maven and pinned Apache Iceberg dependencies"]
async fn official_java_opaque_metadata_publication_preserves_valid_data_and_delete_reads() {
    let (_stack, process, _, _) = setup_with_bounds(ClearBounds {
        request_ms: 300_000,
        delegated_access_ms: 900_000,
        ..ClearBounds::default()
    })
    .await;
    let endpoint = format!("http://{}", process.address());
    run_sdk(endpoint, "TestIcebergSelectedFiles", "").await;
}

async fn run_sdk(endpoint: String, class: &'static str, mode: &'static str) {
    let status = tokio::task::spawn_blocking(move || {
        let maven = std::env::var_os("CROWDB_ICEBERG_E2E_MVN").unwrap_or_else(|| "mvn".into());
        std::process::Command::new("timeout")
            .arg("600")
            .arg("pixi")
            .args(["run", "-e", "iceberg-e2e"])
            .arg(maven)
            .args(["-o", "--batch-mode", "--no-transfer-progress", "-f"])
            .arg(
                std::path::PathBuf::from(std::env::var("CROWDB_E2E_CLIENT_ROOT").unwrap())
                    .join("iceberg-java/pom.xml"),
            )
            .args(["compile", "exec:exec", "-Dexec.executable=java"])
            .arg(format!(
                "-Dexec.args=-classpath %classpath {class} {endpoint} {mode}"
            ))
            .status()
            .unwrap()
    })
    .await
    .unwrap();
    assert!(
        status.success(),
        "official native catalog and Parquet acceptance failed"
    );
}
