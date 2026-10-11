// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::{path::PathBuf, process::Command, time::Duration};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires pinned Apache Iceberg 1.11.0 source and release storage"]
async fn apache_rest_compatibility_kit_supported_catalog_surface() {
    let source = PathBuf::from(std::env::var("CROWDB_E2E_CLIENT_ROOT").unwrap()).join("iceberg-rck/source");
    let ips = crate::common::management::addresses(14);
    let origin = format!("http://{}:7502", ips[12]);
    let selectors = std::env::var("CROWDB_ICEBERG_RCK_SELECTOR").unwrap_or_else(|_| {
        [
            "testCreateNamespace",
            "testBasicCreateTable",
            "testRenameTable",
            "testDropTable",
            "testDropMissingTable",
            "testListTables",
        ]
        .iter()
        .map(|name| format!("org.apache.iceberg.rest.RESTCompatibilityKitCatalogTests.{name}"))
        .collect::<Vec<_>>()
        .join(",")
    });
    let result = tokio::task::spawn_blocking(move || {
        let mut command = Command::new("timeout");
        command.args(["900", "pixi", "run", "-e", "iceberg-e2e"]);
        // Pixi resolves the workspace before Gradle changes to the pinned source.
        command.arg(source.join("gradlew"));
        command.args(["-p", source.to_str().unwrap(), ":iceberg-open-api:test"]);
        command
            .arg("--init-script")
            .arg(source.parent().unwrap().join("init.gradle"));
        for selector in selectors.split(',') {
            command.args(["--tests", selector]);
        }
        let status = command
            .args([
                "--no-daemon",
                "-Drck.local=false",
                "-Drck.requires-namespace-create=true",
            ])
            .env("CATALOG_URI", origin)
            .env("CATALOG_WAREHOUSE", "")
            .env("CATALOG_IO__IMPL", "org.apache.iceberg.aws.s3.S3FileIO")
            .env("CATALOG_TOKEN", "w".repeat(32))
            .status()
            .unwrap();
        assert!(status.success(), "Apache Iceberg RCK Gradle task failed");
        Command::new("pixi")
            .args(["run", "-e", "iceberg-e2e", "python"])
            .arg(source.parent().unwrap().join("verify.py"))
            .arg(source)
            .status()
            .unwrap()
    });
    let status = tokio::time::timeout(Duration::from_secs(930), result)
        .await
        .unwrap()
        .unwrap();
    assert!(status.success(), "Apache Iceberg REST Compatibility Kit failed");
}
