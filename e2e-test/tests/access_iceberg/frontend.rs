// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crate::common::{control, management};
use std::path::PathBuf;
use std::process::Command;

pub(super) struct TestFrontend {
    node: usize,
    pub origin: String,
}

impl TestFrontend {
    pub async fn start(node: usize) -> Self {
        assert!([12, 13].contains(&node));
        let ips = management::addresses(14);
        control::node("start", node).await;
        Self {
            node,
            origin: format!("http://{}:7502", ips[node]),
        }
    }

    pub fn address(&self) -> std::net::SocketAddr {
        self.origin.strip_prefix("http://").unwrap().parse().unwrap()
    }

    pub async fn stop(self) {
        control::node("stop", self.node).await;
    }

    pub fn check_official_reads(&self) {
        let script = super::namespace_script();
        let output = Command::new("pixi")
            .args([
                "run",
                "-e",
                "iceberg-e2e",
                "python",
                &script,
                &self.origin,
                "--read-only",
                "--namespace-only",
            ])
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "official Iceberg reads failed: {}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        eprintln!("{}", String::from_utf8_lossy(&output.stdout));
    }
}

pub(super) fn command(seeds: &[String]) -> Command {
    let artifacts = PathBuf::from(std::env::var("CROWDB_E2E_ARTIFACTS").unwrap());
    let mut command = Command::new(artifacts.join("release-access-cli"));
    command
        .arg("iceberg")
        .arg("--config")
        .arg(artifacts.join("iceberg.toml"))
        .env(
            "LD_LIBRARY_PATH",
            format!(
                "{}:{}",
                artifacts.join("release-cli-lib").display(),
                std::env::var("LD_LIBRARY_PATH").unwrap_or_default()
            ),
        )
        .env("CROWDB_MANAGEMENT_SEEDS", seeds.join(","))
        .env("CROWDB_ICEBERG_READ_TOKEN", "r".repeat(32))
        .env("CROWDB_ICEBERG_WRITE_TOKEN", "w".repeat(32))
        .env("CROWDB_ICEBERG_MANAGE_TOKEN", "m".repeat(32))
        .env("CROWDB_ICEBERG_CLEAR_TOKEN", "c".repeat(32));
    command
}
