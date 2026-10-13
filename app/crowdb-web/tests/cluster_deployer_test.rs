// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Empty reset is a deterministic Web component proof.

use std::path::PathBuf;
use std::time::{Duration, Instant};

use crowdb_console_shared::cluster_deployer::CrowdbClusterDeployer;
use crowdb_console_shared::ConsoleConfig;
use crowdb_web::{router, AppState};

fn tempdir(tag: &str) -> PathBuf {
    let base = crowdb_test_harness::test_dirs::test_data_dir().join("crowdb-deployer-tests");
    let _ = std::fs::create_dir_all(&base);
    let pid = std::process::id();
    let dir = base.join(format!("{tag}-{pid}"));
    let _ = std::fs::create_dir_all(&dir);
    dir
}

async fn spawn_web(cfg_path: PathBuf) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("local_addr");
    let cfg = ConsoleConfig::default();
    let state = AppState::with_config(cfg, Some(cfg_path));
    tokio::spawn(async move {
        let _ = axum::serve(listener, router(state)).await;
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    format!("http://{addr}")
}

#[tokio::test]
async fn deployer_reset_on_empty_is_fast() {
    let cfg_path = tempdir("empty-reset").join("console.toml");
    let base_url = spawn_web(cfg_path).await;

    let deployer = CrowdbClusterDeployer::new(&base_url).expect("deployer");

    let reset = Instant::now();
    deployer.reset().await.expect("reset on empty");
    let reset_ms = reset.elapsed().as_millis();
    eprintln!("reset on empty took {reset_ms}ms");
    assert!(
        reset_ms < 500,
        "reset on empty should be <500ms, took {reset_ms}ms"
    );
}
