// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! CLI e2e (R126 direct-to-group-0): `cluster status` and `cluster rack
//! list` route directly through `--system-ip` / `--system-port` against a
//! real `crowdb-kv-server` with group 0 initialized — no `crowdb-web`
//! intermediary.

mod common;

use common::direct::crowdb_cli_bin;

#[test]
fn clean_rejects_service_restart_without_launch_registry() {
    let output = std::process::Command::new(crowdb_cli_bin())
        .args(["cluster", "clean", "--restart-services"])
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(2));
    assert!(String::from_utf8_lossy(&output.stderr).contains("--registry"));
}
