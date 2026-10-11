// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_monitor::{kv_step_names, BootstrapSession, DeploymentProfile, KvBootstrap, MonitorLog};

#[tokio::test]
#[ignore = "requires the selected release KV container"]
async fn monitor_creates_real_kv_groups_then_validates_after_restart() {
    let root = std::path::PathBuf::from(std::env::var("CROWDB_E2E_ARTIFACTS").unwrap());
    let profile = DeploymentProfile::load(root.join("profile.toml")).unwrap();
    let data = root.join("client-data/kv-bootstrap");
    std::fs::create_dir_all(&data).unwrap();
    let names = kv_step_names(&profile).unwrap();
    let steps = names.iter().map(String::as_str).collect::<Vec<_>>();
    let mut session = BootstrapSession::open(&data, b"profile", b"config", &steps).unwrap();
    std::fs::create_dir_all(data.join("log")).unwrap();
    let mut events = MonitorLog::open(&data.join("log"), profile.logs.clone())
        .await
        .unwrap();
    let origin = format!("http://{}:10000", crate::common::management::addresses(1)[0]);
    KvBootstrap::new(&origin)
        .unwrap()
        .reconcile(&mut session, &profile, &mut events)
        .await
        .unwrap();
    assert_eq!(session.manifest().next_step(), None);
    session.mark_ready().unwrap();
    super::witness_with_write(false, std::env::var("CROWDB_E2E_PHASE").as_deref() == Ok("write")).await;
}
