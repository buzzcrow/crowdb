// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_monitor::{
    kv_step_names, BootstrapManifest, BootstrapSession, DeploymentProfile, KvBootstrap, ManifestState,
    MonitorLog,
};
use reqwest::Method;
use serde_json::{json, Value};

use crate::common::management::{client, request};

#[tokio::test]
#[ignore = "requires the selected release KV container"]
async fn monitor_creates_real_kv_groups_then_validates_after_restart() {
    let root = std::path::PathBuf::from(std::env::var("CROWDB_E2E_ARTIFACTS").unwrap());
    let profile = DeploymentProfile::load(root.join("profile.toml")).unwrap();
    let manifest_bytes = std::fs::read(root.join("managed-manifest.json")).unwrap();
    let manifest: BootstrapManifest = serde_json::from_slice(&manifest_bytes).unwrap();
    assert_eq!(manifest.state(), ManifestState::Ready);
    assert_eq!(manifest.next_step(), None);
    for name in kv_step_names(&profile).unwrap() {
        assert_eq!(manifest.step_complete(&name), Some(true));
    }
    let write = std::env::var("CROWDB_E2E_PHASE").as_deref() == Ok("write");
    let witness = root.join("initial-kv-manifest.json");
    if write {
        std::fs::write(&witness, &manifest_bytes).unwrap();
    } else {
        let initial: BootstrapManifest = serde_json::from_slice(&std::fs::read(witness).unwrap()).unwrap();
        assert_eq!(manifest, initial, "restart must preserve accepted initialization");
    }
    let origin = format!("http://{}:10000", crate::common::management::addresses(1)[0]);
    let http = client();
    let accepted = request(
        &http,
        &origin,
        Method::POST,
        "/system/prepare",
        json!({"replica_id": 1, "bootstrap": manifest.system_bootstrap_identity()}),
        200,
    )
    .await;
    assert_eq!(accepted["bootstrap"], json!(manifest.system_bootstrap_identity()));
    for group in &profile.groups {
        let ready = request(
            &http,
            &origin,
            Method::GET,
            &format!("/stores/0/groups/{}/ready", group.group_id),
            Value::Null,
            200,
        )
        .await;
        assert_eq!(ready["ready"], true);
        assert_eq!(ready["leader_id"], group.replica_id);
        assert_eq!(ready["voting_replicas"], 1);
    }
    // A client-side manifest is a different bootstrap, even with identical
    // replica geometry. It must not adopt the container's existing system group.
    let data = root.join("client-data/kv-bootstrap");
    std::fs::create_dir_all(&data).unwrap();
    let names = kv_step_names(&profile).unwrap();
    let steps = names.iter().map(String::as_str).collect::<Vec<_>>();
    let mut session = BootstrapSession::open(&data, b"profile", b"config", &steps).unwrap();
    std::fs::create_dir_all(data.join("log")).unwrap();
    let mut events = MonitorLog::open(&data.join("log"), profile.logs.clone())
        .await
        .unwrap();
    let error = KvBootstrap::new(&origin)
        .unwrap()
        .reconcile(&mut session, &profile, &mut events)
        .await
        .unwrap_err();
    assert!(error.to_string().contains("conflicting bootstrap"), "{error}");
    assert_eq!(session.manifest().next_step(), Some("kv-group-0-0"));
    super::witness_with_write(false, write).await;
}
