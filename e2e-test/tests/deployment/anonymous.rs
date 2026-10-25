// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[tokio::test]
#[ignore = "requires the owned default release container"]
async fn default_anonymous_volume_is_ready_and_monitor_death_exits_container() {
    let root = std::path::PathBuf::from(std::env::var("CROWDB_E2E_ARTIFACTS").unwrap());
    let info: serde_json::Value =
        serde_json::from_slice(&std::fs::read(root.join("anonymous-runtime.json")).unwrap()).unwrap();
    assert!(info["mounts"]
        .as_array()
        .unwrap()
        .iter()
        .any(|mount| mount["Destination"] == "/opt/crowdb/data" && mount["Type"] == "volume"));
    assert_eq!(info["status"]["phase"], "ready");
    assert_eq!(info["accepted"]["prepared"], true);
    assert_eq!(info["accepted"]["node_id"], 1);
    assert_eq!(
        info["status"]["observation"]["cluster_id"],
        info["accepted"]["bootstrap"]["cluster_id"]
    );
    assert!(info["log"]
        .as_str()
        .unwrap()
        .contains("For data you want to keep across container recreation"));
    let state = crate::common::control::node("kill", 0).await;
    assert_eq!(state["Running"], false);
    assert_ne!(state["ExitCode"], 0);
}
