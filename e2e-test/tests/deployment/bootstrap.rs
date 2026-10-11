// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[tokio::test]
#[ignore = "requires the interrupted normal release-monitor container"]
async fn interrupted_bootstrap_preserves_completed_steps() {
    let root = std::path::PathBuf::from(std::env::var("CROWDB_E2E_ARTIFACTS").unwrap());
    let before: serde_json::Value =
        serde_json::from_slice(&std::fs::read(root.join("interrupted-manifest.json")).unwrap()).unwrap();
    let after: serde_json::Value =
        serde_json::from_slice(&std::fs::read(root.join("recovered-manifest.json")).unwrap()).unwrap();
    assert_eq!(before["state"], "initializing");
    assert_eq!(after["state"], "ready");
    assert_eq!(before["deployment_id"], after["deployment_id"]);
    let steps = after["steps"].as_array().unwrap();
    assert!(steps.iter().all(|step| step["complete"] == true));
    let completed = before["steps"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|step| step["complete"] == true)
        .collect::<Vec<_>>();
    assert!(!completed.is_empty());
    assert!(completed.iter().all(|prior| steps
        .iter()
        .any(|step| step["name"] == prior["name"] && step["complete"] == true)));
    super::witness_with_write(true, true).await;
}
