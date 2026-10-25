// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[tokio::test]
#[ignore = "requires the interrupted normal release-monitor container"]
async fn interrupted_setup_preserves_completed_steps() {
    let root = std::path::PathBuf::from(std::env::var("CROWDB_E2E_ARTIFACTS").unwrap());
    let before: serde_json::Value =
        serde_json::from_slice(&std::fs::read(root.join("interrupted-setup.json")).unwrap()).unwrap();
    let after: serde_json::Value =
        serde_json::from_slice(&std::fs::read(root.join("recovered-setup.json")).unwrap()).unwrap();
    assert_eq!(before["accepted"], after["accepted"]);
    assert_eq!(before["setup"]["request"], after["setup"]["request"]);
    assert_eq!(before["setup"]["status"]["phase"], "running");
    assert_eq!(after["setup"]["status"]["phase"], "ready");
    let completed = before["setup"]["status"]["services"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|step| step["phase"] == "ready")
        .collect::<Vec<_>>();
    assert!(!completed.is_empty());
    let steps = after["setup"]["status"]["services"].as_array().unwrap();
    for prior in completed {
        let retained = steps.iter().find(|step| step["kind"] == prior["kind"]).unwrap();
        assert_eq!(retained["phase"], "ready");
        if prior["kind"] != "paxos-kv" {
            assert!(prior["operation_id"].is_string());
        }
        assert_eq!(retained["operation_id"], prior["operation_id"]);
    }
    super::witness_with_write(true, true).await;
}
