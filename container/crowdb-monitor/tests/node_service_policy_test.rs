// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_monitor::{DeploymentProfile, NodeServiceForTests};
use crowdb_protocol::mgmt::node::{
    deployment::{DeploymentMode, NodeDeploymentOverrides},
    NodeServiceAction, NodeServiceIntent,
};

#[tokio::test]
async fn production_service_intents_cannot_activate_local_placement_or_single_copy_streams() {
    let root = crowdb_test_harness::test_dirs::tempdir_in_test_data("production-service-policy");
    let inputs = NodeDeploymentOverrides {
        mode: Some(DeploymentMode::Production),
        ..NodeDeploymentOverrides::default()
    };
    crowdb_monitor::accept_node_policy(root.path(), &inputs, &NodeDeploymentOverrides::default()).unwrap();
    let mut profile = DeploymentProfile::load(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../oci-image/node-profile.toml"),
    )
    .unwrap();
    profile.paths.data_root = root.path().to_path_buf();
    profile.paths.log_root = root.path().join("log");
    std::fs::create_dir_all(&profile.paths.log_root).unwrap();
    let mut services = NodeServiceForTests::new_for_tests(profile).await.unwrap();
    for (kind, configuration) in [
        ("chunkdb", "[deployment]\nmode='local'"),
        ("access", "[deployment]\nmode='local'"),
        ("chunk-kv", "[storage]\nstream_mirror_copies=1"),
    ] {
        let error = services
            .execute_for_tests(NodeServiceIntent {
                cluster_id: uuid::Uuid::new_v4().to_string(),
                operation_id: uuid::Uuid::new_v4().to_string(),
                node_id: 2,
                service_id: format!("{kind}-2"),
                kind: kind.into(),
                action: NodeServiceAction::Start,
                configuration: configuration.into(),
                environment: std::collections::BTreeMap::new(),
            })
            .await
            .unwrap_err();
        assert!(error.to_string().contains("production node policy"), "{error}");
        assert!(!root.path().join("services").exists());
    }
}
