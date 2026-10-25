// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_monitor::{run_node, serve_node_management, DiscoveryConfig, NodeRuntimeConfig};

#[tokio::test]
async fn invalid_seeds_fail_before_identity_creation_or_listener_binding_without_exposing_credentials() {
    let root = crowdb_test_harness::test_dirs::tempdir_in_test_data("invalid-discovery-seeds");
    let occupied = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let bind = occupied.local_addr().unwrap();
    let mut cases = vec![
        vec!["not-an-origin".into()],
        vec!["https://127.0.0.1".into()],
        vec!["http://operator:private-token@127.0.0.1".into()],
        vec!["http://127.0.0.1/node".into()],
        vec!["http://127.0.0.1/?private-token".into()],
        vec!["http://127.0.0.1/#private-token".into()],
    ];
    cases.push(vec!["http://127.0.0.1".into(); 65]);
    for seeds in cases {
        let config = DiscoveryConfig {
            seeds,
            interfaces: vec!["lo".into()],
            addresses: vec![bind.ip()],
            monitor_port: bind.port(),
            cluster_id: None,
        };
        let error = serve_node_management(root.path(), bind, &config, "test-host".into(), async {})
            .await
            .unwrap_err()
            .to_string();
        assert!(error.contains("discovery seeds"), "{error}");
        assert!(!error.contains("physical host"), "{error}");
        assert!(
            !error.contains("operator") && !error.contains("private-token"),
            "{error}"
        );
        let runtime = NodeRuntimeConfig {
            profile: root.path().join("absent-profile.toml"),
            bind,
            discovery: config,
            physical_host_id: "test-host".into(),
        };
        let error = run_node(runtime).await.unwrap_err().to_string();
        assert!(error.contains("discovery seeds"), "{error}");
        assert!(!error.contains("private-token"), "{error}");
        assert_eq!(std::fs::read_dir(root.path()).unwrap().count(), 0);
    }
}

#[test]
fn container_preflight_rejects_seed_credentials_before_accepting_policy() {
    let root = crowdb_test_harness::test_dirs::tempdir_in_test_data("invalid-seed-preflight");
    for command in ["accept-policy", "run"] {
        let output = std::process::Command::new(env!("CARGO_BIN_EXE_crowdb-monitor"))
            .args([command, "--profile"])
            .arg(root.path().join("absent-profile.toml"))
            .env_remove("CROWDB_NODE_POLICY_FILE")
            .env(
                "CROWDB_DISCOVERY_SEEDS",
                "http://operator:private-token@127.0.0.1",
            )
            .output()
            .unwrap();
        assert!(!output.status.success());
        let diagnostic = String::from_utf8_lossy(&output.stderr);
        assert!(diagnostic.contains("discovery seeds"), "{diagnostic}");
        assert!(!diagnostic.contains("private-token"), "{diagnostic}");
        assert_eq!(std::fs::read_dir(root.path()).unwrap().count(), 0);
    }
}
