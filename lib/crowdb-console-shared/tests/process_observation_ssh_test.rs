// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_console_shared::{config::NodeEntry, deployment::admission::RemoteProcessObservation};
use crowdb_protocol::mgmt::{node::NodeBinding, SystemBootstrapIdentity};
use serde_json::json;
use std::time::Duration;

#[path = "common/ssh_server.rs"]
mod ssh_server;

#[tokio::test]
async fn bound_process_reader_reuses_authentication_and_discards_failed_or_cancelled_reads() {
    let root = crowdb_test_harness::test_dirs::tempdir_in_test_data("process-observation-ssh");
    std::env::set_var("CROWDB_KV_KNOWN_HOSTS", root.path().join("known-hosts"));
    let key = russh::keys::key::KeyPair::generate_ed25519().unwrap();
    let path = root.path().join("node-key");
    russh::keys::encode_pkcs8_pem(&key, std::fs::File::create(&path).unwrap()).unwrap();
    let binding = NodeBinding {
        node_id: 1,
        bootstrap: SystemBootstrapIdentity {
            cluster_id: "bound-cluster".into(),
            operation_id: "bootstrap".into(),
            configuration_digest: "a".repeat(64),
        },
        management_seeds: vec![],
    };
    let value = json!({"node_id": 2, "cluster_id": "bound-cluster", "bootstrap": binding.bootstrap, "processes": {"kv": 42}});
    let bytes = serde_json::to_vec(&value).unwrap();
    for scenario in [
        "success",
        "rejection",
        "wrong-node",
        "wrong-bootstrap",
        "cancelled",
    ] {
        let reply = match scenario {
            "success" => ssh_server::TestReply::Stdout(bytes.clone()),
            "rejection" => ssh_server::TestReply::Failure("monitor rejected observation".into()),
            "wrong-node" => {
                let mut foreign = value.clone();
                foreign["node_id"] = json!(3);
                ssh_server::TestReply::Stdout(serde_json::to_vec(&foreign).unwrap())
            }
            "wrong-bootstrap" => {
                let mut foreign = value.clone();
                foreign["bootstrap"]["operation_id"] = json!("foreign-bootstrap");
                ssh_server::TestReply::Stdout(serde_json::to_vec(&foreign).unwrap())
            }
            _ => ssh_server::TestReply::DelayedStdout(bytes.clone(), Duration::from_secs(1)),
        };
        let server =
            ssh_server::TestSshServer::start_with_result(key.clone_public_key().unwrap(), reply).await;
        let node = NodeEntry {
            id: 2,
            rack_id: 1,
            host: "127.0.0.1".into(),
            ssh_port: server.port,
            ssh_user: "operator".into(),
            ssh_key: Some(path.to_string_lossy().into_owned()),
            ssh_password: None,
            ssh_credential_ref: None,
        };
        let mut reader = RemoteProcessObservation::new(node.clone(), binding.clone()).unwrap();
        if scenario == "success" {
            for _ in 0..5 {
                assert_eq!(reader.read().await.unwrap()["processes"]["kv"], 42);
            }
            assert_eq!(
                server.connections(),
                1,
                "one authentication for repeated fresh reads"
            );
        } else if scenario == "cancelled" {
            for _ in 0..2 {
                assert!(tokio::time::timeout(Duration::from_millis(100), reader.read())
                    .await
                    .is_err());
            }
            assert_eq!(
                server.connections(),
                2,
                "a cancelled read must not retain its session"
            );
        } else {
            for _ in 0..2 {
                assert!(reader.read().await.is_err(), "{scenario}");
            }
            assert_eq!(
                server.connections(),
                2,
                "a rejected response must discard its session"
            );
        }
        let mut password = node;
        password.ssh_password = Some("unused".into());
        assert!(RemoteProcessObservation::new(password, binding.clone()).is_err());
    }
}
