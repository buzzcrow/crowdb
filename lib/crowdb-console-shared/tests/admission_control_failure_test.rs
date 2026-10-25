// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_console_shared::{config::NodeEntry, deployment::admission::remote_control, error::Error};
use crowdb_protocol::mgmt::node::NodeControl;

#[path = "common/ssh_server.rs"]
mod ssh_server;

#[tokio::test]
async fn dependency_wait_survives_the_remote_monitor_control_boundary() {
    let root = crowdb_test_harness::test_dirs::tempdir_in_test_data("monitor-dependency-wait");
    std::env::set_var("CROWDB_KV_KNOWN_HOSTS", root.path().join("known-hosts"));
    let key = russh::keys::key::KeyPair::generate_ed25519().unwrap();
    let path = root.path().join("operator-key");
    russh::keys::encode_pkcs8_pem(&key, std::fs::File::create(&path).unwrap()).unwrap();
    for (message, waiting) in [
        ("Error: Waiting: DiskDB has not loaded registered disks\n", true),
        ("Error: service operation was superseded\n", false),
    ] {
        let server = ssh_server::TestSshServer::start_with_result(
            key.clone_public_key().unwrap(),
            ssh_server::TestReply::Failure(message.into()),
        )
        .await;
        let node = NodeEntry {
            id: 1,
            rack_id: 1,
            host: "127.0.0.1".into(),
            ssh_port: server.port,
            ssh_user: "operator".into(),
            ssh_key: Some(path.to_string_lossy().into_owned()),
            ssh_password: None,
            ssh_credential_ref: None,
        };
        let error = remote_control(&node, &NodeControl::Identity).await.unwrap_err();
        assert_eq!(
            matches!(error, Error::DependenciesNotReady(_)),
            waiting,
            "{error}"
        );
    }
}
