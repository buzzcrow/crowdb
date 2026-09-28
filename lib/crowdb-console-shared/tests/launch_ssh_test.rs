// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#![cfg(target_os = "linux")]

use std::os::unix::fs::PermissionsExt;
use std::time::Duration;

use crowdb_console_shared::config::web::LaunchRecord;
use crowdb_console_shared::launch::LaunchRuntime;
use crowdb_console_shared::lifecycle;
use crowdb_test_harness::test_dirs::tempdir_in_test_data;

#[path = "common/ssh_server.rs"]
mod ssh_server;

struct TestProcess(u32);
impl Drop for TestProcess {
    fn drop(&mut self) {
        if lifecycle::process_is_alive(self.0) {
            let _ = lifecycle::stop_pid_with_timeout(self.0, Duration::from_secs(2));
        }
    }
}

#[tokio::test]
async fn ssh_launch_uses_referenced_key_and_preserves_literal_arguments() {
    let dir = tempdir_in_test_data("launch-ssh");
    std::env::set_var("CROWDB_KV_KNOWN_HOSTS", dir.path().join("known-hosts"));
    let key = russh::keys::key::KeyPair::generate_ed25519().unwrap();
    let credentials = dir.path().join("credentials");
    std::fs::create_dir_all(&credentials).unwrap();
    let file = std::fs::File::create(credentials.join("operator-key")).unwrap();
    russh::keys::encode_pkcs8_pem(&key, file).unwrap();
    let server = ssh_server::TestSshServer::start(key.clone_public_key().unwrap()).await;
    let workspace = dir.path().join("work 'space'");
    std::fs::create_dir_all(&workspace).unwrap();
    let binary = workspace.join("service 'quoted'");
    std::fs::write(
        &binary,
        "#!/bin/sh\nprintf '%s\\n' \"$@\" > arguments\nexec sleep 60\n",
    )
    .unwrap();
    std::fs::set_permissions(&binary, std::fs::Permissions::from_mode(0o700)).unwrap();
    let config = workspace.join("service.toml");
    std::fs::write(&config, "").unwrap();
    let record = LaunchRecord {
        node_id: 701,
        service_id: "kv".into(),
        host: "127.0.0.1".into(),
        ssh_credential_ref: Some("operator-key".into()),
        ssh_user: Some("operator".into()),
        ssh_port: server.port,
        binary_path: binary,
        service_config_path: config,
        workspace: workspace.clone(),
        auto_start: true,
        args: vec!["space and 'quote'".into(), "$(touch should-not-exist)".into()],
        readiness_url: None,
    };
    let runtime = LaunchRuntime::new(dir.path().join("runtime")).with_credential_root(credentials);
    let identity = runtime.start(&record).await.unwrap();
    let _guard = TestProcess(identity.pid);
    assert_eq!(runtime.status(&record).await.unwrap(), Some(identity));
    assert_eq!(runtime.start(&record).await.unwrap(), identity);
    assert_eq!(
        std::fs::read_to_string(workspace.join("arguments"))
            .unwrap()
            .lines()
            .collect::<Vec<_>>(),
        record.command_args()
    );
    assert!(!workspace.join("should-not-exist").exists());
    runtime.stop(&record).await.unwrap();
    assert!(!lifecycle::process_is_alive(identity.pid));
}
