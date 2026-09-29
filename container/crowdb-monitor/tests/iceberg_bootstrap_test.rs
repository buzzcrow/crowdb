use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};

use crowdb_monitor::{
    iceberg_step_names, BootstrapSession, DeploymentProfile, IcebergBootstrap, MonitorLog, ServerCredentials,
};
use uuid::Uuid;

struct TestRoot(PathBuf);

impl TestRoot {
    fn new() -> Self {
        let path = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../.crowdb-runtime/ephemeral")
            .join(format!("monitor-iceberg-{}", Uuid::new_v4()));
        fs::create_dir_all(path.join("data")).unwrap();
        fs::create_dir_all(path.join("log")).unwrap();
        Self(path.canonicalize().unwrap())
    }
}

impl Drop for TestRoot {
    fn drop(&mut self) {
        if !std::thread::panicking() {
            fs::remove_dir_all(&self.0).unwrap();
        }
    }
}

fn profile(root: &TestRoot) -> DeploymentProfile {
    let mut profile = DeploymentProfile::load(
        Path::new(env!("CARGO_MANIFEST_DIR")).join("../single-node-container/profile.toml"),
    )
    .unwrap();
    let program = root.0.join("iceberg-management");
    let script = format!(
        r#"#!/bin/sh
set -eu
root='{}'
shift
printf '%s\n' "$1" >> "$root/calls"
if [ "$1" = inspect ]; then
  if [ ! -f "$root/initialized" ]; then
    printf '%s\n' '{{"initialized":false}}'
    exit 0
  fi
  catalog=$(cat "$root/initialized")
  operation=$(cat "$root/operation")
  capabilities=0x0000
  if [ -f "$root/activated" ]; then capabilities=0x3fff; fi
  printf '{{"initialized":true,"catalog_id":"%s","display_name":"preview","activation_epoch":1,"state":"Ready","capability_bits":"%s","root_operation_id":"%s"}}\n' "$catalog" "$capabilities" "$operation"
  exit 0
fi
if [ "$1" = initialize ]; then
  printf '%s' '11111111-1111-4111-8111-111111111111' > "$root/initialized"
  printf '%s' "$2" | tr -d '-' > "$root/operation"
  exit 1
fi
if [ "$1" = activate ]; then
  printf '%s' "$2" | tr -d '-' > "$root/operation"
  touch "$root/activated"
  exit 1
fi
exit 2
"#,
        root.0.display()
    );
    fs::write(&program, script).unwrap();
    fs::set_permissions(&program, fs::Permissions::from_mode(0o700)).unwrap();
    profile
        .services
        .iter_mut()
        .find(|service| service.id == "access")
        .unwrap()
        .program = program;
    profile
}

#[tokio::test]
async fn lost_management_responses_are_proved_then_ready_restart_is_read_only() {
    let root = TestRoot::new();
    let profile = profile(&root);
    let data_root = root.0.join("data");
    let mut session =
        BootstrapSession::open(&data_root, b"profile", b"config", &iceberg_step_names()).unwrap();
    let credentials = ServerCredentials::load_or_create(&data_root).unwrap();
    let mut events = MonitorLog::open(&root.0.join("log"), profile.logs.clone())
        .await
        .unwrap();

    IcebergBootstrap::reconcile(&mut session, &profile, &credentials, &mut events)
        .await
        .unwrap();
    assert_eq!(session.manifest().step_complete("iceberg-initialize"), Some(true));
    assert_eq!(session.manifest().step_complete("iceberg-activate"), Some(true));
    session.mark_ready().unwrap();
    let before = fs::read_to_string(root.0.join("calls")).unwrap();
    assert!(before.contains("initialize\n"));
    assert!(before.contains("activate\n"));

    let mut restarted =
        BootstrapSession::open(&data_root, b"profile", b"config", &iceberg_step_names()).unwrap();
    IcebergBootstrap::reconcile(&mut restarted, &profile, &credentials, &mut events)
        .await
        .unwrap();
    let after = fs::read_to_string(root.0.join("calls")).unwrap();
    assert_eq!(after.matches("initialize\n").count(), 1);
    assert_eq!(after.matches("activate\n").count(), 1);
}

#[tokio::test]
async fn foreign_catalog_is_rejected_before_any_management_write() {
    let root = TestRoot::new();
    let profile = profile(&root);
    let data_root = root.0.join("data");
    let mut session =
        BootstrapSession::open(&data_root, b"profile", b"config", &iceberg_step_names()).unwrap();
    let credentials = ServerCredentials::load_or_create(&data_root).unwrap();
    let mut events = MonitorLog::open(&root.0.join("log"), profile.logs.clone())
        .await
        .unwrap();
    fs::write(root.0.join("initialized"), "11111111-1111-4111-8111-111111111111").unwrap();
    fs::write(root.0.join("operation"), "foreign").unwrap();

    assert!(
        IcebergBootstrap::reconcile(&mut session, &profile, &credentials, &mut events)
            .await
            .is_err()
    );
    assert_eq!(fs::read_to_string(root.0.join("calls")).unwrap(), "inspect\n");
    assert_eq!(
        session.manifest().step_complete("iceberg-initialize"),
        Some(false)
    );
}
