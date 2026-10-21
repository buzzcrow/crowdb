// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::{fs, path::Path, process::Command};

use crowdb_monitor::accept_node_policy;
use crowdb_protocol::mgmt::node::deployment::NodeDeploymentOverrides;

fn profile(root: &Path) -> std::path::PathBuf {
    let install = root.join("install");
    fs::create_dir_all(install.join("data")).unwrap();
    let template = Path::new(env!("CARGO_MANIFEST_DIR")).join("../single-node-container/profile.toml");
    let text = fs::read_to_string(template)
        .unwrap()
        .replace("/opt/crowdb", install.to_str().unwrap());
    let path = root.join("profile.toml");
    fs::write(&path, text).unwrap();
    path
}

fn command(profile: &Path) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_crowdb-monitor"));
    command.args(["run", "--profile"]).arg(profile);
    for name in [
        "CROWDB_NODE_POLICY_FILE",
        "CROWDB_DEPLOYMENT_MODE",
        "CROWDB_CLUSTER",
        "CROWDB_INITIAL_VOTERS",
        "CROWDB_PHYSICAL_HOST_ID",
    ] {
        command.env_remove(name);
    }
    command
}

#[test]
fn invalid_voter_override_fails_before_node_identity_and_storage_effects() {
    let root = crowdb_test_harness::test_dirs::tempdir_in_test_data("node-cli-invalid-voters");
    let profile = profile(root.path());
    let output = command(&profile)
        .args(["--initial-voters", "2"])
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("initial voters must be one or three"));
    assert_eq!(fs::read_dir(root.path().join("install/data")).unwrap().count(), 0);
}

#[test]
fn explicit_flags_override_environment_and_file_without_rewriting_accepted_inputs() {
    let root = crowdb_test_harness::test_dirs::tempdir_in_test_data("node-cli-input-precedence");
    let profile = profile(root.path());
    let data = root.path().join("install/data");
    let inputs = NodeDeploymentOverrides {
        cluster: Some("retained".into()),
        initial_voters: Some(3),
        ..Default::default()
    };
    accept_node_policy(&data, &NodeDeploymentOverrides::default(), &inputs).unwrap();
    let before = fs::read(data.join("node-policy.json")).unwrap();
    let config = root.path().join("node.toml");
    fs::write(&config, "cluster = 'file-cluster'\ninitial_voters = 1\n").unwrap();
    let output = command(&profile)
        .arg("--deployment-config")
        .arg(config)
        .env("CROWDB_CLUSTER", "environment-cluster")
        .env("CROWDB_INITIAL_VOTERS", "1")
        .args([
            "--cluster",
            "retained",
            "--initial-voters",
            "3",
            "--physical-host-id",
            "",
        ])
        .output()
        .unwrap();
    assert!(!output.status.success());
    let error = String::from_utf8_lossy(&output.stderr);
    assert!(
        error.contains("physical host identity must contain 1 to 256 bytes"),
        "{error}"
    );
    assert_eq!(fs::read(data.join("node-policy.json")).unwrap(), before);
    assert!(!data.join("node-identity").exists());
}

#[test]
fn production_cannot_fall_back_to_local_host_identity() {
    let root = crowdb_test_harness::test_dirs::tempdir_in_test_data("node-cli-production");
    let profile = profile(root.path());
    let output = command(&profile)
        .env("CROWDB_DEPLOYMENT_MODE", "production")
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("production deployment requires physical-host-id")
    );
    assert_eq!(fs::read_dir(root.path().join("install/data")).unwrap().count(), 0);
}

#[test]
fn unknown_file_version_and_corrupt_inputs_do_not_acquire_policy() {
    for content in ["version = 99\n", "invalid = true\n", "not TOML"] {
        let root = crowdb_test_harness::test_dirs::tempdir_in_test_data("node-cli-config-invalid");
        let profile = profile(root.path());
        let config = root.path().join("node.toml");
        fs::write(&config, content).unwrap();
        let output = command(&profile)
            .env("CROWDB_NODE_POLICY_FILE", config)
            .output()
            .unwrap();
        assert!(!output.status.success());
        assert_eq!(fs::read_dir(root.path().join("install/data")).unwrap().count(), 0);
    }
}

#[test]
fn container_preflight_seals_policy_without_generating_keys_identity_or_storage() {
    let root = crowdb_test_harness::test_dirs::tempdir_in_test_data("node-cli-preflight");
    let profile = profile(root.path());
    let invoke = |voters: Option<&str>| {
        let mut command = Command::new(env!("CARGO_BIN_EXE_crowdb-monitor"));
        command.args(["accept-policy", "--profile"]).arg(&profile);
        for name in [
            "CROWDB_NODE_POLICY_FILE",
            "CROWDB_DEPLOYMENT_MODE",
            "CROWDB_CLUSTER",
            "CROWDB_INITIAL_VOTERS",
            "CROWDB_PHYSICAL_HOST_ID",
        ] {
            command.env_remove(name);
        }
        if let Some(voters) = voters {
            command.args(["--initial-voters", voters]);
        }
        command.output().unwrap()
    };
    let output = invoke(Some("3"));
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(output.stdout, b"local\n");
    let data = root.path().join("install/data");
    assert_eq!(fs::read_dir(&data).unwrap().count(), 1);
    let before = fs::read(data.join("node-policy.json")).unwrap();
    assert!(invoke(None).status.success());
    assert!(!invoke(Some("1")).status.success());
    assert_eq!(fs::read(data.join("node-policy.json")).unwrap(), before);
    assert_eq!(fs::read_dir(data).unwrap().count(), 1);
}

#[test]
fn container_preflight_returns_retained_production_mode_without_a_local_fallback() {
    let root = crowdb_test_harness::test_dirs::tempdir_in_test_data("node-cli-preflight-production");
    let profile = profile(root.path());
    let config = root.path().join("node.toml");
    fs::write(&config, "mode = 'production'\ninitial_voters = 3\n").unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_crowdb-monitor"))
        .args(["accept-policy", "--profile"])
        .arg(&profile)
        .arg("--deployment-config")
        .arg(&config)
        .args(["--physical-host-id", "real-host"])
        .env_remove("CROWDB_DEPLOYMENT_MODE")
        .env_remove("CROWDB_CLUSTER")
        .env_remove("CROWDB_INITIAL_VOTERS")
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(output.stdout, b"production\n");
    let output = Command::new(env!("CARGO_BIN_EXE_crowdb-monitor"))
        .args(["accept-policy", "--profile"])
        .arg(&profile)
        .args(["--physical-host-id", "real-host"])
        .env_remove("CROWDB_DEPLOYMENT_MODE")
        .env_remove("CROWDB_CLUSTER")
        .env_remove("CROWDB_INITIAL_VOTERS")
        .env_remove("CROWDB_NODE_POLICY_FILE")
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(output.stdout, b"production\n");
    assert_eq!(fs::read_dir(root.path().join("install/data")).unwrap().count(), 1);
}
