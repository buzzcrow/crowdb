// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_monitor::{DeploymentProfile, ProfileError};

fn profile() -> String {
    r#"
version = 1
name = "test-profile"
display_name = "Test Profile"
placement_mode = "test"
s3_tenant = "test"
iceberg_catalog = "test"

[paths]
install_root = "/opt/crowdb"
bin_root = "/opt/crowdb/bin"
template_root = "/opt/crowdb/etc/templates"
data_root = "/opt/crowdb/data"
run_root = "/opt/crowdb/run"
log_root = "/opt/crowdb/data/log"

[logs]
max_file_bytes = 31457280
max_files = 5
mirror_warnings_to_stderr = true

[[nodes]]
node_id = 1
rack_id = 1

[[groups]]
store_id = 0
group_id = 0
replica_id = 1
role = "system"

[[groups]]
store_id = 0
group_id = 1
replica_id = 2
role = "data"

[[disks]]
disk_id = "00000000000000000000000000000001"
disk_group_id = 101
node_id = 1
path = "/opt/crowdb/data/disks/disk-0001.img"
capacity_bytes = 17179869184
zone_size_bytes = 17179869184

[[public_endpoints]]
id = "web"
bind = "0.0.0.0"
port = 14000

[[services]]
id = "kv"
program = "/opt/crowdb/bin/crowdb-kv-server"
args = []
dependencies = []
config_template = "/opt/crowdb/etc/templates/kv.toml"
[services.probe]
kind = "http"
target = "http://127.0.0.1:10000/health"
timeout_ms = 1000
failure_threshold = 3
[services.restart]
max_attempts = 5
backoff_base_ms = 100
backoff_max_ms = 1000

[[services]]
id = "web"
program = "/opt/crowdb/bin/crowdb-web"
args = []
dependencies = ["kv"]
config_template = "/opt/crowdb/etc/templates/crowdb-web.toml"
[services.probe]
kind = "http"
target = "http://127.0.0.1:14000/healthz"
timeout_ms = 1000
failure_threshold = 3
[services.restart]
max_attempts = 5
backoff_base_ms = 100
backoff_max_ms = 1000
"#
    .into()
}

#[test]
fn valid_profile_orders_dependencies() {
    let profile = DeploymentProfile::parse(&profile()).unwrap();
    let order = profile
        .services_in_start_order()
        .unwrap()
        .into_iter()
        .map(|service| service.id.as_str())
        .collect::<Vec<_>>();
    assert_eq!(order, ["kv", "web"]);
}

#[test]
fn dependency_cycle_is_rejected() {
    let body = profile().replace("dependencies = []", "dependencies = [\"web\"]");
    let error = DeploymentProfile::parse(&body).unwrap_err();
    assert!(matches!(error, ProfileError::Invalid(message) if message.contains("cycle")));
}

#[test]
fn secret_environment_is_rejected() {
    let body = profile().replace("args = []", "args = []\nenv = { API_TOKEN = \"secret\" }");
    let error = DeploymentProfile::parse(&body).unwrap_err();
    assert!(matches!(error, ProfileError::Invalid(message) if message.contains("secret-like")));
}

#[test]
fn path_escape_is_rejected() {
    let body = profile().replace(
        "/opt/crowdb/data/disks/disk-0001.img",
        "/opt/crowdb/data/../etc/disk-0001.img",
    );
    let error = DeploymentProfile::parse(&body).unwrap_err();
    assert!(matches!(error, ProfileError::Invalid(message) if message.contains("data_root/disks")));
}

#[test]
fn unbounded_log_policy_is_rejected() {
    let body = profile().replace("max_files = 5", "max_files = 0");
    let error = DeploymentProfile::parse(&body).unwrap_err();
    assert!(matches!(error, ProfileError::Invalid(message) if message.contains("log rotation")));
}
