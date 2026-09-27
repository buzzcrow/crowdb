use std::fs;
use std::path::Path;

use crowdb_console_shared::config::web::{LaunchRegistry, WebMode, WebProcessConfig};

const WEB: &str = r#"
version = 1
mode = "docker"
bind = "0.0.0.0"
port = 14000
group0_management_seeds = ["http://127.0.0.1:10000"]
ui_root = "/opt/crowdb/ui"
monitor_status = "/opt/crowdb/run/status/monitor.json"
log_dir = "/opt/crowdb/data/log/web"
log_max_file_mb = 30
log_max_files = 5
request_timeout_ms = 3000
"#;

#[test]
fn monitor_web_process_config_accepts_only_process_fields() {
    let config: WebProcessConfig = toml::from_str(WEB).unwrap();
    config.validate().unwrap();
    assert_eq!(config.mode, WebMode::Docker);
    for injected in [
        "rack = []",
        "token = 'secret'",
        "registry = '/tmp/registry.toml'",
        "pid = 1",
    ] {
        assert!(toml::from_str::<WebProcessConfig>(&format!("{WEB}\n{injected}\n")).is_err());
    }
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../container/single-node-preview/templates/crowdb-web.toml");
    let rendered = fs::read_to_string(path)
        .unwrap()
        .replace("{{install_root}}", "/opt/crowdb")
        .replace("{{run_root}}", "/opt/crowdb/run")
        .replace("{{log_root}}", "/opt/crowdb/data/log");
    let parsed: WebProcessConfig = toml::from_str(&rendered).unwrap();
    parsed.validate().unwrap();

    let bare_metal = WEB
        .replace("mode = \"docker\"", "mode = \"bare-metal\"")
        .replace("monitor_status = \"/opt/crowdb/run/status/monitor.json\"\n", "");
    let config: WebProcessConfig = toml::from_str(&bare_metal).unwrap();
    config.validate().unwrap();
    assert_eq!(config.mode, WebMode::BareMetal);
    assert!(toml::from_str::<WebProcessConfig>(&WEB.replace("docker", "monitor-managed")).is_err());
}

#[test]
fn standalone_launch_registry_rejects_topology_and_inline_secret() {
    let body = r#"
version = 1
[[launch]]
node_id = 1
service_id = "kv"
host = "localhost"
ssh_credential_ref = "operator-key"
binary_path = "/opt/crowdb/bin/crowdb-kv-server"
service_config_path = "/opt/crowdb/run/config/kv.toml"
workspace = "/opt/crowdb/run"
auto_start = false
"#;
    let registry: LaunchRegistry = toml::from_str(body).unwrap();
    registry.validate().unwrap();
    assert!(toml::from_str::<LaunchRegistry>(&format!("{body}\npassword = 'secret'\n")).is_err());
    assert!(toml::from_str::<LaunchRegistry>(&format!("{body}\n[[rack]]\nid = 1\n")).is_err());
    let duplicate = format!("{body}\n{}", body.replace("version = 1\n", ""));
    assert!(toml::from_str::<LaunchRegistry>(&duplicate)
        .unwrap()
        .validate()
        .is_err());
}
