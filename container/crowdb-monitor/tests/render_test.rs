// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::fs;
use std::path::{Path, PathBuf};

use crowdb_monitor::{render_configs, DeploymentProfile};
use uuid::Uuid;

struct TestDirs(PathBuf);

impl TestDirs {
    fn new() -> Self {
        let root = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../.crowdb-runtime/ephemeral")
            .join(format!("monitor-render-{}", Uuid::new_v4()));
        fs::create_dir_all(root.join("templates")).unwrap();
        fs::create_dir(root.join("run")).unwrap();
        let source = Path::new(env!("CARGO_MANIFEST_DIR")).join("../single-node-preview/templates");
        for entry in fs::read_dir(source).unwrap() {
            let entry = entry.unwrap();
            fs::copy(entry.path(), root.join("templates").join(entry.file_name())).unwrap();
        }
        Self(root.canonicalize().unwrap())
    }

    fn templates(&self) -> PathBuf {
        self.0.join("templates")
    }

    fn run(&self) -> PathBuf {
        self.0.join("run")
    }
}

impl Drop for TestDirs {
    fn drop(&mut self) {
        if !std::thread::panicking() {
            fs::remove_dir_all(&self.0).unwrap();
        }
    }
}

fn profile() -> DeploymentProfile {
    DeploymentProfile::load(Path::new(env!("CARGO_MANIFEST_DIR")).join("../single-node-preview/profile.toml"))
        .unwrap()
}

#[test]
fn renders_profile_paths_and_topology_without_secrets() {
    let dirs = TestDirs::new();
    let outputs = render_configs(&profile(), &dirs.templates(), &dirs.run()).unwrap();
    assert_eq!(outputs.len(), 6);
    let diskio = fs::read_to_string(dirs.run().join("config/diskio.toml")).unwrap();
    assert!(diskio.contains("path = \"/opt/crowdb/data/disks/disk-0004.img\""));
    assert!(diskio.contains("zone_capacity = 17179869184"));
    let web = fs::read_to_string(dirs.run().join("config/crowdb-web.toml")).unwrap();
    assert!(web.contains("monitor_status = \"/opt/crowdb/run/status/monitor.json\""));
    assert!(!web.contains("{{"));
    for output in outputs {
        let body = fs::read_to_string(output.path).unwrap();
        assert!(!body.contains("MASTER_KEY"));
        assert!(!body.contains("ICEBERG_WRITE_TOKEN"));
        toml::from_str::<toml::Value>(&body).unwrap();
    }
}

#[test]
fn unknown_or_malformed_variables_fail_closed() {
    let dirs = TestDirs::new();
    let path = dirs.templates().join("diskio.toml");
    fs::write(&path, b"value = \"{{unknown}}\"\n").unwrap();
    assert!(render_configs(&profile(), &dirs.templates(), &dirs.run()).is_err());
    fs::write(&path, b"value = \"{{disk.0.path\"\n").unwrap();
    assert!(render_configs(&profile(), &dirs.templates(), &dirs.run()).is_err());
}
