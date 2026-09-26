// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::os::unix::fs::symlink;
use std::path::{Path, PathBuf};

use crowdb_monitor::{disk_step_names, ensure_disk_files, BootstrapSession, DeploymentProfile, MonitorLog};
use uuid::Uuid;

struct TestRoots(PathBuf);

impl TestRoots {
    fn new() -> Self {
        let root = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../.crowdb-runtime/ephemeral")
            .join(format!("monitor-disks-{}", Uuid::new_v4()));
        fs::create_dir_all(root.join("data")).unwrap();
        Self(root.canonicalize().unwrap())
    }

    fn profile(&self) -> DeploymentProfile {
        let mut profile = DeploymentProfile::load(
            Path::new(env!("CARGO_MANIFEST_DIR")).join("../single-node-preview/profile.toml"),
        )
        .unwrap();
        profile.paths.install_root.clone_from(&self.0);
        profile.paths.bin_root = self.0.join("bin");
        profile.paths.template_root = self.0.join("templates");
        profile.paths.data_root = self.0.join("data");
        profile.paths.run_root = self.0.join("run");
        profile.paths.log_root = self.0.join("data/log");
        for disk in &mut profile.disks {
            disk.path = self.0.join("data/disks").join(disk.path.file_name().unwrap());
        }
        for service in &mut profile.services {
            service.program = self.0.join("bin").join(service.program.file_name().unwrap());
            service.config_template = service
                .config_template
                .as_ref()
                .map(|path| self.0.join("templates").join(path.file_name().unwrap()));
        }
        profile.validate().unwrap();
        profile
    }

    fn session(&self, profile: &DeploymentProfile) -> BootstrapSession {
        let names = disk_step_names(profile);
        let steps = names.iter().map(String::as_str).collect::<Vec<_>>();
        BootstrapSession::open(&self.0.join("data"), b"profile", b"config", &steps).unwrap()
    }

    async fn events(&self, profile: &DeploymentProfile) -> MonitorLog {
        let root = self.0.join("data/log");
        fs::create_dir_all(&root).unwrap();
        MonitorLog::open(&root, profile.logs.clone()).await.unwrap()
    }
}

impl Drop for TestRoots {
    fn drop(&mut self) {
        if !std::thread::panicking() {
            fs::remove_dir_all(&self.0).unwrap();
        }
    }
}

#[tokio::test]
async fn creates_sparse_disks_and_ready_restart_preserves_bytes() {
    let roots = TestRoots::new();
    let profile = roots.profile();
    let mut session = roots.session(&profile);
    let mut events = roots.events(&profile).await;
    ensure_disk_files(&mut session, &profile, &mut events)
        .await
        .unwrap();
    assert!(session.manifest().next_step().is_none());
    for disk in &profile.disks {
        let metadata = fs::metadata(&disk.path).unwrap();
        assert_eq!(metadata.len(), disk.capacity_bytes);
    }
    let first = &profile.disks[0].path;
    let mut file = OpenOptions::new().write(true).open(first).unwrap();
    file.seek(SeekFrom::Start(1024)).unwrap();
    file.write_all(b"keep").unwrap();
    file.sync_all().unwrap();
    session.mark_ready().unwrap();
    drop(session);
    let mut restart = roots.session(&profile);
    ensure_disk_files(&mut restart, &profile, &mut events)
        .await
        .unwrap();
    let mut marker = [0; 4];
    let mut file = File::open(first).unwrap();
    file.seek(SeekFrom::Start(1024)).unwrap();
    file.read_exact(&mut marker).unwrap();
    assert_eq!(&marker, b"keep");
    let body = fs::read_to_string(roots.0.join("data/log/monitor/monitor.log")).unwrap();
    assert_eq!(body.matches("bootstrap_step_completed").count(), 4);
}

#[tokio::test]
async fn rejects_changed_or_missing_completed_disk() {
    let roots = TestRoots::new();
    let profile = roots.profile();
    let mut session = roots.session(&profile);
    let mut events = roots.events(&profile).await;
    ensure_disk_files(&mut session, &profile, &mut events)
        .await
        .unwrap();
    session.mark_ready().unwrap();
    let first = &profile.disks[0].path;
    fs::remove_file(first).unwrap();
    let mut restart = roots.session(&profile);
    assert!(ensure_disk_files(&mut restart, &profile, &mut events)
        .await
        .is_err());
    assert!(!first.exists());
    symlink("/dev/null", first).unwrap();
    assert!(ensure_disk_files(&mut restart, &profile, &mut events)
        .await
        .is_err());
}
