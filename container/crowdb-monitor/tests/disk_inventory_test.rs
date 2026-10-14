// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::fs;
use std::os::unix::fs::symlink;
use std::path::PathBuf;

use crowdb_monitor::discover_disks;
use crowdb_protocol::mgmt::node::storage::{DiskBackend, DiskInput};

struct TestRoot(PathBuf);

impl TestRoot {
    fn new() -> Self {
        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../../.crowdb-runtime/ephemeral")
            .join(format!("disk-inventory-{}", uuid::Uuid::new_v4()));
        fs::create_dir_all(&path).unwrap();
        Self(path.canonicalize().unwrap())
    }
}

impl Drop for TestRoot {
    fn drop(&mut self) {
        fs::remove_dir_all(&self.0).unwrap();
    }
}

#[test]
fn absent_inventory_does_not_create_default_disks_or_hide_missing_inputs() {
    let root = TestRoot::new();
    assert!(discover_disks(&root.0, &[]).unwrap().disks.is_empty());
    let input = DiskInput {
        backend: DiskBackend::BlockDevice,
        path: root.0.join("disks/missing").to_string_lossy().into_owned(),
    };
    let inventory = discover_disks(&root.0, &[input]).unwrap();
    assert_eq!(inventory.disks.len(), 1);
    assert!(inventory.disks[0].supplied);
    assert!(inventory.disks[0].error.is_some());
    assert!(!root.0.join("disks").exists());
}

#[test]
fn discovery_reports_file_capacity_without_admitting_or_modifying_it() {
    let root = TestRoot::new();
    fs::create_dir(root.0.join("disks")).unwrap();
    let path = root.0.join("disks/disk.img");
    fs::write(&path, b"retain disk data").unwrap();
    let inventory = discover_disks(&root.0, &[]).unwrap();
    let disk = &inventory.disks[0];
    assert_eq!(disk.backend, Some(DiskBackend::File));
    assert_eq!(disk.capacity_bytes, Some(16));
    assert!(disk.error.is_none());
    assert!(!disk.supplied);
    assert_eq!(fs::read(&path).unwrap(), b"retain disk data");
    assert_eq!(fs::read_dir(&root.0).unwrap().count(), 1);
}

#[test]
fn aliases_symlinks_and_changed_mapping_types_remain_ineligible() {
    let root = TestRoot::new();
    let disks = root.0.join("disks");
    fs::create_dir(&disks).unwrap();
    fs::write(disks.join("a.img"), b"content").unwrap();
    fs::hard_link(disks.join("a.img"), disks.join("b.img")).unwrap();
    symlink(disks.join("a.img"), disks.join("link")).unwrap();
    let input = DiskInput {
        backend: DiskBackend::BlockDevice,
        path: disks.join("a.img").to_string_lossy().into_owned(),
    };
    let inventory = discover_disks(&root.0, &[input]).unwrap();
    assert_eq!(inventory.disks.len(), 3);
    assert!(inventory.disks.iter().all(|disk| disk.error.is_some()));
    let input = DiskInput {
        backend: DiskBackend::File,
        path: root.0.join("outside").to_string_lossy().into_owned(),
    };
    assert!(discover_disks(&root.0, &[input]).is_err());
}

#[test]
fn oversized_inventory_returns_an_explicit_error_instead_of_a_truncated_reply() {
    let root = TestRoot::new();
    let supplied = (0..512)
        .map(|index| DiskInput {
            backend: DiskBackend::File,
            path: root
                .0
                .join(format!("disks/disk-{index}.img"))
                .to_string_lossy()
                .into_owned(),
        })
        .collect::<Vec<_>>();
    let error = discover_disks(&root.0, &supplied).unwrap_err();
    assert!(error.to_string().contains("control response limit"));
    assert!(!root.0.join("disks").exists());
}
