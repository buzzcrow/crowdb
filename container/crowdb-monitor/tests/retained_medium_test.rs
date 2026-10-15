// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_monitor::{check_retained_medium_for_tests, discover_disks};
use crowdb_protocol::{
    common::{DiskId, HwStatus},
    disk_storage::DiskStorageRecord,
    diskdb::rpc::DiskValue,
    mgmt::node::storage::{DiskBackend, DiskInput},
};
use std::{fs, os::unix::fs::symlink, path::PathBuf};
struct TestMedium {
    root: crowdb_test_harness::test_dirs::TestDir,
    path: PathBuf,
    record: DiskStorageRecord,
}
impl TestMedium {
    fn new() -> Self {
        let root = crowdb_test_harness::test_dirs::tempdir_in_test_data("retained-medium");
        fs::create_dir(root.path().join("disks")).unwrap();
        let path = root.path().join("disks/original.img");
        fs::write(&path, vec![b'Z'; 256 * 1024]).unwrap();
        let observation = discover_disks(root.path(), &[]).unwrap().disks.remove(0);
        let record = DiskStorageRecord {
            version: 1,
            physical_backing: None,
            operation_id: "original".into(),
            rack_id: 1,
            node_id: 2,
            disk_group_id: 3,
            disk_id: DiskId { high: 4, low: 5 },
            backend: DiskBackend::File,
            backing_identity: observation.backing_identity.unwrap(),
            media_type: None,
            sparse: false,
            removed: true,
            geometry: DiskValue {
                disk_type: 0,
                capacity_units: 64,
                zone_size_units: 64,
                unit_size_bytes: 4096,
                zone_count: 1,
                status: HwStatus::Init as i32,
                device_path: path.to_string_lossy().into_owned(),
            },
        };
        Self { root, path, record }
    }
    fn check(&self) -> std::io::Result<crowdb_protocol::mgmt::node::storage::RetainedMedium> {
        check_retained_medium_for_tests(self.root.path(), &[], &self.record)
    }
}

#[test]
fn retained_check_opens_original_bytes_without_modification_or_new_files() {
    let test = TestMedium::new();
    let result = test.check().unwrap();
    assert_eq!(result.backing_identity, test.record.backing_identity);
    assert_eq!(result.capacity_bytes, 256 * 1024);
    assert_eq!(fs::read(&test.path).unwrap(), vec![b'Z'; 256 * 1024]);
    assert_eq!(fs::read_dir(test.root.path().join("disks")).unwrap().count(), 1);
}

#[test]
fn retained_check_rejects_missing_truncated_replaced_and_symlinked_media() {
    let test = TestMedium::new();
    fs::OpenOptions::new()
        .write(true)
        .open(&test.path)
        .unwrap()
        .set_len(4096)
        .unwrap();
    assert!(test.check().is_err());
    assert_eq!(fs::metadata(&test.path).unwrap().len(), 4096);
    let replacement = test.root.path().join("replacement.img");
    fs::write(&replacement, vec![b'X'; 256 * 1024]).unwrap();
    fs::rename(&replacement, &test.path).unwrap();
    assert!(test.check().is_err());
    assert_eq!(fs::read(&test.path).unwrap(), vec![b'X'; 256 * 1024]);
    fs::rename(&test.path, &replacement).unwrap();
    assert!(test.check().is_err());
    assert!(!test.path.exists());
    symlink(&replacement, &test.path).unwrap();
    assert!(test.check().is_err());
    assert_eq!(fs::read(&replacement).unwrap(), vec![b'X'; 256 * 1024]);
}

#[test]
fn retained_check_rejects_underlying_aliases_and_changed_external_mapping() {
    let test = TestMedium::new();
    let input = DiskInput {
        path: test.path.to_string_lossy().into_owned(),
        backend: DiskBackend::File,
        backing_identity: Some("file:different".into()),
    };
    assert!(check_retained_medium_for_tests(test.root.path(), &[input], &test.record).is_err());
    fs::hard_link(&test.path, test.root.path().join("disks/alias.img")).unwrap();
    assert!(test.check().is_err());
    assert_eq!(fs::read(&test.path).unwrap(), vec![b'Z'; 256 * 1024]);
}

#[test]
fn retained_physical_tail_is_preserved_and_never_interpreted_as_allocatable_capacity() {
    use crowdb_protocol::disk_storage::PhysicalBacking;
    use std::io::Write;
    let mut test = TestMedium::new();
    let logical = 256 * 1024;
    let mut file = fs::OpenOptions::new().append(true).open(&test.path).unwrap();
    file.write_all(&[b'T'; 512]).unwrap();
    file.sync_all().unwrap();
    assert!(
        test.check().is_err(),
        "legacy geometry still requires its original exact capacity"
    );
    test.record.version = 2;
    test.record.physical_backing = Some(PhysicalBacking {
        physical_host_id: "owned-host".into(),
        resource_identity: test.record.backing_identity.clone(),
        capacity_bytes: logical + 512,
    });
    let observation = test.check().unwrap();
    assert_eq!(observation.capacity_bytes, logical + 512);
    assert_eq!(test.record.geometry.capacity_units, 64);
    let expected = [vec![b'Z'; usize::try_from(logical).unwrap()], vec![b'T'; 512]].concat();
    assert_eq!(fs::read(&test.path).unwrap(), expected);
    test.record.physical_backing.as_mut().unwrap().capacity_bytes = logical;
    assert!(
        test.check().is_err(),
        "physical bytes cannot be inferred from usable geometry"
    );
    assert_eq!(fs::read(&test.path).unwrap(), expected);
}
