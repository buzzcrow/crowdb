// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_monitor::verify_blank_medium_for_tests;
use crowdb_protocol::{
    common::{DiskId, HwStatus},
    disk_storage::{DiskStorageRecord, PhysicalBacking},
    diskdb::rpc::DiskValue,
    mgmt::node::storage::{DiskBackend, DiskInput},
};
use std::{
    fmt::Write as _,
    fs, io,
    os::unix::fs::{FileExt, MetadataExt, PermissionsExt},
    path::Path,
};
#[path = "common/node_setup.rs"]
mod node_setup;

struct TestMedium {
    root: crowdb_test_harness::test_dirs::TestDir,
    record: DiskStorageRecord,
    inputs: Vec<DiskInput>,
}

impl TestMedium {
    fn new(tail: u64) -> Self {
        let root = crowdb_test_harness::test_dirs::tempdir_in_test_data("blank-medium");
        fs::create_dir(root.path().join("disks")).unwrap();
        let path = root.path().join("disks/owned.img");
        let file = fs::File::create(&path).unwrap();
        file.set_len(64 * 1024 * 1024 + tail).unwrap();
        let metadata = file.metadata().unwrap();
        let identity = format!("file:{}:{}", metadata.dev(), metadata.ino());
        let inputs = vec![DiskInput {
            backend: DiskBackend::File,
            path: path.to_string_lossy().into_owned(),
            backing_identity: Some(identity.clone()),
            resource_identity: Some(identity.clone()),
        }];
        let record = DiskStorageRecord {
            version: 2,
            operation_id: uuid::Uuid::new_v4().to_string(),
            rack_id: 1,
            node_id: 2,
            disk_group_id: 3,
            disk_id: DiskId { high: 4, low: 5 },
            backend: DiskBackend::File,
            backing_identity: identity.clone(),
            physical_backing: Some(PhysicalBacking {
                physical_host_id: "owned-host".into(),
                resource_identity: identity,
                capacity_bytes: metadata.len(),
            }),
            media_type: None,
            sparse: false,
            removed: false,
            geometry: DiskValue {
                disk_type: 0,
                capacity_units: 64,
                zone_size_units: 64,
                unit_size_bytes: 1024 * 1024,
                zone_count: 1,
                status: HwStatus::Init as i32,
                device_path: path.to_string_lossy().into_owned(),
            },
        };
        Self { root, record, inputs }
    }

    fn file(&self) -> fs::File {
        fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&self.record.geometry.device_path)
            .unwrap()
    }

    fn verify(&self, progress: impl FnMut(u64) -> io::Result<()>) -> io::Result<()> {
        verify_blank_medium_for_tests(self.root.path(), &self.inputs, &self.record, progress)
    }
}

#[test]
fn complete_usable_bytes_are_verified_without_touching_the_physical_tail() {
    let test = TestMedium::new(512);
    let file = test.file();
    let logical = 64 * 1024 * 1024;
    file.write_all_at(&[b'T'; 512], logical).unwrap();
    let before = file.metadata().unwrap();
    let mut offsets = Vec::new();
    test.verify(|count| {
        offsets.push(count);
        Ok(())
    })
    .unwrap();
    assert_eq!(offsets.first(), Some(&0));
    assert_eq!(offsets.last(), Some(&logical));
    assert_eq!(offsets.len(), 65);
    let mut tail = [0; 512];
    file.read_exact_at(&mut tail, logical).unwrap();
    assert_eq!(tail, [b'T'; 512]);
    let after = file.metadata().unwrap();
    assert_eq!(
        (before.ino(), before.len(), before.blocks()),
        (after.ino(), after.len(), after.blocks())
    );
}

#[test]
fn nonzero_near_the_end_is_not_mistaken_for_an_unsigned_blank_disk() {
    let test = TestMedium::new(0);
    let offset = 63 * 1024 * 1024 + 17;
    test.file().write_all_at(b"X", offset).unwrap();
    let error = test.verify(|_| Ok(())).unwrap_err();
    assert!(error.to_string().contains(&format!("byte {offset}")), "{error}");
    let mut byte = [0];
    test.file().read_exact_at(&mut byte, offset).unwrap();
    assert_eq!(byte, *b"X");
}

#[test]
fn interrupted_verification_restarts_from_zero_instead_of_trusting_partial_progress() {
    let test = TestMedium::new(0);
    let error = test
        .verify(|count| {
            if count >= 1024 * 1024 {
                Err(io::Error::other("cancelled"))
            } else {
                Ok(())
            }
        })
        .unwrap_err();
    assert!(error.to_string().contains("cancelled"));
    test.file().write_all_at(b"X", 0).unwrap();
    let error = test.verify(|_| Ok(())).unwrap_err();
    assert!(error.to_string().contains("byte 0"), "{error}");
}

#[test]
fn replacement_during_the_scan_does_not_gain_a_verified_descriptor() {
    let test = TestMedium::new(0);
    let path = Path::new(&test.record.geometry.device_path);
    let retained = test.root.path().join("retained.img");
    let mut replaced = false;
    let error = test
        .verify(|count| {
            if count > 0 && !replaced {
                fs::rename(path, &retained)?;
                fs::File::create(path)?.set_len(64 * 1024 * 1024)?;
                replaced = true;
            }
            Ok(())
        })
        .unwrap_err();
    assert!(error.to_string().contains("medium changed"), "{error}");
    assert_eq!(fs::metadata(retained).unwrap().len(), 64 * 1024 * 1024);
}

#[test]
fn unlisted_files_and_changed_lineage_are_never_adopted() {
    let mut test = TestMedium::new(0);
    assert!(verify_blank_medium_for_tests(test.root.path(), &[], &test.record, |_| Ok(())).is_err());
    test.record.physical_backing.as_mut().unwrap().resource_identity = "file:1:2".into();
    assert!(test.verify(|_| Ok(())).is_err());
    assert_eq!(test.file().metadata().unwrap().len(), 64 * 1024 * 1024);
}

async fn authority(test: &TestMedium) -> node_setup::TestAuthority {
    use crowdb_protocol::{
        common::{NodeValue, RackValue},
        diskdb::rpc::DiskGroupValue,
        mgmt::node::setup::{NodeSetupChoices, NodeSetupRequest},
    };
    let request = NodeSetupRequest {
        version: 1,
        discovery_id: uuid::Uuid::new_v4().to_string(),
        operation_id: uuid::Uuid::new_v4().to_string(),
        choices: NodeSetupChoices::default(),
    };
    let authority = node_setup::TestAuthority::start(test.root.path(), &request).await;
    assert_eq!(authority.registry.nodes[0].physical_host_id, "owned-host");
    let hardware = crowdb_paxos_kv_client::HardwareClient::new(crowdb_paxos_kv_client::PaxosKvClient::new(
        crowdb_paxos_kv_client::ClientConfig::new(authority.binding.management_seeds.clone()),
    ));
    hardware
        .add_rack(
            1,
            &RackValue {
                node_ids: vec![2],
                ..Default::default()
            },
        )
        .await
        .unwrap();
    hardware
        .add_node(
            1,
            2,
            &NodeValue {
                disk_group_ids: vec![3],
                ..Default::default()
            },
        )
        .await
        .unwrap();
    hardware
        .add_disk_group(1, 2, 3, &DiskGroupValue::default())
        .await
        .unwrap();
    authority
}

#[tokio::test]
async fn verified_medium_publishes_init_and_waits_for_real_diskio_without_reinitializing() {
    use crowdb_protocol::{
        key::DiskKey,
        mgmt::node::storage::{PrepareSuppliedDisk, SuppliedPreparationPhase},
    };
    let test = TestMedium::new(512);
    let authority = authority(&test).await;
    let request = PrepareSuppliedDisk {
        cluster_id: authority.binding.bootstrap.cluster_id.clone(),
        record: test.record.clone(),
        initialize: true,
    };
    let first = crowdb_monitor::prepare_supplied_disk_for_tests(
        test.root.path(),
        request.clone(),
        test.inputs.clone(),
    )
    .await
    .unwrap();
    assert_eq!(first.phase, SuppliedPreparationPhase::WaitingForDiskio);
    let crowdb_paxos_kv_client::GetOutcome::Found { value, .. } = authority
        .kv
        .get(
            0,
            0,
            test.record.key().as_bytes(),
            crowdb_paxos_kv_client::ReadMode::Linearizable,
            None,
        )
        .await
        .unwrap()
    else {
        panic!("verified backing receipt missing")
    };
    assert_eq!(
        serde_json::from_slice::<DiskStorageRecord>(&value).unwrap(),
        test.record
    );
    let hardware = crowdb_paxos_kv_client::HardwareClient::new(crowdb_paxos_kv_client::PaxosKvClient::new(
        crowdb_paxos_kv_client::ClientConfig::new(authority.binding.management_seeds.clone()),
    ));
    let key = DiskKey {
        rack_id: 1,
        node_id: 2,
        disk_group_id: 3,
        disk_id: test.record.disk_id,
    };
    let before = hardware.get_disk_with_revision(&key).await.unwrap().unwrap();
    assert_eq!(before.0.status, HwStatus::Init as i32);
    test.file().write_all_at(b"X", 0).unwrap();
    let second =
        crowdb_monitor::prepare_supplied_disk_for_tests(test.root.path(), request, test.inputs.clone())
            .await
            .unwrap();
    assert_eq!(second.phase, SuppliedPreparationPhase::WaitingForDiskio);
    assert_eq!(hardware.get_disk_with_revision(&key).await.unwrap(), Some(before));
    let mut byte = [0];
    test.file().read_exact_at(&mut byte, 0).unwrap();
    assert_eq!(byte, *b"X");
}

#[tokio::test]
async fn nonblank_provisioning_never_publishes_membership_and_changed_retry_cannot_rewrite_intent() {
    use crowdb_protocol::{key::DiskKey, mgmt::node::storage::PrepareSuppliedDisk};
    let test = TestMedium::new(0);
    let authority = authority(&test).await;
    test.file().write_all_at(b"X", 63 * 1024 * 1024).unwrap();
    let mut request = PrepareSuppliedDisk {
        cluster_id: authority.binding.bootstrap.cluster_id.clone(),
        record: test.record.clone(),
        initialize: true,
    };
    let error = crowdb_monitor::prepare_supplied_disk_for_tests(
        test.root.path(),
        request.clone(),
        test.inputs.clone(),
    )
    .await
    .unwrap_err();
    assert!(error.to_string().contains("not blank"), "{error}");
    let hardware = crowdb_paxos_kv_client::HardwareClient::new(crowdb_paxos_kv_client::PaxosKvClient::new(
        crowdb_paxos_kv_client::ClientConfig::new(authority.binding.management_seeds.clone()),
    ));
    let key = DiskKey {
        rack_id: 1,
        node_id: 2,
        disk_group_id: 3,
        disk_id: test.record.disk_id,
    };
    assert!(hardware.get_disk_with_revision(&key).await.unwrap().is_none());
    let path = test
        .root
        .path()
        .join(format!("disk-supplied-{}.json", request.record.operation_id));
    let before = fs::read(&path).unwrap();
    request.record.geometry.device_path.push_str("-changed");
    let error =
        crowdb_monitor::prepare_supplied_disk_for_tests(test.root.path(), request, test.inputs.clone())
            .await
            .unwrap_err();
    assert!(error.to_string().contains("inputs changed"), "{error}");
    assert_eq!(fs::read(path).unwrap(), before);
}

#[test]
fn supplied_pages_resume_durable_rows_and_distinguish_missing_from_corrupt_records() {
    use crowdb_monitor::SuppliedOperationsForTests as Operations;
    use crowdb_protocol::mgmt::node::storage::PrepareSuppliedDisk;
    let test = TestMedium::new(0);
    let mut ids = Vec::new();
    for _ in 0..18 {
        let mut record = test.record.clone();
        record.operation_id = uuid::Uuid::new_v4().to_string();
        ids.push(record.operation_id.clone());
        Operations::accept_for_tests(
            test.root.path(),
            PrepareSuppliedDisk {
                cluster_id: "cluster".into(),
                record,
                initialize: true,
            },
            &test.inputs,
        )
        .unwrap();
    }
    ids.sort();
    let first = Operations::list_for_tests(test.root.path(), None, 16).unwrap();
    assert_eq!(first.items.len(), 16);
    assert_eq!(first.next.as_ref(), Some(&ids[15]));
    let second = Operations::list_for_tests(test.root.path(), first.next.as_deref(), 16).unwrap();
    assert_eq!(second.items.len(), 2);
    assert!(second.next.is_none());
    assert_eq!(second.items[1].request.record.operation_id, ids[17]);
    assert!(Operations::list_for_tests(test.root.path(), None, 17).is_err());
    assert!(Operations::list_for_tests(test.root.path(), Some("not-a-uuid"), 16).is_err());
    let unknown = uuid::Uuid::new_v4().to_string();
    assert!(Operations::lookup_for_tests(test.root.path(), &unknown)
        .unwrap()
        .is_none());
    fs::write(
        test.root.path().join(format!("disk-supplied-{unknown}.json")),
        b"corrupt",
    )
    .unwrap();
    assert!(Operations::lookup_for_tests(test.root.path(), &unknown).is_err());
    let saved = test.root.path().join(format!("disk-supplied-{}.json", ids[0]));
    assert_eq!(fs::metadata(saved).unwrap().permissions().mode() & 0o777, 0o600);
}

#[tokio::test]
async fn missing_target_group_fails_before_blank_scan_and_global_resource_claim() {
    use crowdb_protocol::mgmt::node::storage::PrepareSuppliedDisk;
    let test = TestMedium::new(0);
    let authority = authority(&test).await;
    let mut record = test.record.clone();
    record.disk_group_id = 99;
    let error = crowdb_monitor::prepare_supplied_disk_for_tests(
        test.root.path(),
        PrepareSuppliedDisk {
            cluster_id: authority.binding.bootstrap.cluster_id.clone(),
            record: record.clone(),
            initialize: true,
        },
        test.inputs.clone(),
    )
    .await
    .unwrap_err();
    assert!(error.to_string().contains("existing DiskGroup"), "{error}");
    let current =
        crowdb_monitor::SuppliedOperationsForTests::lookup_for_tests(test.root.path(), &record.operation_id)
            .unwrap()
            .unwrap();
    assert_eq!(current.verified_bytes, 0);
    let resources = authority
        .kv
        .get(
            0,
            0,
            record.key().as_bytes(),
            crowdb_paxos_kv_client::ReadMode::Linearizable,
            None,
        )
        .await
        .unwrap();
    assert!(matches!(resources, crowdb_paxos_kv_client::GetOutcome::NotFound));
    let physical = record.physical_backing.as_ref().unwrap();
    let encode = |text: &str| {
        text.as_bytes().iter().fold(String::new(), |mut encoded, byte| {
            write!(&mut encoded, "{byte:02x}").unwrap();
            encoded
        })
    };
    let path = format!(
        "/hw/disk-resource/{}/{}",
        encode(&physical.physical_host_id),
        encode(&physical.resource_identity)
    );
    let claim = authority
        .kv
        .get(
            0,
            0,
            path.as_bytes(),
            crowdb_paxos_kv_client::ReadMode::Linearizable,
            None,
        )
        .await
        .unwrap();
    assert!(matches!(claim, crowdb_paxos_kv_client::GetOutcome::NotFound));
    assert!(
        fs::metadata(test.root.path().join("disks/owned.img"))
            .unwrap()
            .len()
            > 0
    );
}
