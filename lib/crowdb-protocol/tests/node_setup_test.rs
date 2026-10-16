// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_protocol::mgmt::node::{
    setup::{NodeSetupChoices, NodeSetupRequest, SetupMedium, SetupStorage},
    storage::{DiskBackend, DEFAULT_FILE_CAPACITY_BYTES, DEFAULT_FILE_RESERVE_BYTES},
};
use serde_json::json;

#[test]
fn progress_is_bound_to_admission_before_and_after_cluster_binding() {
    use crowdb_protocol::mgmt::node::setup::{NodeSetupPhase, NodeSetupStatus};
    let intent = request();
    let status = NodeSetupStatus {
        version: 1,
        discovery_id: intent.discovery_id.clone(),
        operation_id: intent.operation_id.clone(),
        phase: NodeSetupPhase::WaitingForBinding,
        storage_mode: None,
        node_id: None,
        cluster_id: None,
        disks: vec![],
        services: vec![],
        error: None,
    };
    let validate = |status: &NodeSetupStatus| {
        status.validate_identity(&intent.discovery_id, &intent.operation_id, 7, Some("cluster"))
    };
    validate(&status).unwrap();
    let bound = NodeSetupStatus {
        node_id: Some(7),
        cluster_id: Some("cluster".into()),
        ..status.clone()
    };
    validate(&bound).unwrap();
    for changed in [
        NodeSetupStatus {
            version: 2,
            ..bound.clone()
        },
        NodeSetupStatus {
            discovery_id: "foreign".into(),
            ..bound.clone()
        },
        NodeSetupStatus {
            operation_id: "foreign".into(),
            ..bound.clone()
        },
        NodeSetupStatus {
            node_id: Some(8),
            ..bound.clone()
        },
        NodeSetupStatus {
            cluster_id: Some("foreign".into()),
            ..bound.clone()
        },
    ] {
        assert!(validate(&changed).is_err());
    }
}

fn request() -> NodeSetupRequest {
    NodeSetupRequest {
        version: 1,
        discovery_id: "00000000-0000-4000-8000-000000000001".into(),
        operation_id: "00000000-0000-4000-8000-000000000002".into(),
        choices: NodeSetupChoices::default(),
    }
}

#[test]
fn dataset_listener_is_access_owned_and_cannot_collide() {
    let mut plan = request();
    plan.choices.services[5].listeners.dataset_port = Some(19093);
    plan.validate().unwrap();
    plan.choices.services[5].listeners.health_port = Some(19093);
    assert!(plan.validate().is_err());
    plan.choices.services[5].listeners.health_port = Some(19094);
    plan.validate().unwrap();
    plan.choices.services[2].listeners.dataset_port = Some(19095);
    assert!(plan.validate().is_err());
}

#[test]
fn omitted_choices_select_six_services_and_preallocated_standard_capacity() {
    let choices: NodeSetupChoices = serde_json::from_value(json!({})).unwrap();
    assert_eq!(choices, NodeSetupChoices::default());
    assert!(choices.services.iter().all(|service| service.enabled));
    assert_eq!(choices.services.len(), 6);
    assert_eq!(
        choices.storage,
        SetupStorage::Automatic {
            capacity_bytes: DEFAULT_FILE_CAPACITY_BYTES,
            reserve_bytes: DEFAULT_FILE_RESERVE_BYTES,
            sparse: false,
        }
    );
    request().validate().unwrap();
    let mut diskless = request();
    diskless.choices.storage = SetupStorage::Diskless;
    diskless.validate().unwrap();
}

#[test]
fn service_selection_and_reserved_diskdb_listener_range_cannot_be_ambiguous() {
    let mut plan = request();
    plan.choices.services[1].listeners.rpc_port = Some(32000);
    plan.choices.services[2].listeners.rpc_port = Some(32002);
    assert!(plan.validate().is_err());
    plan.choices.services[2].listeners.rpc_port = Some(32003);
    plan.validate().unwrap();
    plan.choices.services[1].listeners.rpc_port = Some(65534);
    assert!(plan.validate().is_err());
    let mut duplicated = request();
    duplicated.choices.services[1] = duplicated.choices.services[0].clone();
    assert!(duplicated.validate().is_err());
    let mut unsupported = request();
    unsupported.choices.services[0].listeners.s3_port = Some(32000);
    assert!(unsupported.validate().is_err());
    let mut fixed_bootstrap = request();
    fixed_bootstrap.choices.services[0].listeners.http_port = Some(19910);
    assert!(
        fixed_bootstrap.validate().is_err(),
        "node setup cannot silently ignore a bootstrap port override"
    );
    fixed_bootstrap.choices.services[0].listeners.http_port = Some(10000);
    fixed_bootstrap.choices.services[0].listeners.rpc_port = Some(10100);
    fixed_bootstrap.validate().unwrap();
}

#[test]
fn supplied_raw_input_requires_agreement_and_preserves_exact_observed_identity() {
    let medium = SetupMedium {
        path: "/opt/crowdb/data/disks/host-device".into(),
        backend: DiskBackend::BlockDevice,
        backing_identity: "device:1234".into(),
        resource_identity: Some("device:1234".into()),
        capacity_bytes: 1024 * 1024 * 1024,
        media_type: None,
        initialize: false,
    };
    let mut plan = request();
    plan.choices.storage = SetupStorage::Supplied {
        disks: vec![medium.clone()],
    };
    assert!(plan.validate().is_err());
    let mut authorized = medium;
    authorized.initialize = true;
    plan.choices.storage = SetupStorage::Supplied {
        disks: vec![authorized.clone()],
    };
    plan.validate().unwrap();
    let decoded: NodeSetupRequest = serde_json::from_slice(&serde_json::to_vec(&plan).unwrap()).unwrap();
    assert_eq!(decoded, plan);
    let mut unverified = authorized.clone();
    unverified.resource_identity = None;
    plan.choices.storage = SetupStorage::Supplied {
        disks: vec![unverified],
    };
    assert!(plan.validate().is_err());
    let mut alias = authorized.clone();
    alias.path = "/opt/crowdb/data/disks/alias".into();
    alias.backing_identity = "device:5678".into();
    plan.choices.storage = SetupStorage::Supplied {
        disks: vec![authorized, alias],
    };
    assert!(plan.validate().is_err());
}

#[test]
fn invalid_default_geometry_versions_and_unrecognized_fields_fail_closed() {
    let mut plan = request();
    plan.choices.storage = SetupStorage::Automatic {
        capacity_bytes: 1,
        reserve_bytes: DEFAULT_FILE_RESERVE_BYTES,
        sparse: false,
    };
    assert!(plan.validate().is_err());
    plan = request();
    plan.version = 2;
    assert!(plan.validate().is_err());
    let mut encoded = serde_json::to_value(request()).unwrap();
    encoded["choices"]["storage"]["fallback"] = json!(true);
    assert!(serde_json::from_value::<NodeSetupRequest>(encoded).is_err());
}
