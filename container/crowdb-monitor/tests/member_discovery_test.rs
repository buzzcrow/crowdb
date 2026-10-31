// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[path = "common/member_trust.rs"]
mod trust;

#[test]
fn pinned_retained_contacts_are_probed_when_multicast_discovery_is_empty() {
    let root = crowdb_test_harness::test_dirs::tempdir_in_test_data("member-recovery-contacts");
    let trust = trust::fixture(root.path());
    let operation = crowdb_console_shared::deployment::PreparedBootstrap::load(
        &root.path().join("prepared-bootstrap.json"),
    )
    .unwrap();
    let mut config = operation.intent.to_config();
    for node in &mut config.nodes {
        node.ssh_port = 2200 + u16::try_from(node.id).unwrap();
        node.ssh_password = Some("discarded-test-password".into());
    }
    let mut foreign = config.nodes[0].clone();
    foreign.id = 99;
    foreign.host = "192.0.2.99".into();
    config.nodes.push(foreign);
    let probes = crowdb_monitor::member_recovery_destinations_for_tests(
        root.path(),
        &config,
        &trust,
        crowdb_protocol::mgmt::node::CandidateSnapshot::default(),
    )
    .unwrap();
    assert_eq!(probes.iter().map(|node| node.id).collect::<Vec<_>>(), [1, 2, 3]);
    for node in probes {
        assert_eq!(node.host, format!("192.0.2.{}", node.id));
        assert_eq!(node.ssh_port, 2200 + u16::try_from(node.id).unwrap());
        assert!(node.ssh_password.is_none());
        assert_eq!(
            node.ssh_key,
            Some(root.path().join("ssh/id_ed25519").to_string_lossy().into_owned())
        );
    }
}

#[test]
fn retained_contacts_do_not_crowd_a_changed_address_out_of_the_probe_budget() {
    use crowdb_protocol::mgmt::node::{CandidateNode, CandidateSnapshot, CandidateState, NodeAdvertisement};
    let root = crowdb_test_harness::test_dirs::tempdir_in_test_data("member-recovery-probe-budget");
    let mut trust = trust::fixture(root.path());
    let operation = crowdb_console_shared::deployment::PreparedBootstrap::load(
        &root.path().join("prepared-bootstrap.json"),
    )
    .unwrap();
    let mut config = operation.intent.to_config();
    let template_node = config.nodes[0].clone();
    let template_member = trust.members[0].clone();
    config.nodes.clear();
    trust.members.clear();
    for id in 1_u8..=64 {
        let mut node = template_node.clone();
        node.id = u64::from(id);
        node.host = format!("192.0.2.{id}");
        config.nodes.push(node);
        let mut member = template_member.clone();
        member.node_id = u64::from(id);
        member.discovery_id = uuid::Uuid::new_v4().to_string();
        member.admission_operation_id = uuid::Uuid::new_v4().to_string();
        member.host_key = trust::key(id);
        trust.members.push(member);
    }
    trust.validate().unwrap();
    let snapshot = CandidateSnapshot {
        nodes: vec![CandidateNode {
            advertisement: NodeAdvertisement {
                discovery_id: trust.members[63].discovery_id.clone(),
                protocol_version: 1,
                monitor_endpoints: vec!["http://203.0.113.64:9095".into()],
                cluster_id: Some(trust.bootstrap.cluster_id.clone()),
                formation: None,
            },
            state: CandidateState::SameCluster,
        }],
        discovery_diagnostics: Vec::new(),
    };
    let probes =
        crowdb_monitor::member_recovery_destinations_for_tests(root.path(), &config, &trust, snapshot)
            .unwrap();
    assert_eq!(probes.len(), 64);
    assert!(
        probes
            .iter()
            .any(|node| node.id == 64 && node.host == "203.0.113.64"),
        "a full retained-address cache must not suppress a member's discovered new address"
    );
}
