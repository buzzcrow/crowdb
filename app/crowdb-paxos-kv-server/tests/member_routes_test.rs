// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_paxos_kv::cluster::{group::PxGroup, PxLocalReplica, PxLocalReplicaRole, PxRemoteReplica};
use crowdb_paxos_kv_server::recovery::member_routes::{apply, configure};
use crowdb_protocol::mgmt::{
    node::recovery::{MemberEndpoint, MemberEndpoints, RecoveryGroup, RecoveryGroups, RecoveryReplica},
    SystemBootstrapIdentity,
};
use crowdb_test_harness::test_dirs::tempdir_in_test_data;
use std::{
    os::unix::fs::PermissionsExt,
    path::Path,
    time::{SystemTime, UNIX_EPOCH},
};

fn write(path: &Path, value: &impl serde::Serialize) {
    std::fs::write(path, serde_json::to_vec(value).unwrap()).unwrap();
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600)).unwrap();
}

#[test]
fn native_routes_require_live_observer_proof_and_keep_canonical_membership() {
    let dir = tempdir_in_test_data("native-member-routes");
    let root = dir.path();
    let config = root.join("conf");
    let identity = SystemBootstrapIdentity {
        cluster_id: uuid::Uuid::new_v4().to_string(),
        operation_id: uuid::Uuid::new_v4().to_string(),
        configuration_digest: "a".repeat(64),
    };
    let local = MemberEndpoint {
        discovery_id: uuid::Uuid::new_v4().to_string(),
        node_id: 1,
        boot_id: uuid::Uuid::new_v4().to_string(),
        addresses: vec!["192.0.2.1".parse().unwrap()],
    };
    write(&root.join("node-endpoint.json"), &local);
    write(
        &root.join("accepted-node.json"),
        &serde_json::json!({"node_id":1,"bootstrap":identity}),
    );
    configure(&config, root).unwrap();
    write(
        &config.join("system-bootstrap.json"),
        &serde_json::json!({"identity":identity,"replica_id":1}),
    );
    write(
        &root.join("recovery-groups.json"),
        &RecoveryGroups {
            bootstrap: identity.clone(),
            groups: vec![RecoveryGroup {
                store_id: 0,
                group_id: 0,
                deployment: None,
                members: vec![
                    RecoveryReplica {
                        replica_id: 1,
                        node_id: 1,
                    },
                    RecoveryReplica {
                        replica_id: 2,
                        node_id: 2,
                    },
                ],
            }],
        },
    );
    let mut group = PxGroup::new(0, PxLocalReplica::new(1, PxLocalReplicaRole::Follower));
    group.set_remote_replicas(vec![PxRemoteReplica::new(2, "192.0.2.2:10100".into())]);
    apply(&group, &config, 0).unwrap();
    assert!(group.status().remotes[0].endpoint.is_empty());
    let mut peer = local.clone();
    peer.node_id = 2;
    peer.discovery_id = uuid::Uuid::new_v4().to_string();
    peer.addresses = vec!["192.0.2.3".parse().unwrap()];
    let mut snapshot = MemberEndpoints {
        version: 1,
        bootstrap: identity,
        observer_boot_id: local.boot_id.clone(),
        observed_at_unix_ms: SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis()
            .try_into()
            .unwrap(),
        members: vec![local, peer],
        conflicts: vec![],
    };
    write(&root.join("member-endpoints.json"), &snapshot);
    apply(&group, &config, 0).unwrap();
    assert_eq!(group.status().remotes[0].endpoint, "192.0.2.3:10100");
    assert_eq!(group.remote_replica_info(), vec![(2, "192.0.2.2:10100", true)]);
    snapshot.observer_boot_id = uuid::Uuid::new_v4().to_string();
    write(&root.join("member-endpoints.json"), &snapshot);
    apply(&group, &config, 0).unwrap();
    assert!(group.status().remotes[0].endpoint.is_empty());
    std::fs::write(config.join("member-route-source.json"), "corrupt").unwrap();
    assert!(apply(&group, &config, 0).is_err());
    assert!(group.status().remotes[0].endpoint.is_empty());
}
