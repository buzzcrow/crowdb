// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_diskio_client::{
    validate_topology_for_tests, DiskId, DiskioError, TestTopologyDisk, TestTopologyInstance,
};

fn instance(endpoint: &str, groups: Vec<u64>) -> TestTopologyInstance {
    TestTopologyInstance {
        instance_id: 10,
        endpoint: endpoint.into(),
        rack_id: Some(1),
        node_id: Some(2),
        disk_group_ids: groups,
    }
}

fn disk(group: u64) -> TestTopologyDisk {
    TestTopologyDisk {
        disk_id: DiskId::new(3, group),
        rack_id: 1,
        node_id: 2,
        disk_group_id: group,
    }
}

#[test]
fn topology_requires_one_matching_authoritative_owner() {
    assert_eq!(
        validate_topology_for_tests(vec![instance("http://127.0.0.1:100", vec![7])], vec![disk(7)])
            .expect("valid topology"),
        1
    );

    for result in [
        validate_topology_for_tests(Vec::new(), vec![disk(7)]),
        validate_topology_for_tests(
            vec![
                instance("127.0.0.1:100", vec![7]),
                TestTopologyInstance {
                    instance_id: 11,
                    endpoint: "127.0.0.1:101".into(),
                    rack_id: Some(1),
                    node_id: Some(2),
                    disk_group_ids: vec![7],
                },
            ],
            vec![disk(7)],
        ),
        validate_topology_for_tests(
            vec![TestTopologyInstance {
                node_id: Some(99),
                ..instance("127.0.0.1:100", vec![7])
            }],
            vec![disk(7)],
        ),
        validate_topology_for_tests(vec![instance("malformed", vec![7])], vec![disk(7)]),
        validate_topology_for_tests(
            vec![TestTopologyInstance {
                rack_id: None,
                ..instance("127.0.0.1:100", vec![7])
            }],
            vec![disk(7)],
        ),
    ] {
        assert!(matches!(result, Err(DiskioError::TopologyInconsistent(_))));
    }
}
