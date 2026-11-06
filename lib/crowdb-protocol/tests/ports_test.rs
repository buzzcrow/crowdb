// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Tests for `crowdb-protocol::ports` — default port allocation,
//! stride rules, and non-overlap across service types.

use crowdb_protocol::ServicePort;
use crowdb_protocol::{
    ACCESS_SERVER_DATASET_HTTP_BASE, ACCESS_SERVER_HEALTH_HTTP_BASE, ACCESS_SERVER_HTTP_BASE,
    ACCESS_SERVER_ICEBERG_HTTP_BASE, CHUNKDB_HTTP_BASE, CHUNKDB_RPC_BASE, CHUNK_KV_HTTP_BASE,
    CHUNK_KV_RPC_BASE, DISKDB_HTTP_BASE, DISKDB_RPC_BASE, DISKIO_RPC_BASE, PAXOS_KV_SERVER_MGMT_BASE,
    PAXOS_KV_SERVER_RPC_BASE, WEB_BASE,
};

// ── base constants match enum ──────────────────────────────────

#[test]
fn base_constants_match_enum_base() {
    assert_eq!(ServicePort::PaxosKvServerMgmt.base(), PAXOS_KV_SERVER_MGMT_BASE);
    assert_eq!(ServicePort::PaxosKvServerRpc.base(), PAXOS_KV_SERVER_RPC_BASE);
    assert_eq!(ServicePort::DiskdbHttp.base(), DISKDB_HTTP_BASE);
    assert_eq!(ServicePort::DiskdbRpc.base(), DISKDB_RPC_BASE);
    assert_eq!(ServicePort::ChunkdbHttp.base(), CHUNKDB_HTTP_BASE);
    assert_eq!(ServicePort::ChunkdbRpc.base(), CHUNKDB_RPC_BASE);
    assert_eq!(ServicePort::DiskioRpc.base(), DISKIO_RPC_BASE);
    assert_eq!(ServicePort::Web.base(), WEB_BASE);
    assert_eq!(ServicePort::ChunkKvHttp.base(), CHUNK_KV_HTTP_BASE);
    assert_eq!(ServicePort::ChunkKvRpc.base(), CHUNK_KV_RPC_BASE);
    assert_eq!(ServicePort::AccessServerHttp.base(), ACCESS_SERVER_HTTP_BASE);
    assert_eq!(
        ServicePort::AccessServerIcebergHttp.base(),
        ACCESS_SERVER_ICEBERG_HTTP_BASE
    );
    assert_eq!(
        ServicePort::AccessServerDatasetHttp.base(),
        ACCESS_SERVER_DATASET_HTTP_BASE
    );
}

// ── known defaults (default port map) ──────────────────

#[test]
fn known_base_ports() {
    assert_eq!(PAXOS_KV_SERVER_MGMT_BASE, 10135);
    assert_eq!(PAXOS_KV_SERVER_RPC_BASE, 11135);
    assert_eq!(DISKDB_HTTP_BASE, 13135);
    assert_eq!(DISKDB_RPC_BASE, 14135);
    assert_eq!(CHUNKDB_HTTP_BASE, 16135);
    assert_eq!(CHUNKDB_RPC_BASE, 17135);
    assert_eq!(DISKIO_RPC_BASE, 18135);
    assert_eq!(WEB_BASE, 6135);
    assert_eq!(CHUNK_KV_HTTP_BASE, 20135);
    assert_eq!(CHUNK_KV_RPC_BASE, 21135);
    assert_eq!(ACCESS_SERVER_HTTP_BASE, 7135);
    assert_eq!(ACCESS_SERVER_ICEBERG_HTTP_BASE, 8135);
    assert_eq!(ACCESS_SERVER_DATASET_HTTP_BASE, 9135);
    assert_eq!(
        ServicePort::AccessServerHealthHttp.base(),
        ACCESS_SERVER_HEALTH_HTTP_BASE
    );
    assert_eq!(ACCESS_SERVER_HEALTH_HTTP_BASE, 25135);
}

// ── stride (all stride 1 — no paired-port logic) ───────────────

#[test]
fn all_services_have_stride_one() {
    for svc in [
        ServicePort::PaxosKvServerMgmt,
        ServicePort::PaxosKvServerRpc,
        ServicePort::DiskdbHttp,
        ServicePort::DiskdbRpc,
        ServicePort::ChunkdbHttp,
        ServicePort::ChunkdbRpc,
        ServicePort::DiskioRpc,
        ServicePort::Web,
        ServicePort::ChunkKvHttp,
        ServicePort::ChunkKvRpc,
        ServicePort::AccessServerHttp,
        ServicePort::AccessServerIcebergHttp,
        ServicePort::AccessServerDatasetHttp,
        ServicePort::AccessServerHealthHttp,
        ServicePort::MonitorHttp,
    ] {
        assert_eq!(svc.stride(), 1, "{svc:?} must have stride 1");
    }
}

// ── port(instance) computation ─────────────────────────────────

#[test]
fn port_instance_zero_is_base() {
    for svc in [
        ServicePort::PaxosKvServerMgmt,
        ServicePort::PaxosKvServerRpc,
        ServicePort::DiskdbHttp,
        ServicePort::DiskdbRpc,
        ServicePort::ChunkdbHttp,
        ServicePort::ChunkdbRpc,
        ServicePort::DiskioRpc,
        ServicePort::Web,
        ServicePort::ChunkKvHttp,
        ServicePort::ChunkKvRpc,
        ServicePort::AccessServerHttp,
        ServicePort::AccessServerIcebergHttp,
        ServicePort::AccessServerDatasetHttp,
        ServicePort::AccessServerHealthHttp,
        ServicePort::MonitorHttp,
    ] {
        assert_eq!(svc.port(0), svc.base());
    }
}

#[test]
fn port_increments_by_one() {
    assert_eq!(ServicePort::PaxosKvServerMgmt.port(0), 10135);
    assert_eq!(ServicePort::PaxosKvServerMgmt.port(1), 10136);
    assert_eq!(ServicePort::PaxosKvServerMgmt.port(99), 10234);

    assert_eq!(ServicePort::PaxosKvServerRpc.port(0), 11135);
    assert_eq!(ServicePort::PaxosKvServerRpc.port(1), 11136);
    assert_eq!(ServicePort::PaxosKvServerRpc.port(99), 11234);

    assert_eq!(ServicePort::DiskdbRpc.port(0), 14135);
    assert_eq!(ServicePort::DiskdbRpc.port(1), 14136);
    assert_eq!(ServicePort::DiskdbRpc.port(99), 14234);

    assert_eq!(ServicePort::DiskioRpc.port(0), 18135);
    assert_eq!(ServicePort::DiskioRpc.port(1), 18136);
    assert_eq!(ServicePort::DiskioRpc.port(99), 18234);
}

// ── non-overlap across service types ───────────────────────────

#[test]
fn port_ranges_do_not_overlap() {
    // Check every declared instance, including the edges of adjacent blocks.
    let all_services = [
        ServicePort::PaxosKvServerMgmt,
        ServicePort::PaxosKvServerRpc,
        ServicePort::DiskdbHttp,
        ServicePort::DiskdbRpc,
        ServicePort::ChunkdbHttp,
        ServicePort::ChunkdbRpc,
        ServicePort::DiskioRpc,
        ServicePort::Web,
        ServicePort::ChunkKvHttp,
        ServicePort::ChunkKvRpc,
        ServicePort::AccessServerHttp,
        ServicePort::AccessServerIcebergHttp,
        ServicePort::AccessServerDatasetHttp,
        ServicePort::AccessServerHealthHttp,
        ServicePort::MonitorHttp,
    ];

    let mut seen: std::collections::HashSet<u16> = std::collections::HashSet::new();
    for svc in all_services {
        let ports: std::collections::HashSet<u16> = (0..svc.range_size()).map(|i| svc.port(i)).collect();
        assert!(
            seen.is_disjoint(&ports),
            "{svc:?} ports overlap with a prior service type",
        );
        seen.extend(ports);
    }
}

// ── range_size ─────────────────────────────────────────────────

#[test]
fn range_sizes_match_listener_scopes() {
    for svc in [
        ServicePort::PaxosKvServerMgmt,
        ServicePort::PaxosKvServerRpc,
        ServicePort::DiskdbHttp,
        ServicePort::DiskdbRpc,
        ServicePort::ChunkdbHttp,
        ServicePort::ChunkdbRpc,
        ServicePort::DiskioRpc,
        ServicePort::Web,
        ServicePort::ChunkKvHttp,
        ServicePort::ChunkKvRpc,
        ServicePort::AccessServerHttp,
        ServicePort::AccessServerIcebergHttp,
        ServicePort::AccessServerDatasetHttp,
        ServicePort::AccessServerHealthHttp,
        ServicePort::MonitorHttp,
    ] {
        let expected = 1000;
        assert_eq!(svc.range_size(), expected, "{svc:?} range_size");
    }
}

#[test]
fn access_listener_scopes_have_disjoint_thousand_port_blocks() {
    let services = [
        ServicePort::AccessServerHttp,
        ServicePort::AccessServerIcebergHttp,
        ServicePort::AccessServerDatasetHttp,
        ServicePort::AccessServerHealthHttp,
    ];
    let mut seen = std::collections::HashSet::new();
    let mut names = std::collections::HashSet::new();
    for service in services {
        assert!(names.insert(service.name()));
        for instance in 0..service.range_size() {
            let port = service.port(instance);
            assert!((7135..10135).contains(&port) || (25135..26135).contains(&port));
            assert!(seen.insert(port), "overlapping access listener port {port}");
        }
    }
    assert_eq!(seen.len(), 4000);
}

#[test]
fn management_and_rpc_defaults_are_one_thousand_apart() {
    for (management, rpc) in [
        (ServicePort::PaxosKvServerMgmt, ServicePort::PaxosKvServerRpc),
        (ServicePort::DiskdbHttp, ServicePort::DiskdbRpc),
        (ServicePort::ChunkdbHttp, ServicePort::ChunkdbRpc),
        (ServicePort::ChunkKvHttp, ServicePort::ChunkKvRpc),
    ] {
        assert_eq!(rpc.base() - management.base(), 1000);
        for offset in 0..3 {
            assert_eq!(management.port(offset), management.base() + offset);
        }
        assert_eq!(management.base() % 1000, 135);
    }
}
