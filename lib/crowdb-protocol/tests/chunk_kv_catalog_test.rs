// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_protocol::chunk_kv::{
    CatalogEntry, CatalogHead, CatalogPage, CatalogPageRef, CatalogPartitionState, ChunkKvProtocolError,
    DomainFailurePolicy, DomainMonitorDescriptor, Id128, KeyRange, OwnerDescriptor, PartitionArtifact,
    ServingAssignment, ServingGrant,
};
use crowdb_protocol::chunk_stream::StreamName;

fn entry(id: u64, start: &[u8], end: Option<&[u8]>, epoch: u64) -> CatalogEntry {
    CatalogEntry {
        partition_id: Id128 { high: 1, low: id },
        range: KeyRange {
            start: start.to_vec(),
            end: end.map(<[u8]>::to_vec),
        },
        owner: OwnerDescriptor {
            instance_id: id,
            rpc_endpoint: format!("127.0.0.1:{}", 9000 + id),
        },
        owner_epoch: epoch,
        state: CatalogPartitionState::Serving,
        artifact: PartitionArtifact {
            tree_manifest: 10 + id,
            stream_name: StreamName { high: 2, low: id },
            applied_seq: 20,
        },
        transition_id: None,
    }
}

fn catalog(entries: Vec<CatalogEntry>) -> (CatalogHead, Vec<CatalogPage>) {
    let mut page = CatalogPage {
        generation: 3,
        page_index: 0,
        entries,
        checksum: [0; 32],
    };
    page.seal().unwrap();
    let mut head = CatalogHead {
        generation: 3,
        previous_generation: Some(2),
        pages: vec![CatalogPageRef {
            page_generation: 3,
            page_index: 0,
            first_key: page.entries[0].range.start.clone(),
            page_checksum: page.checksum,
        }],
        checksum: [0; 32],
    };
    head.seal().unwrap();
    (head, vec![page])
}

#[test]
fn complete_catalog_accepts_adjacent_binary_ranges() {
    let (head, pages) = catalog(vec![entry(1, b"", Some(b"m"), 4), entry(2, b"m", None, 7)]);
    head.validate_pages(&pages).unwrap();
    assert!(pages[0].entries[0].range.contains(b""));
    assert!(!pages[0].entries[0].range.contains(b"m"));
    assert!(pages[0].entries[1].range.contains(b"\xff"));
}

#[test]
fn catalog_rejects_holes_overlaps_and_checksum_changes() {
    let (head, pages) = catalog(vec![entry(1, b"", Some(b"k"), 4), entry(2, b"m", None, 7)]);
    assert_eq!(
        head.validate_pages(&pages),
        Err(ChunkKvProtocolError::IncompleteKeyspace)
    );

    let (head, pages) = catalog(vec![entry(1, b"", Some(b"n"), 4), entry(2, b"m", None, 7)]);
    assert_eq!(
        head.validate_pages(&pages),
        Err(ChunkKvProtocolError::IncompleteKeyspace)
    );

    let (head, mut pages) = catalog(vec![entry(1, b"", None, 4)]);
    pages[0].entries[0].owner_epoch += 1;
    assert_eq!(
        head.validate_pages(&pages),
        Err(ChunkKvProtocolError::InvalidCatalogPage)
    );
}

#[test]
fn serving_grant_digest_binds_sorted_partition_epochs() {
    let mut grant = ServingGrant {
        instance_id: 9,
        lease_sequence: 2,
        catalog_generation: 5,
        issued_at_ms: 100,
        expires_at_ms: 12_100,
        assignments: vec![
            ServingAssignment {
                partition_id: Id128 { high: 1, low: 2 },
                owner_epoch: 8,
            },
            ServingAssignment {
                partition_id: Id128 { high: 1, low: 1 },
                owner_epoch: 7,
            },
        ],
        assignment_digest: [0; 32],
    };
    grant.seal();
    grant.validate().unwrap();
    grant.assignments[0].owner_epoch += 1;
    assert_eq!(grant.validate(), Err(ChunkKvProtocolError::InvalidServingGrant));
}

#[test]
fn monitor_descriptor_enforces_safe_timing_order() {
    let descriptor = DomainMonitorDescriptor {
        domain: "chunk-kv".into(),
        service_registry_name: "chunk-kv".into(),
        driver_version: 1,
        capability_version: 1,
        heartbeat_interval_ms: 2_000,
        suspect_after_ms: 6_000,
        dead_after_ms: 10_000,
        lease_duration_ms: 12_000,
        max_clock_skew_ms: 1_000,
        self_fence_margin_ms: 1_000,
        failure_policy: DomainFailurePolicy::AutomaticSharedStorage,
        balance_policy: "count-first-v1".into(),
    };
    descriptor.validate().unwrap();
    let mut unsafe_descriptor = descriptor;
    unsafe_descriptor.lease_duration_ms = 10_000;
    assert_eq!(
        unsafe_descriptor.validate(),
        Err(ChunkKvProtocolError::InvalidMonitorDescriptor)
    );
}

#[test]
fn successor_reuses_unchanged_pages_and_rejects_epoch_regression() {
    let (previous, previous_pages) = catalog(vec![entry(1, b"", None, 4)]);
    let reused_page = previous_pages[0].clone();
    let mut reused_head = CatalogHead {
        generation: 4,
        previous_generation: Some(3),
        pages: vec![CatalogPageRef {
            page_generation: reused_page.generation,
            page_index: reused_page.page_index,
            first_key: Vec::new(),
            page_checksum: reused_page.checksum,
        }],
        checksum: [0; 32],
    };
    reused_head.seal().unwrap();
    reused_head
        .validate_successor(&[reused_page], &previous, &previous_pages)
        .unwrap();

    let mut regressed_page = CatalogPage {
        generation: 4,
        page_index: 0,
        entries: vec![entry(1, b"", None, 3)],
        checksum: [0; 32],
    };
    regressed_page.seal().unwrap();
    let mut regressed_head = CatalogHead {
        generation: 4,
        previous_generation: Some(3),
        pages: vec![CatalogPageRef {
            page_generation: 4,
            page_index: 0,
            first_key: Vec::new(),
            page_checksum: regressed_page.checksum,
        }],
        checksum: [0; 32],
    };
    regressed_head.seal().unwrap();
    assert_eq!(
        regressed_head.validate_successor(&[regressed_page], &previous, &previous_pages),
        Err(ChunkKvProtocolError::CatalogRegression)
    );
}
