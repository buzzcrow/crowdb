// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_chunk_kv_client::{CatalogCache, CatalogMap, ClientError, RequestIdentityAllocator};
use crowdb_protocol::chunk_kv::{
    CatalogEntry, CatalogHead, CatalogPage, CatalogPageRef, CatalogPartitionState, Id128, KeyRange,
    OwnerDescriptor, PartitionArtifact,
};
use crowdb_protocol::chunk_stream::StreamName;

fn entry(id: u64, start: &[u8], end: Option<&[u8]>) -> CatalogEntry {
    CatalogEntry {
        partition_id: Id128 { high: 1, low: id },
        range: KeyRange {
            start: start.to_vec(),
            end: end.map(<[u8]>::to_vec),
        },
        owner: OwnerDescriptor {
            instance_id: id,
            rpc_endpoint: format!("owner-{id}"),
        },
        owner_epoch: 1,
        state: CatalogPartitionState::Serving,
        artifact: PartitionArtifact {
            tree_manifest: id,
            stream_name: StreamName { high: 2, low: id },
            applied_seq: 0,
        },
        transition_id: None,
    }
}

fn catalog(generation: u64, entries: Vec<CatalogEntry>) -> (CatalogHead, Vec<CatalogPage>) {
    let mut page = CatalogPage {
        generation,
        page_index: 0,
        entries,
        checksum: [0; 32],
    };
    page.seal().unwrap();
    let mut head = CatalogHead {
        generation,
        previous_generation: generation.checked_sub(1).filter(|previous| *previous != 0),
        pages: vec![CatalogPageRef {
            page_generation: generation,
            page_index: 0,
            first_key: Vec::new(),
            page_checksum: page.checksum,
        }],
        checksum: [0; 32],
    };
    head.seal().unwrap();
    (head, vec![page])
}

#[test]
fn complete_map_routes_every_boundary_once() {
    let (head, pages) = catalog(1, vec![entry(1, b"", Some(b"m")), entry(2, b"m", None)]);
    let map = CatalogMap::decode(&head, &pages).unwrap();
    assert_eq!(map.route(b"").unwrap().partition_id.low, 1);
    assert_eq!(map.route(b"l").unwrap().partition_id.low, 1);
    assert_eq!(map.route(b"m").unwrap().partition_id.low, 2);
    assert_eq!(map.route(&[0xff]).unwrap().partition_id.low, 2);
}

#[test]
fn invalid_or_regressing_map_preserves_warm_cache() {
    let cache = CatalogCache::default();
    let (head, pages) = catalog(2, vec![entry(1, b"", None)]);
    cache.install(CatalogMap::decode(&head, &pages).unwrap()).unwrap();
    let (older_head, older_pages) = catalog(1, vec![entry(1, b"", None)]);
    assert!(cache
        .install(CatalogMap::decode(&older_head, &older_pages).unwrap())
        .is_err());
    assert_eq!(cache.load().unwrap().generation(), 2);

    let (bad_head, mut bad_pages) = catalog(3, vec![entry(1, b"x", None)]);
    bad_pages[0].seal().unwrap();
    assert!(CatalogMap::decode(&bad_head, &bad_pages).is_err());
    assert_eq!(cache.load().unwrap().generation(), 2);
}

#[test]
fn identity_is_nonzero_monotonic_and_stops_before_overflow() {
    let first_handle = RequestIdentityAllocator::new();
    let second_handle = RequestIdentityAllocator::new();
    assert_ne!(first_handle.client_instance_id(), Id128::default());
    assert_ne!(
        first_handle.client_instance_id(),
        second_handle.client_instance_id()
    );
    assert_eq!(first_handle.allocate().unwrap().client_sequence, 1);
    assert_eq!(first_handle.allocate().unwrap().client_sequence, 2);

    let exhausted = RequestIdentityAllocator::resume(Id128 { high: 1, low: 1 }, u64::MAX).unwrap();
    assert_eq!(exhausted.allocate(), Err(ClientError::SequenceExhausted));
}
