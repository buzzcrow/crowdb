// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_monitor::CandidateCache;
use crowdb_protocol::mgmt::node::{CandidateState, NodeAdvertisement, NODE_PROTOCOL_VERSION};
use uuid::Uuid;

fn node() -> NodeAdvertisement {
    NodeAdvertisement {
        discovery_id: Uuid::new_v4().to_string(),
        protocol_version: NODE_PROTOCOL_VERSION,
        monitor_endpoints: vec!["http://192.0.2.1:9093".into()],
        cluster_id: None,
    }
}

#[test]
fn conflicts_are_visible_and_expiry_only_removes_observations() {
    let local = node();
    let peer = node();
    let mut clone = peer.clone();
    clone.monitor_endpoints = vec!["http://192.0.2.2:9093".into()];
    let mut cache = CandidateCache::default();
    assert!(cache.observe("a".into(), peer.clone()));
    assert_eq!(cache.snapshot(&local)[0].state, CandidateState::Unbound);
    assert!(cache.observe("b".into(), clone));
    assert!(cache
        .snapshot(&local)
        .iter()
        .all(|node| node.state == CandidateState::IdentityConflict));
    cache.remove("b");
    assert_eq!(cache.snapshot(&local)[0].advertisement, peer);
    cache.remove("a");
    assert!(cache.snapshot(&local).is_empty());
}

#[test]
fn cluster_separation_and_compatibility_are_independent_of_admission() {
    let mut local = node();
    local.cluster_id = Some(Uuid::new_v4().to_string());
    let mut peer = node();
    peer.cluster_id = local.cluster_id.clone();
    let mut cache = CandidateCache::default();
    assert!(cache.observe("a".into(), peer.clone()));
    assert_eq!(cache.snapshot(&local)[0].state, CandidateState::SameCluster);
    peer.cluster_id = Some(Uuid::new_v4().to_string());
    assert!(cache.observe("a".into(), peer.clone()));
    assert_eq!(cache.snapshot(&local)[0].state, CandidateState::ForeignCluster);
    peer.protocol_version += 1;
    assert!(cache.observe("a".into(), peer));
    assert_eq!(cache.snapshot(&local)[0].state, CandidateState::Incompatible);
}

#[test]
fn malformed_metadata_is_not_a_candidate() {
    let mut peer = node();
    peer.discovery_id = "bad".into();
    let mut cache = CandidateCache::default();
    assert!(!cache.observe("a".into(), peer));
    let mut peer = node();
    peer.monitor_endpoints = vec!["http://user:password@192.0.2.1:9093".into()];
    assert!(!cache.observe("b".into(), peer));
    assert!(cache.snapshot(&node()).is_empty());
}
