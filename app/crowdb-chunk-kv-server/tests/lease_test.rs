// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_chunk_kv_server::{classify_instance, replacement_may_activate, AuthorityError, ServingAuthority};
use crowdb_protocol::chunk_kv::{
    DomainFailurePolicy, DomainMonitorDescriptor, Id128, InstanceHealth, ServingAssignment, ServingGrant,
};

fn policy() -> DomainMonitorDescriptor {
    DomainMonitorDescriptor {
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
    }
}

fn assignment(epoch: u64) -> ServingAssignment {
    ServingAssignment {
        partition_id: Id128 { high: 4, low: 5 },
        owner_epoch: epoch,
    }
}

fn grant(sequence: u64, epoch: u64) -> ServingGrant {
    let mut grant = ServingGrant {
        instance_id: 7,
        lease_sequence: sequence,
        catalog_generation: 3,
        issued_at_ms: 1_000,
        expires_at_ms: 13_000,
        assignments: vec![assignment(epoch)],
        assignment_digest: [0; 32],
    };
    grant.seal();
    grant
}

#[test]
fn default_timing_fences_before_replacement_can_activate() {
    let policy = policy();
    assert_eq!(classify_instance(1_000, 7_000, &policy), InstanceHealth::Suspect);
    assert_eq!(classify_instance(1_000, 11_000, &policy), InstanceHealth::Dead);

    let authority = ServingAuthority::new(7);
    authority.install(grant(1, 9), &policy, 1_000, 50_000).unwrap();
    assert!(authority
        .authorize(3, assignment(9).partition_id, 9, 59_999)
        .is_ok());
    assert_eq!(
        authority.authorize(3, assignment(9).partition_id, 9, 60_000),
        Err(AuthorityError::LeaseExpired)
    );
    assert!(!replacement_may_activate(13_000, 13_999, 1_000, false));
    assert!(replacement_may_activate(13_000, 14_000, 1_000, false));
    assert!(replacement_may_activate(13_000, 2_000, 1_000, true));
}

#[test]
fn grant_authorizes_only_exact_catalog_partition_and_epoch() {
    let authority = ServingAuthority::new(7);
    authority.install(grant(4, 11), &policy(), 1_000, 8_000).unwrap();
    let partition = assignment(11).partition_id;
    assert!(authority.authorize(3, partition, 11, 8_001).is_ok());
    assert_eq!(
        authority.authorize(2, partition, 11, 8_001),
        Err(AuthorityError::StaleCatalog)
    );
    assert_eq!(
        authority.authorize(3, partition, 10, 8_001),
        Err(AuthorityError::NotAssigned)
    );
}

#[test]
fn renewal_is_monotonic_and_epoch_change_revokes_old_assignment() {
    let authority = ServingAuthority::new(7);
    authority.install(grant(4, 11), &policy(), 1_000, 8_000).unwrap();
    authority.install(grant(5, 12), &policy(), 1_100, 8_100).unwrap();
    assert_eq!(
        authority.install(grant(4, 11), &policy(), 1_200, 8_200),
        Err(AuthorityError::StaleGrant)
    );
    let partition = assignment(12).partition_id;
    assert_eq!(
        authority.authorize(3, partition, 11, 8_201),
        Err(AuthorityError::NotAssigned)
    );
    assert!(authority.authorize(3, partition, 12, 8_201).is_ok());
}
