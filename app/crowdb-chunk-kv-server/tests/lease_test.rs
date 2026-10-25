// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[path = "common/process.rs"]
mod test_process;

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
        chunk_kv_range_balance: Some(crowdb_protocol::chunk_kv::ChunkKvRangeBalancePolicy::default()),
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
        process_epoch: 1,
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
fn a_live_grant_for_another_boot_cannot_authorize_the_same_instance() {
    let authority = ServingAuthority::new(test_process::TestProcess::authority(7));
    let mut successor = grant(99, 11);
    successor.process_epoch = 2;
    assert_eq!(
        authority.install(successor, &policy(), 1_000, 8_000),
        Err(AuthorityError::WrongProcess)
    );
    assert!(!authority.has_live_grant(3, 8_001));
}

#[test]
fn process_revocation_invalidates_captured_grants_and_prevents_reinstallation() {
    let authority = ServingAuthority::new(test_process::TestProcess::authority(7));
    authority.install(grant(1, 11), &policy(), 1_000, 8_000).unwrap();
    let context = authority
        .capture(3, assignment(11).partition_id, 11, 8_001)
        .unwrap();
    authority.process().fence();
    assert_eq!(
        authority.validate_context(&context, 8_002),
        Err(AuthorityError::LeaseExpired)
    );
    assert!(!authority.has_live_grant(3, 8_002));
    assert_eq!(
        authority.install(grant(2, 11), &policy(), 1_100, 8_100),
        Err(AuthorityError::LeaseExpired)
    );
}

#[test]
fn default_timing_fences_before_replacement_can_activate() {
    let policy = policy();
    assert_eq!(classify_instance(1_000, 7_000, &policy), InstanceHealth::Suspect);
    assert_eq!(classify_instance(1_000, 11_000, &policy), InstanceHealth::Dead);

    let authority = ServingAuthority::new(test_process::TestProcess::authority(7));
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
    let authority = ServingAuthority::new(test_process::TestProcess::authority(7));
    authority.install(grant(4, 11), &policy(), 1_000, 8_000).unwrap();
    let partition = assignment(11).partition_id;
    assert!(authority.has_live_grant(3, 8_001));
    assert!(!authority.has_live_grant(2, 8_001));
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
    let authority = ServingAuthority::new(test_process::TestProcess::authority(7));
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

#[test]
fn clearing_authority_preserves_the_sequence_fence() {
    let authority = ServingAuthority::new(test_process::TestProcess::authority(7));
    authority.install(grant(4, 11), &policy(), 1_000, 8_000).unwrap();
    authority.clear();
    assert_eq!(
        authority.install(grant(4, 11), &policy(), 1_100, 8_100),
        Err(AuthorityError::StaleGrant)
    );
    assert!(!authority.has_live_grant(3, 8_101));
    authority.install(grant(5, 11), &policy(), 1_200, 8_200).unwrap();
    assert!(authority
        .authorize(3, assignment(11).partition_id, 11, 8_201)
        .is_ok());
}

#[test]
fn continuous_renewal_preserves_acknowledgment_but_not_the_original_io_deadline() {
    let authority = ServingAuthority::new(test_process::TestProcess::authority(7));
    authority.install(grant(4, 11), &policy(), 1_000, 8_000).unwrap();
    let context = authority
        .capture(3, assignment(11).partition_id, 11, 8_001)
        .unwrap();
    assert_eq!(context.deadline_ms(), 18_000);
    let mut renewed = grant(5, 11);
    renewed.issued_at_ms = 1_100;
    renewed.expires_at_ms = 13_100;
    renewed.seal();
    authority.install(renewed, &policy(), 1_100, 8_100).unwrap();
    assert!(authority.validate_context(&context, 18_001).is_ok());
    assert_eq!(context.deadline_ms(), 18_000);
}

#[test]
fn renewing_after_a_gap_does_not_resurrect_an_admitted_context() {
    let authority = ServingAuthority::new(test_process::TestProcess::authority(7));
    authority.install(grant(4, 11), &policy(), 1_000, 8_000).unwrap();
    let context = authority
        .capture(3, assignment(11).partition_id, 11, 8_001)
        .unwrap();
    let mut renewed = grant(5, 11);
    renewed.issued_at_ms = 12_000;
    renewed.expires_at_ms = 25_000;
    renewed.seal();
    authority.install(renewed, &policy(), 12_000, 19_000).unwrap();
    assert_eq!(
        authority.validate_context(&context, 19_001),
        Err(AuthorityError::LeaseExpired)
    );
    let fresh = authority
        .capture(3, assignment(11).partition_id, 11, 19_001)
        .unwrap();
    assert!(authority.validate_context(&fresh, 19_002).is_ok());
}

#[test]
fn restoring_an_assignment_cannot_restore_its_old_context() {
    let authority = ServingAuthority::new(test_process::TestProcess::authority(7));
    authority.install(grant(4, 11), &policy(), 1_000, 8_000).unwrap();
    let context = authority
        .capture(3, assignment(11).partition_id, 11, 8_001)
        .unwrap();
    authority.install(grant(5, 12), &policy(), 1_100, 8_100).unwrap();
    authority.install(grant(6, 11), &policy(), 1_200, 8_200).unwrap();
    assert_eq!(
        authority.validate_context(&context, 8_201),
        Err(AuthorityError::LeaseExpired)
    );
}
