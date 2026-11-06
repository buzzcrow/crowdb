// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[path = "common/stream_lease_clock.rs"]
mod fixture;

use crowdb_paxos_kv_client::{StreamProcessAuthority, StreamProcessLeaseError, StreamProcessLeaseTiming};
use fixture::TestStreamLeaseRecord as Test;
use std::time::{Duration, Instant};

#[test]
fn continuous_renewal_preserves_ownership_without_extending_admitted_io() {
    let start = Instant::now();
    let owner = StreamProcessAuthority::new(
        Test::at(11_000),
        StreamProcessLeaseTiming::default(),
        1_000,
        start,
    )
    .unwrap();
    let admitted = owner.capture(start).unwrap();
    assert_eq!(admitted.deadline(), start + Duration::from_secs(8));
    owner
        .observe(Test::at(12_000), 2_000, start + Duration::from_secs(1))
        .unwrap();
    let later = start + Duration::from_millis(8_500);
    owner.validate_context(&admitted, later).unwrap();
    assert_eq!(admitted.deadline(), start + Duration::from_secs(8));
    assert_eq!(
        owner.capture(later).unwrap().deadline(),
        start + Duration::from_secs(9)
    );
    assert_eq!(owner.status(later).remaining_ms, 500);
}

#[test]
fn a_gap_cannot_be_cleared_by_a_later_successful_renewal() {
    let start = Instant::now();
    let owner = StreamProcessAuthority::new(
        Test::at(11_000),
        StreamProcessLeaseTiming::default(),
        1_000,
        start,
    )
    .unwrap();
    let admitted = owner.capture(start).unwrap();
    assert!(matches!(
        owner.observe(Test::at(21_000), 11_000, start + Duration::from_secs(10)),
        Err(StreamProcessLeaseError::Expired)
    ));
    assert!(matches!(
        owner.validate_context(&admitted, start + Duration::from_secs(11)),
        Err(StreamProcessLeaseError::Expired)
    ));
    assert!(matches!(
        owner.observe(Test::at(31_000), 21_000, start + Duration::from_secs(20)),
        Err(StreamProcessLeaseError::Expired)
    ));
    assert!(owner.status(start).fenced);
    assert_eq!(owner.status(start).remaining_ms, 0);
}

#[test]
fn an_out_of_order_expired_observation_does_not_revoke_a_continuously_renewed_owner() {
    let start = Instant::now();
    let first = Test::at(11_000);
    let owner = StreamProcessAuthority::new(first.clone(), StreamProcessLeaseTiming::default(), 1_000, start)
        .unwrap();
    let admitted = owner.capture(start).unwrap();
    owner
        .observe(Test::at(12_000), 2_000, start + Duration::from_secs(1))
        .unwrap();
    owner
        .observe(Test::at(18_000), 8_000, start + Duration::from_secs(7))
        .unwrap();
    owner
        .observe(first, 12_000, start + Duration::from_secs(11))
        .unwrap();
    owner
        .validate_context(&admitted, start + Duration::from_secs(11))
        .unwrap();
    assert_eq!(
        owner.capture(start + Duration::from_secs(11)).unwrap().deadline(),
        start + Duration::from_secs(15)
    );
}

#[test]
fn a_different_boot_fences_old_contexts_and_cannot_install_in_place() {
    let start = Instant::now();
    let first = Test::at(11_000);
    let owner = StreamProcessAuthority::new(first.clone(), StreamProcessLeaseTiming::default(), 1_000, start)
        .unwrap();
    let admitted = owner.capture(start).unwrap();
    let mut next = Test::at(22_000);
    next.identity.process_epoch = 2;
    next.identity.boot_nonce = [2; 16];
    next.prior_exclusion_deadline_ms = first.expires_at_ms;
    assert!(matches!(
        owner.observe(next.clone(), 2_000, start + Duration::from_secs(1)),
        Err(StreamProcessLeaseError::Replaced)
    ));
    assert!(owner.status(start).fenced);
    let replacement = StreamProcessAuthority::new(
        next,
        StreamProcessLeaseTiming::default(),
        2_000,
        start + Duration::from_secs(1),
    )
    .unwrap();
    assert!(matches!(
        replacement.validate_context(&admitted, start),
        Err(StreamProcessLeaseError::Replaced)
    ));
}

#[test]
fn unsafe_clock_bounds_and_margin_overflow_are_rejected() {
    let timing = StreamProcessLeaseTiming {
        max_clock_skew_ms: 1_001,
        self_fence_margin_ms: 1_000,
    };
    assert!(matches!(
        StreamProcessAuthority::new(Test::at(11_000), timing, 1_000, Instant::now()),
        Err(StreamProcessLeaseError::Invalid(_))
    ));
    let timing = StreamProcessLeaseTiming {
        max_clock_skew_ms: 1,
        self_fence_margin_ms: u64::MAX,
    };
    assert!(matches!(
        StreamProcessAuthority::new(Test::at(11_000), timing, 1_000, Instant::now()),
        Err(StreamProcessLeaseError::Invalid(_))
    ));
}

#[test]
fn observing_expiry_cannot_be_undone_by_a_backdated_renewal() {
    let start = Instant::now();
    let owner = StreamProcessAuthority::new(
        Test::at(11_000),
        StreamProcessLeaseTiming::default(),
        1_000,
        start,
    )
    .unwrap();
    let admitted = owner.capture(start).unwrap();
    assert!(matches!(
        owner.capture(start + Duration::from_secs(9)),
        Err(StreamProcessLeaseError::Expired)
    ));
    assert!(matches!(
        owner.observe(Test::at(12_000), 2_000, start + Duration::from_secs(1)),
        Err(StreamProcessLeaseError::Expired)
    ));
    assert!(matches!(
        owner.validate_context(&admitted, start),
        Err(StreamProcessLeaseError::Expired)
    ));
}

#[test]
fn malformed_observations_do_not_revoke_an_existing_live_lease() {
    let start = Instant::now();
    let owner = StreamProcessAuthority::new(
        Test::at(11_000),
        StreamProcessLeaseTiming::default(),
        1_000,
        start,
    )
    .unwrap();
    let admitted = owner.capture(start).unwrap();
    let mut malformed = Test::at(12_000);
    malformed.grant_sequence = 1;
    assert!(matches!(
        owner.observe(malformed, 2_000, start + Duration::from_secs(1)),
        Err(StreamProcessLeaseError::Invalid(_))
    ));
    owner
        .validate_context(&admitted, start + Duration::from_secs(1))
        .unwrap();
    assert!(!owner.status(start).fenced);
    assert_eq!(owner.capture(start).unwrap().deadline(), admitted.deadline());
}
