// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_access_dataset::{
    CommitAttempt, CommitGeneration, CommitTarget, OperationId, SnapshotId, SplitRevisionId, SuccessfulCommit,
};

fn attempt() -> CommitAttempt {
    CommitAttempt {
        version: 1,
        id: OperationId::from_bytes(&[1; 16]).unwrap(),
        target: CommitTarget::Snapshot {
            snapshot: SnapshotId::from_bytes(&[2; 16]).unwrap(),
        },
        expected_head: None,
    }
}

#[test]
fn required_absent_head_and_observed_head_are_distinct() {
    let empty = attempt();
    assert_eq!(CommitAttempt::decode(&empty.encode().unwrap()).unwrap(), empty);
    let mut missing = serde_json::to_value(&empty).unwrap();
    missing.as_object_mut().unwrap().remove("expected_head");
    assert!(CommitAttempt::decode(&serde_json::to_vec(&missing).unwrap()).is_err());
    let mut observed = empty.clone();
    observed.expected_head = Some(CommitGeneration::from_head(b"head-fence-1"));
    assert_ne!(observed.encode().unwrap(), empty.encode().unwrap());
    assert_ne!(
        CommitGeneration::from_head(b"head-fence-1"),
        CommitGeneration::from_head(b"head-fence-2")
    );
}

#[test]
fn receipt_preserves_exact_attempt_for_both_head_domains() {
    let mut attempt = attempt();
    for target in [
        attempt.target,
        CommitTarget::SplitRevision {
            snapshot: SnapshotId::from_bytes(&[2; 16]).unwrap(),
            revision: SplitRevisionId::from_bytes(&[3; 16]).unwrap(),
        },
    ] {
        attempt.target = target;
        let receipt = SuccessfulCommit {
            version: 1,
            attempt: attempt.clone(),
            committed_head: CommitGeneration::from_head(b"complete head with commit marker"),
        };
        assert_eq!(
            SuccessfulCommit::decode(&receipt.encode().unwrap()).unwrap(),
            receipt
        );
        let mut retry = attempt.clone();
        retry.id = OperationId::random();
        retry.expected_head = Some(receipt.committed_head);
        assert_eq!(retry.target, attempt.target);
        assert_ne!(retry.encode().unwrap(), receipt.attempt.encode().unwrap());
    }
}

#[test]
fn commit_records_reject_unknown_versions_fields_and_zero_identities() {
    let mut value = serde_json::to_value(attempt()).unwrap();
    value["version"] = 2.into();
    assert!(CommitAttempt::decode(&serde_json::to_vec(&value).unwrap()).is_err());
    value["version"] = 1.into();
    value["future_condition"] = true.into();
    assert!(CommitAttempt::decode(&serde_json::to_vec(&value).unwrap()).is_err());
    value.as_object_mut().unwrap().remove("future_condition");
    value["id"] = serde_json::to_value([0u8; 16]).unwrap();
    assert!(CommitAttempt::decode(&serde_json::to_vec(&value).unwrap()).is_err());
    assert!(CommitAttempt::decode(&vec![b' '; 4097]).is_err());
}
