// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_chunk_kv_server::{TransferAction, TransferStateMachine};
use crowdb_protocol::chunk_kv::{
    AuthorityReleaseProof, Id128, KeyRange, OwnerDescriptor, PartitionArtifact, TargetReadinessProof,
    TransferPhase, TransferTransition,
};
use crowdb_protocol::chunk_stream::StreamName;

fn transition() -> TransferTransition {
    TransferTransition {
        transition_id: Id128 { high: 1, low: 2 },
        partition_id: Id128 { high: 3, low: 4 },
        range: KeyRange {
            start: Vec::new(),
            end: None,
        },
        source: OwnerDescriptor {
            instance_id: 5,
            rpc_endpoint: "old:9000".into(),
        },
        source_epoch: 6,
        target: OwnerDescriptor {
            instance_id: 7,
            rpc_endpoint: "new:9000".into(),
        },
        target_epoch: 8,
        artifact: PartitionArtifact {
            tree_manifest: 9,
            stream_name: StreamName { high: 10, low: 11 },
            applied_seq: 12,
        },
        old_grant_expires_at_ms: 20_000,
        phase: TransferPhase::Planned,
        release_proof: None,
        readiness_proof: None,
        failure: None,
    }
}

#[test]
fn dead_owner_waits_through_grant_and_skew_before_prepare() {
    let mut machine = TransferStateMachine::restore(transition()).unwrap();
    assert_eq!(
        machine.next_action(false, 1_000),
        TransferAction::WaitForFence {
            activation_not_before_ms: 21_000
        }
    );
    assert!(machine.record_lease_expiry(20_999, 1_000).is_err());
    machine.record_lease_expiry(21_000, 1_000).unwrap();
    machine.begin_target_prepare().unwrap();
    assert_eq!(
        machine.next_action(false, 1_000),
        TransferAction::PrepareTarget {
            instance_id: 7,
            owner_epoch: 8
        }
    );
}

#[test]
fn graceful_transfer_requires_exact_fence_and_readiness_proofs() {
    let mut machine = TransferStateMachine::restore(transition()).unwrap();
    let fence = AuthorityReleaseProof::ExplicitFence {
        source_instance_id: 5,
        source_epoch: 6,
        durable_tail: 12,
    };
    machine.record_source_fence(fence.clone()).unwrap();
    machine.record_source_fence(fence).unwrap();
    machine.begin_target_prepare().unwrap();
    let proof = TargetReadinessProof {
        target_instance_id: 7,
        target_epoch: 8,
        artifact: machine.transition().artifact.clone(),
        durable_tail: 12,
    };
    machine.record_target_ready(proof.clone()).unwrap();
    machine.record_target_ready(proof).unwrap();
    assert_eq!(machine.next_action(true, 1_000), TransferAction::PublishCatalog);
    machine.commit_catalog().unwrap();
    machine.commit_catalog().unwrap();
    assert_eq!(machine.next_action(true, 1_000), TransferAction::Complete);
    assert!(machine.abort("too late").is_err());
}

#[test]
fn failed_target_can_abort_without_losing_artifact_identity() {
    let original = transition();
    let artifact = original.artifact.clone();
    let mut machine = TransferStateMachine::restore(original).unwrap();
    machine.abort("manifest unavailable").unwrap();
    machine.abort("manifest unavailable").unwrap();
    assert_eq!(machine.next_action(false, 1_000), TransferAction::Aborted);
    assert_eq!(machine.transition().artifact, artifact);
}
