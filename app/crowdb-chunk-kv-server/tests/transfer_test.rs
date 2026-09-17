// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_chunk_kv_server::{TransferAction, TransferStateMachine};
use crowdb_protocol::chunk_kv::{
    AuthorityReleaseProof, Id128, KeyRange, OwnerDescriptor, PartitionArtifact, TailOverlayArtifact,
    TargetReadinessProof, TransferPhase, TransferReadinessLimits, TransferTransition,
};
use crowdb_protocol::chunk_stream::StreamName;

fn transition() -> TransferTransition {
    let source_artifact = PartitionArtifact {
        tree_id: 1,
        stream_name: StreamName { high: 10, low: 11 },
        tail_overlay: None,
    };
    let target_artifact = PartitionArtifact {
        tree_id: 1,
        stream_name: StreamName { high: 12, low: 13 },
        tail_overlay: Some(TailOverlayArtifact {
            source_partition_id: Id128 { high: 3, low: 4 },
            source_epoch: 6,
            source_stream_name: source_artifact.stream_name,
            source_stream_manifest_generation: 1,
            replay_offset: 0,
            cutover_offset: 10,
            base_tree_manifest: 1,
            base_applied_seq: 10,
            cutover_seq: 10,
            target_stream_start_seq: 11,
        }),
    };
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
        artifact: source_artifact,
        target_artifact,
        readiness_limits: TransferReadinessLimits {
            max_tail_records: 100,
            max_tail_bytes: 1_000_000,
            max_estimated_catchup_ms: 1_000,
            prepare_deadline_ms: 10_000,
            forwarding_grace_ms: 1_000,
        },
        planned_at_ms: 0,
        old_grant_expires_at_ms: 20_000,
        phase: TransferPhase::Planned,
        release_proof: None,
        readiness_proof: None,
        catchup_proof: None,
        failure: None,
    }
}

fn prepare_source(machine: &mut TransferStateMachine) {
    machine.begin_source_prepare().unwrap();
    let artifact = machine.transition().target_artifact.clone();
    machine.record_source_base(artifact).unwrap();
}

#[test]
fn dead_owner_waits_through_grant_and_skew_after_target_prepare() {
    let mut machine = TransferStateMachine::restore(transition()).unwrap();
    prepare_source(&mut machine);
    machine.begin_target_prepare().unwrap();
    let artifact = machine.transition().target_artifact.clone();
    machine
        .record_target_ready(TargetReadinessProof {
            target_instance_id: 7,
            target_epoch: 8,
            artifact,
            durable_tail: 10,
        })
        .unwrap();
    assert_eq!(
        machine.next_action(false, 1_000),
        TransferAction::WaitForFence {
            activation_not_before_ms: 21_000
        }
    );
    assert!(machine.record_lease_expiry(20_999, 1_000).is_err());
    machine.record_lease_expiry(21_000, 1_000).unwrap();
    assert_eq!(
        machine.next_action(false, 1_000),
        TransferAction::PublishCatchingUp
    );
}

#[test]
fn graceful_transfer_requires_exact_fence_and_readiness_proofs() {
    let mut lagging = TransferStateMachine::restore(transition()).unwrap();
    prepare_source(&mut lagging);
    lagging.begin_target_prepare().unwrap();
    let lagging_artifact = lagging.transition().target_artifact.clone();
    lagging
        .record_target_ready(TargetReadinessProof {
            target_instance_id: 7,
            target_epoch: 8,
            artifact: lagging_artifact,
            durable_tail: 11,
        })
        .unwrap();
    assert!(lagging
        .record_source_fence(AuthorityReleaseProof::ExplicitFence {
            source_instance_id: 5,
            source_epoch: 6,
            durable_tail: 12,
            durable_tail_offset: 12,
        })
        .is_ok());

    let mut machine = TransferStateMachine::restore(transition()).unwrap();
    let fence = AuthorityReleaseProof::ExplicitFence {
        source_instance_id: 5,
        source_epoch: 6,
        durable_tail: 12,
        durable_tail_offset: 12,
    };
    prepare_source(&mut machine);
    machine.begin_target_prepare().unwrap();
    let proof = TargetReadinessProof {
        target_instance_id: 7,
        target_epoch: 8,
        artifact: machine.transition().target_artifact.clone(),
        durable_tail: 10,
    };
    machine.record_target_ready(proof.clone()).unwrap();
    machine.record_target_ready(proof).unwrap();
    machine.record_source_fence(fence.clone()).unwrap();
    machine.record_source_fence(fence).unwrap();
    assert_eq!(
        machine.next_action(true, 1_000),
        TransferAction::PublishCatchingUp
    );
    machine.mark_catchup_published().unwrap();
    let caught_up = TargetReadinessProof {
        target_instance_id: 7,
        target_epoch: 8,
        artifact: machine.transition().target_artifact.clone(),
        durable_tail: 12,
    };
    machine.record_target_caught_up(caught_up).unwrap();
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
