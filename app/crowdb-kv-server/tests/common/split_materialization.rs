// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::catalog_publication::{replace_json, tick};
use super::*;

#[path = "split_materialization/transfer.rs"]
mod transfer;

struct TestMaterialization {
    control: Group0ControlPlane,
    split: SplitTransition,
    head: ChunkKvRangeCatalogHead,
    page: ChunkKvRangeCatalogPage,
}

#[tokio::test]
async fn child_materializes_before_the_split_commit_status_is_persisted() {
    let (control, prepared) = assert_chunk_kv_split_plan(2, u64::MAX).await;
    replace_json(
        &control,
        ChunkKvSplitKey {
            transition_id: prepared.transition_id,
        }
        .to_path(),
        &prepared,
    )
    .await;
    test_process::TestProcessLease::seed(&control, 1).await;
    let (before, original) = current_catalog(&control).await;
    observe_partition(&control, &original.entries[1], true).await;
    tick(&control).await.unwrap();
    let (head, page) = current_catalog(&control).await;
    assert_eq!(head.generation, before.generation + 1);
    for (mut expected, actual) in original.entries.into_iter().zip(page.entries) {
        expected.artifact.tail_overlay = None;
        expected.transition_id = None;
        assert_eq!(actual, expected);
    }
    let committed: SplitTransition = get_json(
        &control,
        &ChunkKvSplitKey {
            transition_id: prepared.transition_id,
        }
        .to_path(),
    )
    .await;
    assert_eq!(committed.phase, SplitPhase::CatalogCommitted);
}

#[tokio::test]
async fn transferred_split_parent_does_not_block_child_materialization_or_grants() {
    let fixture = TestMaterialization::transferred_parent().await;
    let parent = fixture.page.entries[0].clone();
    fixture.observe_child().await;
    tick(&fixture.control).await.unwrap();
    let (head, page) = current_catalog(&fixture.control).await;
    assert_eq!(head.generation, fixture.head.generation + 1);
    assert_eq!(page.entries[0], parent);
    let mut child = fixture.page.entries[1].clone();
    child.artifact.tail_overlay = None;
    child.transition_id = None;
    assert_eq!(page.entries[1], child);
    for entry in &page.entries {
        let grant: ServingGrant = get_json(
            &fixture.control,
            &ServingGrantKey {
                instance_id: entry.owner.instance_id,
            }
            .to_path(),
        )
        .await;
        assert_eq!(grant.catalog_generation, head.generation);
        assert_eq!(grant.assignments.len(), 1);
        assert_eq!(grant.assignments[0].partition_id, entry.partition_id);
        assert_eq!(grant.assignments[0].owner_epoch, entry.owner_epoch);
    }
    tick(&fixture.control).await.unwrap();
    assert_eq!(current_catalog(&fixture.control).await.0, head);
}

#[tokio::test]
async fn split_materialization_rejects_changed_assignment_or_reused_marker() {
    for mismatch in 0..3 {
        let mut fixture = TestMaterialization::transferred_parent().await;
        match mismatch {
            0 => fixture.page.entries[1].owner_epoch += 1,
            1 => fixture.page.entries[1].artifact.tree_id += 1,
            2 => fixture.page.entries[0].transition_id = Some(fixture.split.transition_id),
            _ => unreachable!(),
        }
        fixture.publish_page().await;
        fixture.observe_child().await;
        let error = tick(&fixture.control)
            .await
            .expect_err("changed split identity must be rejected");
        assert!(error.contains("materialized split"), "{error}");
        assert_eq!(current_catalog(&fixture.control).await.0, fixture.head);
    }
}

#[tokio::test]
async fn uncommitted_split_still_requires_both_published_assignments() {
    let fixture = TestMaterialization::transferred_parent().await;
    replace_json(
        &fixture.control,
        ChunkKvSplitKey {
            transition_id: fixture.split.transition_id,
        }
        .to_path(),
        &fixture.split,
    )
    .await;
    assert_eq!(fixture.split.phase, SplitPhase::ChildPrepared);
    fixture.observe_child().await;
    assert!(tick(&fixture.control).await.is_err());
    assert_eq!(current_catalog(&fixture.control).await.0, fixture.head);
}

impl TestMaterialization {
    async fn transferred_parent() -> Self {
        let (control, split) = assert_chunk_kv_split_plan(2, u64::MAX).await;
        let (_, page) = current_catalog(&control).await;
        let parent = &page.entries[0];
        let target = OwnerDescriptor {
            instance_id: 2,
            rpc_endpoint: "127.0.0.1:17002".into(),
        };
        let proof = TargetReadinessProof {
            target_instance_id: target.instance_id,
            target_epoch: parent.owner_epoch + 1,
            artifact: parent.artifact.clone(),
            durable_tail: 11,
        };
        let transfer = TransferTransition {
            transition_id: Id128 { high: 97, low: 98 },
            partition_id: parent.partition_id,
            range: parent.range.clone(),
            source: parent.owner.clone(),
            source_epoch: parent.owner_epoch,
            target: target.clone(),
            target_epoch: proof.target_epoch,
            artifact: parent.artifact.clone(),
            target_artifact: parent.artifact.clone(),
            readiness_limits: TransferReadinessLimits {
                max_tail_records: 100,
                max_tail_bytes: 1_000_000,
                max_estimated_catchup_ms: 1_000,
                prepare_deadline_ms: 1_000,
                forwarding_grace_ms: 1_000,
            },
            planned_at_ms: 0,
            old_grant_expires_at_ms: 1,
            phase: TransferPhase::TargetReady,
            release_proof: Some(AuthorityReleaseProof::LeaseExpired {
                activation_not_before_ms: 2,
            }),
            readiness_proof: Some(proof.clone()),
            catchup_proof: Some(proof),
            failure: None,
        };
        transfer.validate().unwrap();
        put_json(
            &control,
            ChunkKvTransferKey {
                transition_id: transfer.transition_id,
            }
            .to_path(),
            &transfer,
        )
        .await;
        for instance_id in [1, 2] {
            test_process::TestProcessLease::seed(&control, instance_id).await;
        }
        let mut moved = parent.clone();
        moved.owner = target;
        moved.owner_epoch = transfer.target_epoch;
        observe_partition(&control, &moved, false).await;
        observe_partition(&control, &page.entries[1], false).await;
        tick(&control).await.unwrap();
        let (head, page) = current_catalog(&control).await;
        assert_eq!(page.entries[0].transition_id, Some(transfer.transition_id));
        assert_eq!(page.entries[1].transition_id, Some(split.transition_id));
        assert_eq!(page.entries[0].owner_epoch, transfer.target_epoch);
        Self {
            control,
            split,
            head,
            page,
        }
    }

    async fn observe_child(&self) {
        observe_partition(&self.control, &self.page.entries[1], true).await;
    }

    async fn publish_page(&mut self) {
        self.page.generation += 1;
        self.page.seal().unwrap();
        self.head.previous_generation = Some(self.head.generation);
        self.head.generation = self.page.generation;
        self.head.pages[0].page_generation = self.page.generation;
        self.head.pages[0].page_checksum = self.page.checksum;
        self.head.seal().unwrap();
        self.head
            .validate_pages(std::slice::from_ref(&self.page))
            .unwrap();
        put_json(
            &self.control,
            ChunkKvRangeCatalogPageKey {
                generation: self.page.generation,
                page_index: 0,
            }
            .to_path(),
            &self.page,
        )
        .await;
        replace_json(&self.control, ChunkKvRangeCatalogHeadKey.to_path(), &self.head).await;
    }
}

async fn observe_partition(
    control: &Group0ControlPlane,
    entry: &ChunkKvRangeCatalogEntry,
    independent: bool,
) {
    let instance = InstanceValue {
        instance_id: entry.owner.instance_id,
        rpc_endpoint: entry.owner.rpc_endpoint.clone(),
        last_heartbeat_ms: u64::MAX,
        extra: Some(ServiceExtra {
            chunk_kv: Some(ChunkKvExtra {
                process_epoch: 1,
                hosted: vec![HostedPartition {
                    partition_id: entry.partition_id,
                    owner_epoch: entry.owner_epoch,
                    recovering: false,
                }],
                partition_loads: vec![ChunkKvPartitionLoad {
                    partition_id: entry.partition_id,
                    independently_recoverable: independent,
                    ..ChunkKvPartitionLoad::default()
                }],
                ..ChunkKvExtra::default()
            }),
            ..ServiceExtra::default()
        }),
    };
    replace_json(
        control,
        InstanceKey {
            service: "chunk-kv".into(),
            instance_id: instance.instance_id,
        }
        .to_path(),
        &instance,
    )
    .await;
}

async fn current_catalog(control: &Group0ControlPlane) -> (ChunkKvRangeCatalogHead, ChunkKvRangeCatalogPage) {
    let head: ChunkKvRangeCatalogHead = get_json(control, &ChunkKvRangeCatalogHeadKey.to_path()).await;
    let page = get_json(
        control,
        &ChunkKvRangeCatalogPageKey {
            generation: head.pages[0].page_generation,
            page_index: 0,
        }
        .to_path(),
    )
    .await;
    (head, page)
}
