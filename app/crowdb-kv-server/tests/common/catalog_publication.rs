// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::*;
use crowdb_kv_server::background::domain_monitor::ChunkKvRangeMonitorDriver;

struct TestPublication {
    control: Group0ControlPlane,
    prepared: SplitTransition,
    head: ChunkKvRangeCatalogHead,
}

#[tokio::test]
async fn expired_lease_transfer_publishes_directly_from_serving_source() {
    for (prior_marker, retained_overlay) in [
        (None, false),
        (Some(Id128 { high: 88, low: 99 }), false),
        (Some(Id128 { high: 88, low: 99 }), true),
    ] {
        let (control, mut ready) = assert_ready_transfer_publication().await;
        ready.release_proof = Some(AuthorityReleaseProof::LeaseExpired {
            activation_not_before_ms: 2,
        });
        if retained_overlay {
            ready.artifact.tail_overlay = ready.target_artifact.tail_overlay.clone();
        }
        ready.target_artifact = ready.artifact.clone();
        ready.readiness_proof.as_mut().unwrap().artifact = ready.artifact.clone();
        ready.catchup_proof.as_mut().unwrap().artifact = ready.artifact.clone();
        ready.validate().unwrap();
        let mut head: ChunkKvRangeCatalogHead =
            get_json(&control, &ChunkKvRangeCatalogHeadKey.to_path()).await;
        let mut page: ChunkKvRangeCatalogPage = get_json(
            &control,
            &ChunkKvRangeCatalogPageKey {
                generation: head.pages[0].page_generation,
                page_index: 0,
            }
            .to_path(),
        )
        .await;
        page.entries[0].owner = ready.source.clone();
        page.entries[0].owner_epoch = ready.source_epoch;
        page.entries[0].artifact = ready.artifact.clone();
        page.entries[0].transition_id = prior_marker;
        page.generation += 1;
        page.seal().unwrap();
        head.previous_generation = Some(head.generation);
        head.generation = page.generation;
        head.pages[0].page_generation = page.generation;
        head.pages[0].page_checksum = page.checksum;
        head.seal().unwrap();
        put_json(
            &control,
            ChunkKvRangeCatalogPageKey {
                generation: page.generation,
                page_index: 0,
            }
            .to_path(),
            &page,
        )
        .await;
        replace_json(&control, ChunkKvRangeCatalogHeadKey.to_path(), &head).await;
        replace_json(
            &control,
            ChunkKvTransferKey {
                transition_id: ready.transition_id,
            }
            .to_path(),
            &ready,
        )
        .await;
        tick(&control).await.unwrap();
        let committed: TransferTransition = get_json(
            &control,
            &ChunkKvTransferKey {
                transition_id: ready.transition_id,
            }
            .to_path(),
        )
        .await;
        assert_eq!(committed.phase, TransferPhase::CatalogCommitted);
        let published: ChunkKvRangeCatalogHead =
            get_json(&control, &ChunkKvRangeCatalogHeadKey.to_path()).await;
        assert_eq!(published.generation, head.generation + 1);
        let grant: ServingGrant = get_json(
            &control,
            &ServingGrantKey {
                instance_id: ready.target.instance_id,
            }
            .to_path(),
        )
        .await;
        assert_eq!(grant.catalog_generation, published.generation);
        assert_eq!(grant.assignments[0].owner_epoch, ready.target_epoch);
        let page: ChunkKvRangeCatalogPage = get_json(
            &control,
            &ChunkKvRangeCatalogPageKey {
                generation: published.pages[0].page_generation,
                page_index: 0,
            }
            .to_path(),
        )
        .await;
        assert_eq!(page.entries[0].artifact, ready.artifact);
        assert_eq!(page.entries[0].owner, ready.target);
    }
}

#[tokio::test]
async fn published_split_resumes_after_materialization_before_transition_commit() {
    for retain_markers in [true, false] {
        let fixture = prepare_materialized_split(retain_markers, false).await;
        fixture.tick().await.unwrap();
        let committed: SplitTransition = get_json(
            &fixture.control,
            &ChunkKvSplitKey {
                transition_id: fixture.prepared.transition_id,
            }
            .to_path(),
        )
        .await;
        assert_eq!(committed.phase, SplitPhase::CatalogCommitted);
        let current: ChunkKvRangeCatalogHead =
            get_json(&fixture.control, &ChunkKvRangeCatalogHeadKey.to_path()).await;
        assert_eq!(current, fixture.head);
    }
}

#[tokio::test]
async fn materialized_split_with_changed_epoch_does_not_prove_old_publication() {
    let fixture = prepare_materialized_split(false, true).await;
    assert!(fixture.tick().await.is_err());
    let current: SplitTransition = get_json(
        &fixture.control,
        &ChunkKvSplitKey {
            transition_id: fixture.prepared.transition_id,
        }
        .to_path(),
    )
    .await;
    assert_eq!(current.phase, SplitPhase::ChildPrepared);
}

async fn prepare_materialized_split(retain_markers: bool, advance_epoch: bool) -> TestPublication {
    let (control, prepared) = assert_chunk_kv_split_plan(2, u64::MAX).await;
    // Publication succeeded, but its separate transition status write was lost.
    // Both writers then materialized their inherited overlays before restart.
    replace_json(
        &control,
        ChunkKvSplitKey {
            transition_id: prepared.transition_id,
        }
        .to_path(),
        &prepared,
    )
    .await;
    let head = publish_materialized_catalog(&control, retain_markers, advance_epoch).await;
    TestPublication {
        control,
        prepared,
        head,
    }
}

impl TestPublication {
    async fn tick(&self) -> Result<(), String> {
        tick(&self.control).await
    }
}

async fn replace_json<T: serde::Serialize>(control: &Group0ControlPlane, key: String, value: &T) {
    let current = control.get(key.as_bytes()).await.unwrap();
    control
        .compare_and_put(
            Bytes::from(key),
            Bytes::from(serde_json::to_vec(value).unwrap()),
            current.revision,
        )
        .await
        .unwrap();
}

async fn publish_materialized_catalog(
    control: &Group0ControlPlane,
    retain_markers: bool,
    advance_epoch: bool,
) -> ChunkKvRangeCatalogHead {
    let mut head: ChunkKvRangeCatalogHead = get_json(control, &ChunkKvRangeCatalogHeadKey.to_path()).await;
    let mut page: ChunkKvRangeCatalogPage = get_json(
        control,
        &ChunkKvRangeCatalogPageKey {
            generation: head.generation,
            page_index: 0,
        }
        .to_path(),
    )
    .await;
    for entry in &mut page.entries {
        entry.artifact.tail_overlay = None;
        if !retain_markers {
            entry.transition_id = None;
        }
    }
    if advance_epoch {
        page.entries[0].owner_epoch += 1;
    }
    page.generation += 1;
    page.seal().unwrap();
    head.previous_generation = Some(head.generation);
    head.generation = page.generation;
    head.pages[0].page_generation = page.generation;
    head.pages[0].page_checksum = page.checksum;
    head.seal().unwrap();
    head.validate_pages(&[page.clone()]).unwrap();
    put_json(
        control,
        ChunkKvRangeCatalogPageKey {
            generation: page.generation,
            page_index: 0,
        }
        .to_path(),
        &page,
    )
    .await;
    replace_json(control, ChunkKvRangeCatalogHeadKey.to_path(), &head).await;
    head
}

async fn tick(control: &Group0ControlPlane) -> Result<(), String> {
    let mut policy = descriptor();
    policy.domain = "chunk-kv".into();
    policy.service_registry_name = "chunk-kv".into();
    ChunkKvRangeMonitorDriver::new().tick(control, &policy).await
}

#[tokio::test]
async fn published_transfer_resumes_after_materialization_before_transition_commit() {
    let (control, ready) = assert_ready_transfer_publication().await;
    replace_json(
        &control,
        ChunkKvTransferKey {
            transition_id: ready.transition_id,
        }
        .to_path(),
        &ready,
    )
    .await;
    let head = publish_materialized_catalog(&control, false, false).await;
    tick(&control).await.unwrap();
    let committed: TransferTransition = get_json(
        &control,
        &ChunkKvTransferKey {
            transition_id: ready.transition_id,
        }
        .to_path(),
    )
    .await;
    assert_eq!(committed.phase, TransferPhase::CatalogCommitted);
    let current: ChunkKvRangeCatalogHead = get_json(&control, &ChunkKvRangeCatalogHeadKey.to_path()).await;
    assert_eq!(current, head);
}
