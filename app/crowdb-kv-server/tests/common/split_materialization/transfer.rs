// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::*;

#[tokio::test]
async fn committed_transfer_without_overlay_releases_its_catalog_marker() {
    let fixture = TestMaterialization::transferred_parent().await;
    let parent = &fixture.page.entries[0];
    assert!(parent.artifact.tail_overlay.is_none());
    assert!(parent.transition_id.is_some());
    observe_partition(&fixture.control, parent, true).await;
    tick(&fixture.control).await.unwrap();

    let (head, page) = current_catalog(&fixture.control).await;
    let mut expected = parent.clone();
    expected.transition_id = None;
    assert_eq!(page.entries[0], expected);
    assert_eq!(page.entries[1], fixture.page.entries[1]);
    assert_eq!(head.generation, fixture.head.generation + 1);

    let transfer: TransferTransition = get_json(
        &fixture.control,
        &ChunkKvTransferKey {
            transition_id: parent.transition_id.unwrap(),
        }
        .to_path(),
    )
    .await;
    assert_eq!(transfer.phase, TransferPhase::CatalogCommitted);
    tick(&fixture.control).await.unwrap();
    assert_eq!(current_catalog(&fixture.control).await.0, head);
}

#[tokio::test]
async fn transfer_marker_waits_for_independent_recovery() {
    let fixture = TestMaterialization::transferred_parent().await;
    tick(&fixture.control).await.unwrap();
    let (head, page) = current_catalog(&fixture.control).await;
    assert_eq!(head, fixture.head);
    assert_eq!(page, fixture.page);
}

#[tokio::test]
async fn obsolete_owner_observation_cannot_release_a_transfer_marker() {
    let fixture = TestMaterialization::transferred_parent().await;
    let mut obsolete = fixture.page.entries[0].clone();
    obsolete.owner_epoch += 1;
    observe_partition(&fixture.control, &obsolete, true).await;
    tick(&fixture.control).await.unwrap();
    let (head, page) = current_catalog(&fixture.control).await;
    assert_eq!(head, fixture.head);
    assert_eq!(page, fixture.page);
}

#[tokio::test]
async fn transfer_marker_release_requires_the_exact_committed_assignment() {
    for mismatch in 0..4 {
        let mut fixture = TestMaterialization::transferred_parent().await;
        let parent = &mut fixture.page.entries[0];
        match mismatch {
            0 => parent.owner_epoch += 1,
            1 => parent.artifact.tree_id += 1,
            2 => parent.artifact.stream_name.low += 1,
            3 => parent.transition_id = Some(Id128 { high: 999, low: 1 }),
            _ => unreachable!(),
        }
        fixture.publish_page().await;
        observe_partition(&fixture.control, &fixture.page.entries[0], true).await;
        tick(&fixture.control).await.unwrap();
        let (head, page) = current_catalog(&fixture.control).await;
        assert_eq!(head, fixture.head, "mismatch {mismatch}");
        assert_eq!(page, fixture.page, "mismatch {mismatch}");
    }
}
