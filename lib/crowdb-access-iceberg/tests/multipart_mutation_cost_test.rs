// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[path = "common/store.rs"]
mod common;
#[path = "common/file.rs"]
#[allow(dead_code)]
mod file;
#[path = "common/multipart.rs"]
#[allow(dead_code)]
mod fixtures;
#[path = "common/multipart_mutation.rs"]
mod mutation;

use crowdb_access_iceberg::file::{
    MultipartMutation, MultipartPhase, MultipartRepository, MultipartSelection,
};
use mutation::{applied, TestUpload};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

#[tokio::test]
async fn complete_passes_known_states_and_selection_without_confirmation_reads() {
    let fixture = TestUpload::new().await;
    let selection = fixture.selection().await;
    let before = fixture.counts();
    let (frozen, ()) = applied(
        fixture
            .repository
            .freeze_completion(&fixture.session, &selection, 102)
            .await
            .unwrap(),
    );
    assert_eq!(fixture.delta(before), (5, 2));
    assert_eq!(frozen.phase, MultipartPhase::Completing);
    let before = fixture.counts();
    let (publishing, ()) = applied(
        fixture
            .repository
            .prepare_stream_publication(&frozen, &selection, 103)
            .await
            .unwrap()
            .unwrap(),
    );
    assert_eq!(fixture.delta(before), (5, 2));
    let before = fixture.counts();
    let (published, record) = applied(fixture.repository.publish(&publishing).await.unwrap());
    assert_eq!(fixture.delta(before), (6, 2));
    assert_eq!(published.phase, MultipartPhase::Published);
    assert_eq!(published.published, Some(record.file));
    assert_eq!(record.length, 14);
    let locations = record.content.locations(record.length).unwrap().unwrap();
    assert_eq!(
        locations
            .iter()
            .map(|location| location.chunk_id.unwrap().low)
            .collect::<Vec<_>>(),
        vec![1, 2]
    );
    assert_eq!(
        locations
            .iter()
            .map(|location| location.logical_offset)
            .collect::<Vec<_>>(),
        vec![0, 7]
    );
    let before = fixture.counts();
    assert_eq!(
        applied(fixture.repository.publish(&published).await.unwrap()).1,
        record
    );
    assert_eq!(fixture.delta(before), (5, 0));
}

#[tokio::test]
async fn unknown_part_reply_has_no_confirmation_get_and_full_retry_preserves_other_parts() {
    let fixture = TestUpload::new().await;
    fixture.file.store.fail_after.store(
        fixture.file.store.writes.load(Ordering::SeqCst) + 1,
        Ordering::SeqCst,
    );
    let first = fixture.part(1, 1, b"original");
    let before = fixture.counts();
    assert!(fixture
        .repository
        .put_stream_part(&fixture.session, &first, 101)
        .await
        .is_err());
    assert_eq!(fixture.delta(before), (1, 1));
    let other = fixture.part(2, 2, b"other");
    let before = fixture.counts();
    let other = fixture
        .repository
        .put_stream_part(&fixture.session, &other, 102)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(fixture.delta(before), (1, 1));
    let replacement = fixture.part(1, 3, b"original");
    let before = fixture.counts();
    let replacement = fixture
        .repository
        .put_stream_part(&fixture.session, &replacement, 103)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(fixture.delta(before), (1, 1));
    assert_eq!(replacement.revision, 2);
    assert_eq!(
        fixture
            .repository
            .part_for_upload(&fixture.session, 2)
            .await
            .unwrap(),
        Some(other.clone())
    );
    let selection = MultipartSelection::with_stream_parts(&[replacement, other]).unwrap();
    let (frozen, ()) = applied(
        fixture
            .repository
            .freeze_completion(&fixture.session, &selection, 104)
            .await
            .unwrap(),
    );
    let (publishing, ()) = applied(
        fixture
            .repository
            .prepare_stream_publication(&frozen, &selection, 105)
            .await
            .unwrap()
            .unwrap(),
    );
    let (_, record) = applied(fixture.repository.publish(&publishing).await.unwrap());
    assert_eq!(
        record
            .content
            .locations(record.length)
            .unwrap()
            .unwrap()
            .iter()
            .map(|location| location.chunk_id.unwrap().low)
            .collect::<Vec<_>>(),
        vec![3, 2]
    );
}

#[tokio::test]
async fn definite_freeze_cas_conflict_returns_its_observation_without_a_get() {
    let fixture = TestUpload::new().await;
    let selection = fixture.selection().await;
    let store = Arc::new(mutation::TestSessionConflictStore {
        inner: fixture.file.store.clone(),
        armed: AtomicBool::new(true),
    });
    let repository = MultipartRepository::new(store);
    let before = fixture.counts();
    let MultipartMutation::Conflict(Some(observed)) = repository
        .freeze_completion(&fixture.session, &selection, 102)
        .await
        .unwrap()
    else {
        panic!("expected observed abort")
    };
    assert_eq!(fixture.delta(before), (5, 2));
    assert_eq!(observed.phase, MultipartPhase::Aborted);
    assert_eq!(observed.revision, fixture.session.revision + 1);
    let before = fixture.counts();
    assert!(repository
        .prepare_stream_publication(&observed, &selection, 103)
        .await
        .is_err());
    assert_eq!(fixture.delta(before), (0, 0));
}

#[tokio::test]
async fn a_replacement_after_freeze_cannot_change_the_selected_file_and_changed_input_is_rejected() {
    let fixture = TestUpload::new().await;
    let selection = fixture.selection().await;
    let (frozen, ()) = applied(
        fixture
            .repository
            .freeze_completion(&fixture.session, &selection, 102)
            .await
            .unwrap(),
    );
    let late = fixture
        .repository
        .put_stream_part(&fixture.session, &fixture.part(1, 99, b"changed"), 103)
        .await
        .unwrap()
        .unwrap();
    let other = fixture
        .repository
        .part_for_upload(&fixture.session, 2)
        .await
        .unwrap()
        .unwrap();
    let changed = MultipartSelection::with_stream_parts(&[late, other]).unwrap();
    let before = fixture.counts();
    assert!(fixture
        .repository
        .prepare_stream_publication(&frozen, &changed, 104)
        .await
        .is_err());
    assert_eq!(fixture.delta(before), (0, 0));
    let (publishing, ()) = applied(
        fixture
            .repository
            .prepare_stream_publication(&frozen, &selection, 104)
            .await
            .unwrap()
            .unwrap(),
    );
    let (_, record) = applied(fixture.repository.publish(&publishing).await.unwrap());
    assert_eq!(
        record.content.locations(record.length).unwrap().unwrap()[0]
            .chunk_id
            .unwrap()
            .low,
        1
    );
    assert_eq!(
        fixture.repository.load_selection(&frozen).await.unwrap(),
        selection
    );
}

#[tokio::test]
async fn unknown_completion_mutations_do_not_read_to_confirm_and_retry_from_durable_resources() {
    for phase in [0, 1, 2] {
        let fixture = TestUpload::new().await;
        let selection = fixture.selection().await;
        let (frozen, ()) = applied(
            fixture
                .repository
                .freeze_completion(&fixture.session, &selection, 102)
                .await
                .unwrap(),
        );
        if phase == 0 {
            fixture.file.store.fail_after.store(
                fixture.file.store.writes.load(Ordering::SeqCst) + 2,
                Ordering::SeqCst,
            );
            let before = fixture.counts();
            assert!(fixture
                .repository
                .prepare_stream_publication(&frozen, &selection, 103)
                .await
                .is_err());
            assert_eq!(fixture.delta(before), (4, 2));
        } else {
            let (publishing, ()) = applied(
                fixture
                    .repository
                    .prepare_stream_publication(&frozen, &selection, 103)
                    .await
                    .unwrap()
                    .unwrap(),
            );
            fixture.file.store.fail_after.store(
                fixture.file.store.writes.load(Ordering::SeqCst) + usize::from(phase != 1) + 1,
                Ordering::SeqCst,
            );
            let before = fixture.counts();
            assert!(fixture.repository.publish(&publishing).await.is_err());
            assert_eq!(fixture.delta(before), if phase == 1 { (4, 1) } else { (5, 2) });
        }
        let current = fixture
            .repository
            .load(fixture.session.context, fixture.session.upload)
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(
            current.phase,
            MultipartPhase::Publishing | MultipartPhase::Published
        ));
        let (_, record) = applied(fixture.repository.publish(&current).await.unwrap());
        assert_eq!(record.length, 14);
        assert_eq!(
            fixture.repository.load_selection(&current).await.unwrap(),
            selection
        );
        assert!(fixture.repository.abort(&current).await.is_err());
    }
}
