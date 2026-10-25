// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::time::Duration;

use crowdb_access_s3::metadata::MultipartRepositoryError;

use super::{part, selection, MetadataKey, MultipartPartRecord, MultipartPhase, TestIndependent};

fn replacement() -> MultipartPartRecord {
    let mut value = part();
    value.raw_md5 = [8; 16];
    value.locations[0].offset += 39;
    value
}

fn spawn_write(
    test: &TestIndependent,
    value: MultipartPartRecord,
) -> tokio::task::JoinHandle<Result<Option<MultipartPartRecord>, MultipartRepositoryError>> {
    let repository = test.repository.clone();
    let session = test.session.clone();
    tokio::spawn(async move { repository.put_stream_part(&session, &value, 111).await })
}

#[tokio::test]
async fn complete_freezes_a_while_real_writer_b_is_between_generation_and_pointer() {
    let test = TestIndependent::new().await;
    let first = test
        .repository
        .put_stream_part(&test.session, &part(), 110)
        .await
        .unwrap()
        .unwrap();
    let barrier = test.probe.pause_mutation(test.pointer_key(1));
    let writer = spawn_write(&test, replacement());
    tokio::time::timeout(Duration::from_secs(2), barrier.started)
        .await
        .unwrap()
        .unwrap();
    let candidate = test
        .repository
        .part_generation(&test.session, 1, 2)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(candidate.raw_md5, replacement().raw_md5);
    assert_eq!(candidate.locations, replacement().locations);
    let frozen = test
        .repository
        .freeze_completion(&test.session, &selection(&first), 120)
        .await;
    barrier.resume.send(()).unwrap();
    let writer = writer.await.unwrap();
    let frozen = frozen
        .unwrap()
        .expect("Complete must freeze A without helping the paused writer B");
    let second = writer
        .unwrap()
        .expect("an admitted writer may acknowledge B after A froze");
    assert_eq!(second, candidate);
    assert_eq!(
        test.repository.part(&test.session, 1).await.unwrap(),
        Some(second.clone())
    );
    assert_eq!(
        test.repository.load(&test.session).await.unwrap(),
        Some(frozen.clone())
    );
    let selected = &frozen.selection.as_ref().unwrap()[0];
    assert_eq!(
        (selected.number, selected.revision),
        (first.number, first.revision)
    );
    test.repository.publish_completion(&frozen).await.unwrap();
    test.assert_object(&first).await;

    let restarted = test.restart();
    let replay = restarted
        .freeze_completion(&test.session, &selection(&first), 121)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(replay, frozen);
    assert_eq!(
        restarted
            .expire_page(test.session.bucket_id, None, 500, 10)
            .await
            .unwrap()
            .expired,
        0
    );
    let object_key =
        MetadataKey::object(&super::tenant(), test.session.bucket_id, &test.session.object_key).unwrap();
    test.store
        .put(object_key, b"later ordinary overwrite".to_vec())
        .await
        .unwrap();
    restarted.publish_completion(&replay).await.unwrap();
    test.assert_object(&first).await;
    assert!(matches!(
        restarted
            .freeze_completion(&test.session, &selection(&second), 501)
            .await,
        Err(MultipartRepositoryError::Conflict)
    ));
    assert!(matches!(
        restarted.abort(&test.session).await,
        Err(MultipartRepositoryError::Conflict)
    ));
    let retained = test.raw_upload().await;
    assert_eq!(retained.len(), 4);
    for key in [
        test.upload_key(),
        test.pointer_key(1),
        test.generation_key(1, 1),
        test.generation_key(1, 2),
    ] {
        assert!(retained.iter().any(|row| row.key == key));
    }
    test.assert_object(&first).await;
}

#[tokio::test]
async fn missing_current_pointer_does_not_remove_the_frozen_generation_reference() {
    let test = TestIndependent::new().await;
    let saved = test
        .repository
        .put_stream_part(&test.session, &part(), 110)
        .await
        .unwrap()
        .unwrap();
    let frozen = test
        .repository
        .freeze_completion(&test.session, &selection(&saved), 120)
        .await
        .unwrap()
        .unwrap();
    test.probe.conflict_absent(test.pointer_key(1), 0);
    assert!(!test
        .store
        .compare_exchange(
            test.pointer_key(1),
            saved.encode().unwrap(),
            saved.encode().unwrap()
        )
        .await
        .unwrap());
    assert!(!test
        .raw_upload()
        .await
        .iter()
        .any(|row| row.key == test.pointer_key(1)));
    let before = test.probe.counts();
    test.restart().publish_completion(&frozen).await.unwrap();
    assert_eq!(
        test.delta(before),
        (2, 1),
        "publication must not reload the missing current pointer"
    );
    test.assert_object(&saved).await;
}

#[tokio::test]
async fn abort_before_pointer_cas_rejects_the_late_ack_and_retains_both_generations() {
    let test = TestIndependent::new().await;
    test.repository
        .put_stream_part(&test.session, &part(), 110)
        .await
        .unwrap();
    let barrier = test.probe.pause_mutation(test.pointer_key(1));
    let writer = spawn_write(&test, replacement());
    tokio::time::timeout(Duration::from_secs(2), barrier.started)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        test.repository.abort(&test.session).await.unwrap().phase,
        MultipartPhase::Aborted
    );
    barrier.resume.send(()).unwrap();
    assert!(matches!(
        writer.await.unwrap(),
        Err(MultipartRepositoryError::Conflict)
    ));
    assert_eq!(
        test.repository
            .part(&test.session, 1)
            .await
            .unwrap()
            .unwrap()
            .revision,
        2
    );
    assert_eq!(test.raw_upload().await.len(), 4);
    assert!(matches!(
        test.restart().publish_completion(&test.session).await,
        Err(MultipartRepositoryError::Conflict)
    ));
}

#[tokio::test]
async fn abort_before_final_upload_observation_rejects_ack_without_removing_pointer_evidence() {
    let test = TestIndependent::new().await;
    let barrier = test.probe.pause_get(test.upload_key(), 1);
    let writer = spawn_write(&test, part());
    tokio::time::timeout(Duration::from_secs(2), barrier.started)
        .await
        .unwrap()
        .unwrap();
    test.repository.abort(&test.session).await.unwrap();
    barrier.resume.send(()).unwrap();
    assert!(matches!(
        writer.await.unwrap(),
        Err(MultipartRepositoryError::Conflict)
    ));
    assert_eq!(
        test.repository
            .part(&test.session, 1)
            .await
            .unwrap()
            .unwrap()
            .raw_md5,
        part().raw_md5
    );
    assert_eq!(test.raw_upload().await.len(), 3);
}

#[tokio::test]
async fn abort_after_final_upload_observation_does_not_retroactively_cancel_ack() {
    let test = TestIndependent::new().await;
    let barrier = test.probe.pause_after_get(test.upload_key(), 1);
    let writer = spawn_write(&test, part());
    tokio::time::timeout(Duration::from_secs(2), barrier.started)
        .await
        .unwrap()
        .unwrap();
    test.repository.abort(&test.session).await.unwrap();
    barrier.resume.send(()).unwrap();
    let saved = writer.await.unwrap().unwrap().unwrap();
    assert_eq!(saved.raw_md5, part().raw_md5);
    assert_eq!(
        test.repository.load(&test.session).await.unwrap().unwrap().phase,
        MultipartPhase::Aborted
    );
    assert_eq!(
        test.repository
            .part_generation(&test.session, 1, saved.revision)
            .await
            .unwrap(),
        Some(saved)
    );
}

#[tokio::test]
async fn read_only_matching_replay_keeps_initial_admission_across_a_later_abort() {
    let test = TestIndependent::new().await;
    let saved = test
        .repository
        .put_stream_part(&test.session, &part(), 110)
        .await
        .unwrap()
        .unwrap();
    let barrier = test.probe.pause_get(test.pointer_key(1), 0);
    let before = test.probe.counts();
    let writer = spawn_write(&test, part());
    tokio::time::timeout(Duration::from_secs(2), barrier.started)
        .await
        .unwrap()
        .unwrap();
    test.repository.abort(&test.session).await.unwrap();
    barrier.resume.send(()).unwrap();
    assert_eq!(writer.await.unwrap().unwrap(), Some(saved));
    assert_eq!(
        test.delta(before),
        (3, 1),
        "two replay GETs plus independent Abort GET/CAS"
    );
}

#[tokio::test]
async fn frozen_generation_is_authority_even_when_pointer_is_malformed_but_tampered_generation_is_rejected() {
    let test = TestIndependent::new().await;
    let first = test
        .repository
        .put_stream_part(&test.session, &part(), 110)
        .await
        .unwrap()
        .unwrap();
    let frozen = test
        .repository
        .freeze_completion(&test.session, &selection(&first), 120)
        .await
        .unwrap()
        .unwrap();
    test.store
        .put(test.pointer_key(1), b"malformed current pointer".to_vec())
        .await
        .unwrap();
    let before = test.probe.counts();
    test.repository.publish_completion(&frozen).await.unwrap();
    assert_eq!(test.delta(before), (2, 1));
    test.assert_object(&first).await;
    let mut changed = first.clone();
    changed.locations[0].offset += 39;
    test.store
        .put(test.generation_key(1, 1), changed.encode().unwrap())
        .await
        .unwrap();
    let before = test.probe.counts();
    assert!(matches!(
        test.restart().publish_completion(&frozen).await,
        Err(MultipartRepositoryError::InvalidPart)
    ));
    assert_eq!(test.delta(before), (2, 0));
    test.assert_object(&first).await;
}
