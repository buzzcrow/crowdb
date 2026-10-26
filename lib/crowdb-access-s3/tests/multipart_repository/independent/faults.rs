// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_access_s3::metadata::{MultipartRecordError, MultipartRepositoryError};

use super::super::unknown::assert_unknown;
use super::{part, selection, MultipartPartRecord, TestIndependent};

#[tokio::test]
async fn each_independent_mutation_unknown_stops_without_confirmation_and_fresh_retry_recovers() {
    for after_apply in [false, true] {
        for offset in 0..2 {
            let test = TestIndependent::new().await;
            let before = test.probe.counts();
            let start = test.probe.operation_log().len();
            if after_apply {
                test.probe.lose_after(offset);
            } else {
                test.probe.fail_before(offset);
            }
            assert_unknown(test.repository.put_stream_part(&test.session, &part(), 110).await);
            assert_eq!(
                test.delta(before),
                (2, offset + 1),
                "after_apply={after_apply}, offset={offset}"
            );
            test.assert_no_upload_writes(start);
            let before = test.probe.counts();
            let restarted = test.restart();
            let saved = restarted
                .put_stream_part(&test.session, &part(), 110)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(
                test.delta(before),
                if after_apply && offset == 1 {
                    (2, 0)
                } else {
                    (3, 2)
                }
            );
            assert_eq!(saved.raw_md5, part().raw_md5);
            assert_eq!(saved.locations, part().locations);
            assert_eq!(
                restarted.part(&test.session, 1).await.unwrap(),
                Some(saved.clone())
            );
            assert_eq!(
                restarted
                    .part_generation(&test.session, 1, saved.revision)
                    .await
                    .unwrap(),
                Some(saved)
            );
            let current = restarted.load(&test.session).await.unwrap().unwrap();
            assert_eq!(
                (current.revision, current.part_count, current.staged_bytes),
                (1, 0, 0)
            );
            assert!(current.pending.is_none());
            assert_eq!(test.raw_upload().await.len(), 3);
        }
    }
}

#[tokio::test]
async fn final_upload_read_failure_is_not_confirmed_and_read_only_replay_recovers() {
    let test = TestIndependent::new().await;
    test.probe.fail_get(test.upload_key(), 1);
    let before = test.probe.counts();
    let start = test.probe.operation_log().len();
    assert_unknown(test.repository.put_stream_part(&test.session, &part(), 110).await);
    assert!(test.probe.get_fault_consumed());
    let log = test.probe.operation_log();
    let writes = log[start..].iter().filter(|entry| entry.mutation).count();
    assert_eq!(
        (log.len() - start - writes, writes),
        (3, 2),
        "transport attempted the final GET exactly once"
    );
    assert_eq!(
        test.delta(before),
        (2, 2),
        "the failing transport hook does not reach the actor"
    );
    test.assert_no_upload_writes(start);
    let before = test.probe.counts();
    assert_eq!(
        test.restart()
            .put_stream_part(&test.session, &part(), 111)
            .await
            .unwrap()
            .unwrap()
            .raw_md5,
        part().raw_md5
    );
    assert_eq!(test.delta(before), (2, 0));
}

async fn installed_winner(test: &TestIndependent, same_content: bool) -> MultipartPartRecord {
    test.repository
        .put_stream_part(&test.session, &part(), 110)
        .await
        .unwrap();
    let mut winner = part();
    winner.revision = 3;
    winner.raw_md5 = if same_content { [8; 16] } else { [6; 16] };
    winner.locations[0].offset += 78;
    test.store
        .put_if_absent(test.generation_key(1, 3), winner.encode().unwrap())
        .await
        .unwrap();
    winner
}

#[tokio::test]
async fn definite_pointer_conflicts_consume_matching_or_newer_observations_without_get() {
    for same_content in [false, true] {
        let test = TestIndependent::new().await;
        let winner = installed_winner(&test, same_content).await;
        let mut incoming = part();
        incoming.raw_md5 = [8; 16];
        incoming.locations[0].offset += 39;
        test.probe
            .conflict(test.pointer_key(1), winner.encode().unwrap(), 0);
        let before = test.probe.counts();
        let start = test.probe.operation_log().len();
        let saved = test
            .repository
            .put_stream_part(&test.session, &incoming, 111)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(test.delta(before), (3, if same_content { 2 } else { 4 }));
        test.assert_no_upload_writes(start);
        assert_eq!(saved.raw_md5, incoming.raw_md5);
        assert_eq!(
            saved.revision,
            if same_content { 3 } else { 4 },
            "the injected newer observation must be consumed"
        );
        assert_eq!(
            saved.locations,
            if same_content {
                winner.locations
            } else {
                incoming.locations
            }
        );
        test.probe.clear_conflict_check();
        assert_eq!(test.restart().part(&test.session, 1).await.unwrap(), Some(saved));
    }
}

#[tokio::test]
async fn absent_pointer_conflict_reuses_retained_generations_without_confirmation_get() {
    let test = TestIndependent::new().await;
    test.repository
        .put_stream_part(&test.session, &part(), 110)
        .await
        .unwrap();
    let mut incoming = part();
    incoming.raw_md5 = [8; 16];
    test.probe.conflict_absent(test.pointer_key(1), 0);
    let before = test.probe.counts();
    let saved = test
        .repository
        .put_stream_part(&test.session, &incoming, 111)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(test.delta(before), (3, 5));
    assert_eq!(saved.revision, 2);
    assert_eq!(saved.raw_md5, incoming.raw_md5);
    test.probe.clear_conflict_check();
    assert_eq!(test.restart().part(&test.session, 1).await.unwrap(), Some(saved));
    assert_eq!(test.raw_upload().await.len(), 4);
}

#[tokio::test]
async fn corrupt_or_foreign_pointer_observation_is_rejected_without_more_rpc() {
    for corrupt in [false, true] {
        let test = TestIndependent::new().await;
        test.repository
            .put_stream_part(&test.session, &part(), 110)
            .await
            .unwrap();
        let mut wrong = part();
        wrong.upload_id = [5; 16];
        let observed = if corrupt {
            b"not a part".to_vec()
        } else {
            wrong.encode().unwrap()
        };
        test.probe.conflict(test.pointer_key(1), observed, 0);
        let mut incoming = part();
        incoming.raw_md5 = [8; 16];
        let before = test.probe.counts();
        assert!(matches!(
            test.repository
                .put_stream_part(&test.session, &incoming, 111)
                .await,
            Err(MultipartRepositoryError::Record(
                MultipartRecordError::Invalid | MultipartRecordError::Identity
            ))
        ));
        assert_eq!(test.delta(before), (2, 2));
    }
}

#[tokio::test]
async fn immutable_generation_collision_reuses_equal_content_or_allocates_a_fresh_identity() {
    for same_content in [false, true] {
        let test = TestIndependent::new().await;
        let mut orphan = part();
        orphan.locations[0].offset += 78;
        if !same_content {
            orphan.raw_md5 = [6; 16];
        }
        test.store
            .put_if_absent(test.generation_key(1, 1), orphan.encode().unwrap())
            .await
            .unwrap();
        let before = test.probe.counts();
        let saved = test
            .repository
            .put_stream_part(&test.session, &part(), 110)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(test.delta(before), (3, if same_content { 2 } else { 3 }));
        assert_eq!(saved.revision, if same_content { 1 } else { 2 });
        assert_eq!(saved.raw_md5, part().raw_md5);
        assert_eq!(
            saved.locations,
            if same_content {
                orphan.locations.clone()
            } else {
                part().locations
            }
        );
        assert_eq!(
            test.repository
                .part_generation(&test.session, 1, 1)
                .await
                .unwrap(),
            Some(orphan)
        );
    }
}

#[tokio::test]
async fn freeze_and_publication_unknown_keep_the_generation_selection_for_fresh_retry() {
    for during_publication in [false, true] {
        let test = TestIndependent::new().await;
        let saved = test
            .repository
            .put_stream_part(&test.session, &part(), 110)
            .await
            .unwrap()
            .unwrap();
        let frozen = if during_publication {
            Some(
                test.repository
                    .freeze_completion(&test.session, &selection(&saved), 120)
                    .await
                    .unwrap()
                    .unwrap(),
            )
        } else {
            None
        };
        test.probe.lose_after(0);
        let before = test.probe.counts();
        if let Some(frozen) = frozen {
            assert_unknown(test.repository.publish_completion(&frozen).await);
        } else {
            assert_unknown(
                test.repository
                    .freeze_completion(&test.session, &selection(&saved), 120)
                    .await,
            );
        }
        assert_eq!(test.delta(before), (2, 1));
        let restarted = test.restart();
        let before = test.probe.counts();
        let frozen = restarted
            .freeze_completion(&test.session, &selection(&saved), 121)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(test.delta(before), (2, 0));
        assert_eq!(frozen.selection.as_ref().unwrap()[0].revision, saved.revision);
        let before = test.probe.counts();
        restarted.publish_completion(&frozen).await.unwrap();
        assert_eq!(test.delta(before), (2, 1));
        test.assert_object(&saved).await;
    }
}
