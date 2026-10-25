// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use bincode::Options as _;
use crowdb_access_s3::metadata::{MultipartRecordError, MultipartRepositoryError, PendingPartMutation};

use super::{modern_session, part, strict_v4, MultipartPhase, MultipartSessionRecord, TestIndependent};

fn bytes_with_version(session: &MultipartSessionRecord, version: u8) -> Vec<u8> {
    let mut bytes = b"S3MS".to_vec();
    bytes.push(version);
    bytes.extend(
        bincode::DefaultOptions::new()
            .with_fixint_encoding()
            .serialize(session)
            .unwrap(),
    );
    bytes
}

fn decode(
    bytes: &[u8],
    session: &MultipartSessionRecord,
) -> Result<MultipartSessionRecord, MultipartRecordError> {
    MultipartSessionRecord::decode(bytes, session.bucket_id, &session.object_key, &session.upload_id)
}

#[test]
fn modern_codec_has_a_distinct_version_and_legacy_body_is_unchanged() {
    let session = modern_session();
    let bytes = session.encode().unwrap();
    assert!(bytes.starts_with(b"S3MS\x05"));
    assert_eq!(decode(&bytes, &session).unwrap(), session);
    let legacy_bytes = bytes_with_version(&session, 4);
    let legacy = decode(&legacy_bytes, &session).unwrap();
    assert_eq!(legacy, session);
    assert_eq!(legacy.legacy_observation, Some((1, legacy_bytes.clone())));
    assert_eq!(legacy.encode().unwrap(), legacy_bytes);
    assert_eq!(&legacy_bytes[5..], &bytes[5..]);
    let mut unsupported = bytes.clone();
    unsupported[4] = 6;
    assert_eq!(decode(&unsupported, &session), Err(MultipartRecordError::Invalid));
}

#[test]
fn modern_codec_rejects_a_strict_quota_or_pending_payload_even_if_legacy_accepts_it() {
    let mut quota = modern_session();
    quota.max_parts = 2;
    quota.max_part_bytes = 6;
    quota.max_object_bytes = 10;
    quota.max_staged_bytes = 10;
    let mut pending = modern_session();
    pending.part_count = 1;
    pending.staged_bytes = 5;
    pending.pending = Some(PendingPartMutation {
        number: 1,
        before_revision: None,
        before_digest: None,
        after_revision: 1,
        after_digest: [5; 32],
        after_length: 5,
    });
    for session in [quota, pending] {
        assert_eq!(
            decode(&bytes_with_version(&session, 5), &session),
            Err(MultipartRecordError::Invalid)
        );
        assert!(decode(&bytes_with_version(&session, 4), &session).is_ok());
    }
}

#[tokio::test]
async fn ordinary_bounds_do_not_upgrade_an_explicit_v4_upload_or_remove_pending_recovery() {
    let session = strict_v4(&modern_session());
    let test = TestIndependent::with_session(session).await;
    let before = test.probe.counts();
    test.probe.lose_after(1);
    super::super::unknown::assert_unknown(test.repository.put_stream_part(&test.session, &part(), 110).await);
    assert_eq!(test.delta(before), (2, 2));
    let pending = test.repository.load(&test.session).await.unwrap().unwrap();
    assert!(pending.pending.is_some());
    assert!(pending.encode().unwrap().starts_with(b"S3MS\x04"));
    let restarted = test.restart();
    let saved = restarted
        .put_stream_part(&test.session, &part(), 111)
        .await
        .unwrap()
        .unwrap();
    let current = restarted.load(&test.session).await.unwrap().unwrap();
    assert!(current.pending.is_none());
    assert_eq!(
        (current.revision, current.part_count, current.staged_bytes),
        (3, 1, 5)
    );
    assert!(current.encode().unwrap().starts_with(b"S3MS\x04"));
    assert_eq!(
        restarted
            .part_generation(&test.session, 1, saved.revision)
            .await
            .unwrap(),
        Some(saved)
    );
}

#[tokio::test]
async fn session_exchange_rejects_switching_protocol_before_any_write() {
    for modern in [false, true] {
        let session = if modern {
            modern_session()
        } else {
            strict_v4(&modern_session())
        };
        let test = TestIndependent::with_session(session).await;
        let previous = test.repository.load(&test.session).await.unwrap().unwrap();
        let mut next = if modern {
            strict_v4(&previous)
        } else {
            modern_session()
        };
        next.revision = previous.revision + 1;
        next.phase = MultipartPhase::Aborted;
        let before = test.probe.counts();
        assert!(matches!(
            test.repository.exchange(&previous, &next).await,
            Err(MultipartRepositoryError::Conflict)
        ));
        assert_eq!(test.delta(before), (0, 0));
        assert_eq!(test.repository.load(&test.session).await.unwrap(), Some(previous));
    }
}

#[tokio::test]
async fn wrong_protocol_observation_is_not_accepted_as_a_successful_abort() {
    let test = TestIndependent::new().await;
    let mut winner = strict_v4(&test.session);
    winner.revision += 1;
    winner.phase = MultipartPhase::Aborted;
    test.probe
        .conflict(test.upload_key(), winner.encode().unwrap(), 0);
    let before = test.probe.counts();
    assert!(matches!(
        test.repository.abort(&test.session).await,
        Err(MultipartRepositoryError::Conflict)
    ));
    assert_eq!(test.delta(before), (1, 1));
    test.probe.clear_conflict_check();
    assert_eq!(test.repository.load(&test.session).await.unwrap(), Some(winner));
}

#[tokio::test]
async fn forged_legacy_observation_cannot_downgrade_a_modern_resource() {
    let test = TestIndependent::new().await;
    let original = test.session.encode().unwrap();
    let mut forged = test.session.clone();
    forged.legacy_observation = Some((forged.revision, original.clone()));
    let mut next = forged.clone();
    next.revision += 1;
    next.phase = MultipartPhase::Aborted;
    let before = test.probe.counts();
    assert!(matches!(
        test.repository.exchange(&forged, &next).await,
        Err(MultipartRepositoryError::Conflict)
    ));
    assert_eq!(test.delta(before), (0, 1));
    assert_eq!(
        test.store.get(test.upload_key()).await.unwrap().unwrap().value,
        original
    );
}
