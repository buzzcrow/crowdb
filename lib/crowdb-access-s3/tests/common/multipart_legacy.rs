// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{
    part, repository_with_probe, session, unknown, CompletionPart, MetadataKey, MultipartPhase,
    MultipartSessionRecord, ObjectRecord, TenantId,
};
use bincode::Options as _;
use crowdb_access_s3::metadata::MultipartRepository;

fn tenant() -> TenantId {
    TenantId::new(b"tenant".to_vec()).unwrap()
}

fn selection() -> [CompletionPart; 1] {
    [CompletionPart {
        number: 1,
        etag: "09".repeat(16),
    }]
}

fn legacy_value(record: &MultipartSessionRecord, phase: MultipartPhase) -> Vec<u8> {
    let mut bytes = record.encode().unwrap();
    bytes[4] = 3;
    let options = bincode::DefaultOptions::new().with_fixint_encoding();
    let prefix = options
        .serialize(&(
            record.bucket_id,
            &record.object_key,
            record.upload_id,
            record.revision,
        ))
        .unwrap();
    let phase_offset = 5 + prefix.len();
    let encoded_phase = options.serialize(&phase).unwrap();
    bytes[phase_offset..phase_offset + encoded_phase.len()].copy_from_slice(&encoded_phase);
    let bound = matches!(
        phase,
        MultipartPhase::Completing | MultipartPhase::Publishing | MultipartPhase::Published
    );
    let published = matches!(phase, MultipartPhase::Publishing | MultipartPhase::Published);
    let receipt = (
        bound.then_some([5_u8; 32]),
        published.then_some(record.created_ms + 1),
        published.then_some(if phase == MultipartPhase::Publishing {
            Some([9_u8; 32])
        } else {
            None
        }),
        published.then(|| format!("{}-{}", "11".repeat(16), record.selection.as_ref().unwrap().len())),
    );
    bytes.extend_from_slice(&options.serialize(&receipt).unwrap());
    bytes
}

#[tokio::test]
async fn legacy_open_uses_original_bytes_for_reservation_then_new_bytes_for_clear() {
    let (repository, probe, store) = repository_with_probe().await;
    let session = session();
    repository.begin(&session).await.unwrap();
    let key = MetadataKey::multipart_session(&tenant(), session.bucket_id, &session.upload_id);
    store
        .put(key.clone(), legacy_value(&session, MultipartPhase::Open))
        .await
        .unwrap();
    let before = probe.counts();
    let saved = repository
        .put_stream_part(&session, &part(), 110)
        .await
        .unwrap()
        .unwrap();
    let after = probe.counts();
    assert_eq!((after.0 - before.0, after.1 - before.1), (2, 4));
    assert_eq!(saved.raw_md5, part().raw_md5);
    let value = store.get(key).await.unwrap().unwrap().value;
    assert!(value.starts_with(b"S3MS\x04"));
    let current = repository.load(&session).await.unwrap().unwrap();
    assert!(current.pending.is_none());
    assert!(current.legacy_observation.is_none());
    assert_eq!(current.revision, 3);
}

#[tokio::test]
async fn mutated_legacy_clone_cannot_reuse_the_original_expected_bytes() {
    let (repository, _, store) = repository_with_probe().await;
    let session = session();
    repository.begin(&session).await.unwrap();
    let key = MetadataKey::multipart_session(&tenant(), session.bucket_id, &session.upload_id);
    let original = legacy_value(&session, MultipartPhase::Open);
    store.put(key.clone(), original.clone()).await.unwrap();
    let mut forged = repository.load(&session).await.unwrap().unwrap();
    forged.content_type = "application/different".into();
    let mut next = forged.clone();
    next.revision += 1;
    next.phase = MultipartPhase::Aborted;
    assert!(!repository.exchange(&forged, &next).await.unwrap());
    assert_eq!(store.get(key).await.unwrap().unwrap().value, original);
}

#[tokio::test]
async fn legacy_pending_can_settle_after_restart_without_losing_generations() {
    let (repository, probe, store) = repository_with_probe().await;
    let session = session();
    repository.begin(&session).await.unwrap();
    probe.lose_after(1);
    unknown::assert_unknown(repository.put_stream_part(&session, &part(), 110).await);
    let pending = repository.load(&session).await.unwrap().unwrap();
    assert!(pending.pending.is_some());
    let key = MetadataKey::multipart_session(&tenant(), session.bucket_id, &session.upload_id);
    store
        .put(key, legacy_value(&pending, MultipartPhase::Open))
        .await
        .unwrap();
    let restarted = MultipartRepository::new(store, tenant());
    let saved = restarted
        .put_stream_part(&session, &part(), 110)
        .await
        .unwrap()
        .unwrap();
    assert!(restarted.load(&session).await.unwrap().unwrap().pending.is_none());
    assert_eq!(
        restarted
            .part_generation(&session, saved.number, saved.revision)
            .await
            .unwrap(),
        Some(saved)
    );
}

#[tokio::test]
async fn legacy_completion_phases_republish_without_mutating_the_upload_receipt() {
    for phase in [
        MultipartPhase::Completing,
        MultipartPhase::Publishing,
        MultipartPhase::Published,
    ] {
        let (repository, probe, store) = repository_with_probe().await;
        let session = session();
        repository.begin(&session).await.unwrap();
        repository.put_stream_part(&session, &part(), 110).await.unwrap();
        let frozen = repository
            .freeze_completion(&session, &selection(), 120)
            .await
            .unwrap()
            .unwrap();
        let key = MetadataKey::multipart_session(&tenant(), session.bucket_id, &session.upload_id);
        let legacy = legacy_value(&frozen, phase);
        store.put(key.clone(), legacy.clone()).await.unwrap();
        let object_key = MetadataKey::object(&tenant(), session.bucket_id, &session.object_key).unwrap();
        store
            .put(object_key.clone(), b"later ordinary PUT".to_vec())
            .await
            .unwrap();
        let before = probe.counts();
        let completed = repository.publish_completion(&session).await.unwrap();
        let after = probe.counts();
        assert_eq!((after.0 - before.0, after.1 - before.1), (3, 1));
        assert_eq!(store.get(key).await.unwrap().unwrap().value, legacy);
        let object = ObjectRecord::decode(&store.get(object_key).await.unwrap().unwrap().value).unwrap();
        assert_eq!(object.etag, completed.etag);
        assert_eq!(object.logical_length, part().length);
        let loaded = repository.load(&session).await.unwrap().unwrap();
        assert_eq!(loaded.phase, MultipartPhase::Publishing);
        assert_eq!(loaded.encode().unwrap(), frozen.encode().unwrap());
        assert!(loaded.encode().unwrap().len() < legacy.len());
    }
}

#[tokio::test]
async fn legacy_binding_cas_unknown_stops_without_confirmation_and_retry_works() {
    let (repository, probe, store) = repository_with_probe().await;
    let session = session();
    repository.begin(&session).await.unwrap();
    repository.put_stream_part(&session, &part(), 110).await.unwrap();
    let open = repository.load(&session).await.unwrap().unwrap();
    let key = MetadataKey::multipart_session(&tenant(), session.bucket_id, &session.upload_id);
    store
        .put(key, legacy_value(&open, MultipartPhase::Open))
        .await
        .unwrap();
    probe.lose_after(0);
    let before = probe.counts();
    unknown::assert_unknown(repository.freeze_completion(&session, &selection(), 120).await);
    let after = probe.counts();
    assert_eq!((after.0 - before.0, after.1 - before.1), (2, 1));
    let restarted = MultipartRepository::new(store, tenant());
    let bound = restarted
        .freeze_completion(&session, &selection(), 120)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        restarted.publish_completion(&bound).await.unwrap().logical_length,
        part().length
    );
}

#[test]
fn legacy_decode_still_rejects_an_incoherent_phase_and_trailing_bytes() {
    let session = session();
    let mut bytes = legacy_value(&session, MultipartPhase::Open);
    bytes.push(0);
    assert!(MultipartSessionRecord::decode(
        &bytes,
        session.bucket_id,
        &session.object_key,
        &session.upload_id
    )
    .is_err());
    let bytes = legacy_value(&session, MultipartPhase::Completing);
    assert!(MultipartSessionRecord::decode(
        &bytes,
        session.bucket_id,
        &session.object_key,
        &session.upload_id
    )
    .is_err());
}
