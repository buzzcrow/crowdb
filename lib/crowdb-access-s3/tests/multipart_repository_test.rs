// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::sync::atomic::Ordering;

use crowdb_access_multipart::SelectedPart;
use crowdb_access_s3::metadata::{
    BucketId, CompletionPart, MetadataKey, MultipartPartRecord, MultipartPhase, MultipartRepositoryError,
    MultipartSessionRecord, ObjectRecord, PendingPartMutation, TenantId,
};
use crowdb_protocol::chunkdb::rpc::Location;
use crowdb_protocol::common::ChunkId;
use sha2::{Digest as _, Sha256};

#[path = "common/multipart_actor.rs"]
mod actor;
use actor::{repository, repository_with_probe};
#[path = "common/multipart_completion_request.rs"]
mod completion_request;
#[path = "common/multipart_cost.rs"]
mod cost;
#[path = "common/multipart_expiry.rs"]
mod expiry;
#[path = "multipart_repository/expiry_discovery.rs"]
mod expiry_discovery;
#[path = "common/multipart_expiry_sweep.rs"]
mod expiry_sweep;
#[path = "multipart_repository/independent.rs"]
mod independent;
#[path = "common/multipart_legacy.rs"]
mod legacy;
#[path = "common/multipart_progress.rs"]
mod progress;
#[path = "common/retrieval_storage.rs"]
mod retrieval_storage;
#[path = "common/multipart_unknown.rs"]
mod unknown;

fn session() -> MultipartSessionRecord {
    let record = MultipartSessionRecord {
        bucket_id: BucketId::new([3; 16]),
        object_key: b"object".to_vec(),
        upload_id: [7; 16],
        revision: 1,
        phase: MultipartPhase::Open,
        created_ms: 100,
        expires_ms: 200,
        content_type: "application/octet-stream".into(),
        attributes: crowdb_access_s3::metadata::UserMetadata::from_headers(&hyper::HeaderMap::from_iter([(
            hyper::header::HeaderName::from_static("x-amz-meta-mtime"),
            hyper::header::HeaderValue::from_static("123.456"),
        )]))
        .unwrap()
        .encode()
        .unwrap(),
        max_parts: 10,
        max_part_bytes: 100,
        max_object_bytes: 500,
        max_staged_bytes: 1_000,
        part_count: 0,
        staged_bytes: 0,
        pending: None,
        selection: None,
        legacy_observation: None,
    };
    // These accounting and interrupted-reservation cases exercise historical v4 resources.
    let mut bytes = record.encode().unwrap();
    bytes[4] = 4;
    MultipartSessionRecord::decode(&bytes, record.bucket_id, &record.object_key, &record.upload_id).unwrap()
}

fn part() -> MultipartPartRecord {
    MultipartPartRecord {
        bucket_id: BucketId::new([3; 16]),
        upload_id: [7; 16],
        number: 1,
        revision: 1,
        modified_ms: 0,
        length: 5,
        raw_md5: [9; 16],
        locations: vec![Location {
            chunk_id: Some(ChunkId { high: 1, low: 2 }),
            offset: 10,
            length: 39,
            logical_offset: 0,
            logical_length: 5,
        }],
    }
}

#[tokio::test]
async fn session_cas_and_independent_part_replacement_obey_the_freeze() {
    let (repository, lose_reply, _) = repository().await;
    let session = session();
    assert_eq!(repository.begin(&session).await.unwrap(), session);
    assert_eq!(repository.begin(&session).await.unwrap(), session);
    let mut changed = session.clone();
    changed.content_type = "text/plain".into();
    assert!(matches!(
        repository.begin(&changed).await,
        Err(MultipartRepositoryError::Conflict)
    ));

    let first = repository
        .put_stream_part(&session, &part(), 110)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(first.revision, 1);
    let replay = repository
        .put_stream_part(&session, &part(), 111)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(replay, first);
    let mut relocated_retry = part();
    relocated_retry.locations[0].offset += 10;
    assert_eq!(
        repository
            .put_stream_part(&session, &relocated_retry, 111)
            .await
            .unwrap(),
        Some(first.clone())
    );
    let mut replacement = part();
    replacement.locations[0].offset += 39;
    replacement.raw_md5 = [8; 16];
    let second = repository
        .put_stream_part(&session, &replacement, 112)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(second.revision, 2);
    assert_eq!(repository.part(&session, 1).await.unwrap(), Some(second));
    assert_eq!(
        repository.part_generation(&session, 1, 1).await.unwrap(),
        Some(first)
    );

    let current = repository.load(&session).await.unwrap().unwrap();
    let mut frozen = current.clone();
    frozen.revision += 1;
    frozen.phase = MultipartPhase::Publishing;
    frozen.part_count = 1;
    frozen.staged_bytes = 5;
    frozen.selection = Some(vec![SelectedPart {
        number: 1,
        revision: 2,
        digest: [4; 32],
    }]);
    lose_reply.store(true, Ordering::SeqCst);
    unknown::assert_unknown(repository.exchange(&current, &frozen).await);
    assert!(repository.exchange(&current, &frozen).await.unwrap());
    assert!(matches!(
        repository.put_stream_part(&session, &part(), 112).await,
        Err(MultipartRepositoryError::Conflict)
    ));
}

#[tokio::test]
async fn completion_freezes_exact_part_revision_and_replays_the_same_request() {
    let (repository, _, store) = repository().await;
    let session = session();
    repository.begin(&session).await.unwrap();
    let first = repository
        .put_stream_part(&session, &part(), 110)
        .await
        .unwrap()
        .unwrap();
    let requested = [CompletionPart {
        number: 1,
        etag: format!("\"{}\"", "09".repeat(16)),
    }];
    let frozen = repository
        .freeze_completion(&session, &requested, 120)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(frozen.phase, MultipartPhase::Publishing);
    assert_eq!(frozen.selection.as_ref().unwrap()[0].revision, first.revision);
    assert_eq!(
        repository
            .freeze_completion(&session, &requested, 121)
            .await
            .unwrap(),
        Some(frozen.clone())
    );
    assert!(matches!(
        repository.put_stream_part(&session, &part(), 122).await,
        Err(MultipartRepositoryError::Conflict)
    ));
    let published = repository.publish_completion(&frozen).await.unwrap();
    assert!(published.etag.ends_with("-1"));
    assert_eq!(repository.publish_completion(&session).await.unwrap(), published);
    let key = MetadataKey::object(
        &TenantId::new(b"tenant".to_vec()).unwrap(),
        session.bucket_id,
        &session.object_key,
    )
    .unwrap();
    let object = ObjectRecord::decode(&store.get(key).await.unwrap().unwrap().value).unwrap();
    assert_eq!(object.attributes, session.attributes);
    assert_eq!(object.logical_length, 5);
    assert_eq!(object.etag, published.etag);
    assert_eq!(object.checksum.len(), 18);
    let locations: Vec<Location> = bincode::deserialize(&object.data_reference).unwrap();
    assert_eq!(locations, part().locations);
}

#[tokio::test]
async fn completion_settles_an_interrupted_part_reservation_before_freezing() {
    let (repository, _, store) = repository().await;
    let session = session();
    repository.begin(&session).await.unwrap();
    let first = repository
        .put_stream_part(&session, &part(), 110)
        .await
        .unwrap()
        .unwrap();
    let mut replacement = first.clone();
    replacement.revision += 1;
    replacement.modified_ms = 111;
    replacement.raw_md5 = [8; 16];
    replacement.locations[0].offset += 39;
    let generation_key = MetadataKey::multipart_part_generation(
        &TenantId::new(b"tenant".to_vec()).unwrap(),
        session.bucket_id,
        &session.upload_id,
        1,
        replacement.revision,
    )
    .unwrap();
    store
        .put_if_absent(generation_key, replacement.encode().unwrap())
        .await
        .unwrap();
    let current = repository.load(&session).await.unwrap().unwrap();
    let mut reserved = current.clone();
    reserved.revision += 1;
    reserved.pending = Some(PendingPartMutation {
        number: 1,
        before_revision: Some(first.revision),
        before_digest: Some(Sha256::digest(first.encode().unwrap()).into()),
        after_revision: replacement.revision,
        after_digest: Sha256::digest(replacement.encode().unwrap()).into(),
        after_length: replacement.length,
    });
    assert!(repository.exchange(&current, &reserved).await.unwrap());
    let request = [CompletionPart {
        number: 1,
        etag: "08".repeat(16),
    }];
    assert!(repository
        .freeze_completion(&session, &request, 120)
        .await
        .unwrap()
        .is_none());
    assert_eq!(
        repository.part(&session, 1).await.unwrap(),
        Some(replacement.clone())
    );
    assert!(repository
        .load(&session)
        .await
        .unwrap()
        .unwrap()
        .pending
        .is_none());
    let frozen = repository
        .freeze_completion(&session, &request, 120)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        frozen.selection.as_ref().unwrap()[0].revision,
        replacement.revision
    );
}

#[tokio::test]
async fn completion_rejects_undersized_nonfinal_and_wrong_etag() {
    let (repository, _, _) = repository().await;
    let session = session();
    repository.begin(&session).await.unwrap();
    repository.put_stream_part(&session, &part(), 110).await.unwrap();
    let mut second = part();
    second.number = 2;
    repository.put_stream_part(&session, &second, 111).await.unwrap();
    let etag = "09".repeat(16);
    let request = [
        CompletionPart {
            number: 1,
            etag: etag.clone(),
        },
        CompletionPart { number: 2, etag },
    ];
    assert!(matches!(
        repository.freeze_completion(&session, &request, 120).await,
        Err(MultipartRepositoryError::EntityTooSmall)
    ));
    let wrong = [CompletionPart {
        number: 1,
        etag: "00".repeat(16),
    }];
    assert!(matches!(
        repository.freeze_completion(&session, &wrong, 120).await,
        Err(MultipartRepositoryError::InvalidPart)
    ));
    let current = repository.load(&session).await.unwrap().unwrap();
    assert_eq!(current.phase, MultipartPhase::Open);
    assert_eq!(current.part_count, 2);
    assert!(current.pending.is_none());
}

#[tokio::test]
async fn publication_retry_overwrites_a_competing_object_after_a_lost_reply() {
    let (repository, lose_reply, store) = repository().await;
    let session = session();
    repository.begin(&session).await.unwrap();
    repository.put_stream_part(&session, &part(), 110).await.unwrap();
    let request = [CompletionPart {
        number: 1,
        etag: "09".repeat(16),
    }];
    let frozen = repository
        .freeze_completion(&session, &request, 120)
        .await
        .unwrap()
        .unwrap();
    lose_reply.store(true, Ordering::SeqCst);
    unknown::assert_unknown(repository.publish_completion(&frozen).await);
    let published = repository.publish_completion(&frozen).await.unwrap();
    assert!(published.etag.ends_with("-1"));
    assert_eq!(repository.publish_completion(&frozen).await.unwrap(), published);

    let mut second = session.clone();
    second.upload_id = [8; 16];
    repository.begin(&second).await.unwrap();
    repository
        .put_stream_part(&second, &part_for(&second), 130)
        .await
        .unwrap();
    let frozen_second = repository
        .freeze_completion(&second, &request, 140)
        .await
        .unwrap()
        .unwrap();
    let key = MetadataKey::object(
        &TenantId::new(b"tenant".to_vec()).unwrap(),
        second.bucket_id,
        &second.object_key,
    )
    .unwrap();
    let previous = store.get(key.clone()).await.unwrap().unwrap();
    assert!(store
        .compare_exchange(key.clone(), previous.value, b"competing generation".to_vec())
        .await
        .unwrap());
    let completed = repository.publish_completion(&frozen_second).await.unwrap();
    let object = ObjectRecord::decode(&store.get(key).await.unwrap().unwrap().value).unwrap();
    assert_eq!(object.etag, completed.etag);
    assert_eq!(object.logical_length, part().length);
}

#[tokio::test]
async fn frozen_part_generation_rejects_a_late_pointer_change() {
    let (repository, _, store) = repository().await;
    let session = session();
    repository.begin(&session).await.unwrap();
    repository.put_stream_part(&session, &part(), 110).await.unwrap();
    let mut replacement = part();
    replacement.locations[0].offset += 39;
    replacement.raw_md5 = [8; 16];
    let selected = repository
        .put_stream_part(&session, &replacement, 111)
        .await
        .unwrap()
        .unwrap();
    let request = [CompletionPart {
        number: 1,
        etag: "08".repeat(16),
    }];
    let frozen = repository
        .freeze_completion(&session, &request, 120)
        .await
        .unwrap()
        .unwrap();
    let pointer_key = MetadataKey::multipart_part(
        &TenantId::new(b"tenant".to_vec()).unwrap(),
        session.bucket_id,
        &session.upload_id,
        1,
    )
    .unwrap();
    let mut late = selected.clone();
    late.revision = 3;
    late.locations[0].offset = 77;
    let previous = store.get(pointer_key.clone()).await.unwrap().unwrap();
    assert!(store
        .compare_exchange(pointer_key, previous.value, late.encode().unwrap())
        .await
        .unwrap());
    assert!(matches!(
        repository.publish_completion(&frozen).await,
        Err(MultipartRepositoryError::InvalidPart)
    ));
    let object_key = MetadataKey::object(
        &TenantId::new(b"tenant".to_vec()).unwrap(),
        session.bucket_id,
        &session.object_key,
    )
    .unwrap();
    assert!(store.get(object_key).await.unwrap().is_none());
}

fn part_for(session: &MultipartSessionRecord) -> MultipartPartRecord {
    let mut value = part();
    value.upload_id = session.upload_id;
    value
}

#[tokio::test]
async fn abort_retry_observes_terminal_state_and_rejects_part_publication() {
    let (repository, lose_reply, _) = repository().await;
    let session = session();
    repository.begin(&session).await.unwrap();
    repository.put_stream_part(&session, &part(), 110).await.unwrap();
    lose_reply.store(true, Ordering::SeqCst);
    unknown::assert_unknown(repository.abort(&session).await);
    let aborted = repository.abort(&session).await.unwrap();
    assert_eq!(aborted.phase, MultipartPhase::Aborted);
    assert_eq!(repository.abort(&session).await.unwrap(), aborted);
    assert!(matches!(
        repository.put_stream_part(&session, &part(), 120).await,
        Err(MultipartRepositoryError::Conflict)
    ));
    assert_eq!(
        repository
            .part_generation(&session, 1, 1)
            .await
            .unwrap()
            .unwrap()
            .length,
        5
    );
}

#[tokio::test]
async fn part_listing_paginates_current_generations_in_number_order() {
    let (repository, _, _) = repository().await;
    let session = session();
    repository.begin(&session).await.unwrap();
    for number in [3, 1, 2] {
        let mut value = part();
        value.number = number;
        repository.put_stream_part(&session, &value, 110).await.unwrap();
    }
    let mut replacement = part();
    replacement.number = 2;
    replacement.locations[0].offset += 39;
    replacement.raw_md5 = [8; 16];
    repository
        .put_stream_part(&session, &replacement, 111)
        .await
        .unwrap();
    let first = repository.list_parts(&session, 0, 2).await.unwrap();
    assert_eq!(
        first.parts.iter().map(|part| part.number).collect::<Vec<_>>(),
        [1, 2]
    );
    assert_eq!(first.parts[1].revision, 2);
    assert_eq!(first.next_part_number_marker, Some(2));
    let second = repository.list_parts(&session, 2, 2).await.unwrap();
    assert_eq!(
        second.parts.iter().map(|part| part.number).collect::<Vec<_>>(),
        [3]
    );
    assert_eq!(second.next_part_number_marker, None);
}

#[tokio::test]
async fn upload_prefix_retains_session_and_replaced_part_generations() {
    let (repository, _, store) = repository().await;
    let session = session();
    repository.begin(&session).await.unwrap();
    assert_eq!(
        repository
            .load_identity(session.bucket_id, &session.object_key, &session.upload_id)
            .await
            .unwrap(),
        Some(session.clone())
    );
    assert!(repository
        .load_identity(session.bucket_id, b"another", &session.upload_id)
        .await
        .unwrap()
        .is_none());
    repository.put_stream_part(&session, &part(), 110).await.unwrap();
    let mut replacement = part();
    replacement.locations[0].offset += 39;
    replacement.raw_md5 = [8; 16];
    repository
        .put_stream_part(&session, &replacement, 111)
        .await
        .unwrap();
    repository.abort(&session).await.unwrap();

    let tenant = TenantId::new(b"tenant".to_vec()).unwrap();
    let prefix = MetadataKey::multipart_upload_prefix(&tenant, session.bucket_id, &session.upload_id);
    let mut end = prefix.clone();
    end.push(u8::MAX);
    let records = store.scan(prefix, end, 10, 1024 * 1024).await.unwrap();
    assert_eq!(records.len(), 4);
    assert_eq!(
        records[0].key,
        MetadataKey::multipart_session(&tenant, session.bucket_id, &session.upload_id)
    );
    assert_eq!(
        records[1].key,
        MetadataKey::multipart_part(&tenant, session.bucket_id, &session.upload_id, 1).unwrap()
    );
    for revision in [1, 2] {
        assert!(records.iter().any(|record| record.key
            == MetadataKey::multipart_part_generation(
                &tenant,
                session.bucket_id,
                &session.upload_id,
                1,
                revision,
            )
            .unwrap()));
    }
}

#[tokio::test]
async fn upload_listing_filters_terminal_records_and_resumes_same_key() {
    let (repository, _, _) = repository().await;
    let mut first = session();
    first.object_key = b"pre/a".to_vec();
    first.upload_id = [1; 16];
    let mut second = first.clone();
    second.upload_id = [2; 16];
    second.created_ms = 101;
    let mut third = first.clone();
    third.object_key = b"pre/b".to_vec();
    third.upload_id = [3; 16];
    let mut terminal = first.clone();
    terminal.object_key = b"pre/aborted".to_vec();
    terminal.upload_id = [4; 16];
    for upload in [&first, &second, &third, &terminal] {
        repository.begin(upload).await.unwrap();
    }
    repository.abort(&terminal).await.unwrap();

    let page = repository
        .list_uploads(first.bucket_id, b"pre/", None, None, 1, 110)
        .await
        .unwrap();
    assert_eq!(page.uploads, [first.clone()]);
    assert_eq!(page.next, Some((first.object_key.clone(), first.upload_id)));
    let (key, id) = page.next.unwrap();
    let page = repository
        .list_uploads(first.bucket_id, b"pre/", Some(&key), Some(&id), 1, 110)
        .await
        .unwrap();
    assert_eq!(page.uploads, [second.clone()]);
    assert_eq!(page.next, Some((second.object_key.clone(), second.upload_id)));
    let page = repository
        .list_uploads(first.bucket_id, b"pre/", Some(b"pre/a"), None, 10, 110)
        .await
        .unwrap();
    assert_eq!(page.uploads, [third]);
    assert!(page.next.is_none());
    assert!(repository
        .list_uploads(first.bucket_id, b"pre/", None, Some(&id), 1, 110)
        .await
        .is_err());
}

#[tokio::test]
async fn expiry_pages_mark_old_uploads_terminal_without_removing_part_evidence() {
    let (repository, _, store) = repository().await;
    let first = session();
    let mut second = first.clone();
    second.upload_id = [8; 16];
    second.object_key = b"later".to_vec();
    second.expires_ms = 300;
    repository.begin(&first).await.unwrap();
    repository.begin(&second).await.unwrap();
    repository.put_stream_part(&first, &part(), 110).await.unwrap();

    let page = repository
        .expire_page(first.bucket_id, None, 250, 1)
        .await
        .unwrap();
    assert_eq!(page.expired, 0);
    let next = page.next.expect("another index page remains");
    let page = repository
        .expire_page(first.bucket_id, Some(&next), 250, 1)
        .await
        .unwrap();
    assert_eq!(page.expired, 1);
    assert!(page.next.is_none());
    assert_eq!(
        repository.load(&first).await.unwrap().unwrap().phase,
        MultipartPhase::Aborted
    );
    assert_eq!(
        repository.load(&second).await.unwrap().unwrap().phase,
        MultipartPhase::Open
    );
    assert!(repository.part_generation(&first, 1, 1).await.unwrap().is_some());

    let tenant = TenantId::new(b"tenant".to_vec()).unwrap();
    let index =
        MetadataKey::multipart_upload_index(&tenant, first.bucket_id, &first.object_key, &first.upload_id)
            .unwrap();
    assert!(store.get(index).await.unwrap().is_some());
}
