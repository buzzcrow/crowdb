// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{part, repository_with_probe, session};
use crowdb_access_s3::metadata::{
    BucketId, BucketNameRecord, ChunkKvMetadataStore, MetadataKey, MultipartExpirySweep, MultipartPhase,
    MultipartRepository, MultipartSessionRecord, ObjectRecord, TenantId,
};

#[tokio::test]
async fn an_oversized_legacy_row_retains_its_resource_and_allows_later_buckets_to_advance() {
    let (repository, probe, store) = repository_with_probe().await;
    let session = upload(1);
    let mut id = [0; 16];
    id[0] = 1;
    id[1] = 16;
    let object = ObjectRecord {
        bucket_id: BucketId::new(id),
        key: b"oversized".to_vec(),
        logical_length: 1,
        checksum: vec![7; 16],
        etag: "etag".into(),
        created_at_ms: 100,
        modified_at_ms: 100,
        content_type: "application/octet-stream".into(),
        attributes: vec![0; 5 * 1024 * 1024],
        data_reference: vec![7],
        data_length: 1,
    };
    let bytes = object.encode().unwrap();
    assert_eq!(ObjectRecord::decode(&bytes).unwrap(), object);
    let object_key = MetadataKey::object(&tenant(), object.bucket_id, &object.key).unwrap();
    store.put_if_absent(object_key.clone(), bytes).await.unwrap();
    bucket(&store, &session, b"zz-active", false).await;
    repository.begin(&session).await.unwrap();
    let mut sweep = MultipartExpirySweep::new(store.clone(), tenant());
    let scans = probe.scan_counts().0;
    assert!(matches!(
        sweep.step(250).await,
        Err(crowdb_access_s3::metadata::MultipartExpiryError::OversizedBucketRow)
    ));
    assert_eq!(probe.scan_counts().0 - scans, 1);
    assert_eq!(drain(&mut sweep, 10).await, (1, 0));
    assert_eq!(
        repository.load(&session).await.unwrap().unwrap().phase,
        MultipartPhase::Aborted
    );
    assert!(store.get(object_key).await.unwrap().is_some());
    assert_eq!(drain(&mut sweep, 10).await, (0, 1));
}

fn tenant() -> TenantId {
    TenantId::new(b"tenant".to_vec()).unwrap()
}

fn upload(index: u32) -> MultipartSessionRecord {
    let mut session = session();
    let mut bucket = [128; 16];
    bucket[12..].copy_from_slice(&index.to_be_bytes());
    session.bucket_id = BucketId::new(bucket);
    session
}

async fn bucket(
    store: &ChunkKvMetadataStore,
    session: &MultipartSessionRecord,
    name: &[u8],
    tombstone: bool,
) {
    let record = BucketNameRecord {
        tenant: tenant(),
        name: name.to_vec(),
        bucket_id: session.bucket_id,
        tombstone,
    };
    store
        .put_if_absent(
            MetadataKey::bucket_name(&tenant(), name).unwrap(),
            record.encode(),
        )
        .await
        .unwrap();
}

async fn drain(sweep: &mut MultipartExpirySweep, limit: usize) -> (usize, usize) {
    let mut expired = 0;
    let mut errors = 0;
    for _ in 0..limit {
        match sweep.step(250).await {
            Ok(step) => {
                expired += step.expired;
                if !step.more {
                    return (expired, errors);
                }
            }
            Err(_) => errors += 1,
        }
    }
    panic!("expiry did not finish one bounded scan round");
}

#[tokio::test]
async fn more_than_a_thousand_buckets_are_eventually_expired_without_a_full_drain_call() {
    let (repository, probe, store) = repository_with_probe().await;
    for index in 0..1_003 {
        let session = upload(index);
        bucket(&store, &session, format!("bucket-{index:04}").as_bytes(), false).await;
        repository.begin(&session).await.unwrap();
    }
    let before = probe.counts();
    let mut sweep = MultipartExpirySweep::new(store, tenant());
    // Short name keys also take one raw-key window; each ID takes discovery plus I.
    assert_eq!(drain(&mut sweep, 3_200).await, (1_003, 0));
    let after = probe.counts();
    assert_eq!((after.0 - before.0, after.1 - before.1), (1_003, 1_003));
    assert!(probe.scan_counts().1 <= 20);
}

#[tokio::test]
async fn empty_active_pages_and_large_legacy_object_rows_advance_by_their_raw_keys() {
    let (repository, probe, store) = repository_with_probe().await;
    let session = upload(1);
    bucket(&store, &session, b"00-tombstone", true).await;
    for index in 16..19 {
        let mut id = [0; 16];
        id[0] = 1;
        id[1] = index;
        let object = ObjectRecord {
            bucket_id: BucketId::new(id),
            key: b"object".to_vec(),
            logical_length: 1,
            checksum: vec![7; 16],
            etag: "etag".into(),
            created_at_ms: 100,
            modified_at_ms: 100,
            content_type: "application/octet-stream".into(),
            attributes: vec![0; 2_100_000],
            data_reference: vec![7],
            data_length: 1,
        };
        store
            .put_if_absent(
                MetadataKey::object(&tenant(), object.bucket_id, &object.key).unwrap(),
                object.encode().unwrap(),
            )
            .await
            .unwrap();
    }
    bucket(&store, &session, b"zz-active", false).await;
    repository.begin(&session).await.unwrap();
    let mut sweep = MultipartExpirySweep::new(store, tenant());
    assert_eq!(drain(&mut sweep, 20).await, (1, 0));
    assert_eq!(probe.scan_counts().1, 20);
    assert_eq!(
        repository.load(&session).await.unwrap().unwrap().phase,
        MultipartPhase::Aborted
    );
}

#[tokio::test]
async fn name_payloads_do_not_supply_authority_and_bad_indexes_cannot_pin_healthy_uploads() {
    let (repository, _, store) = repository_with_probe().await;
    let session = upload(1);
    store
        .put_if_absent(
            MetadataKey::bucket_name(&tenant(), b"00-corrupt").unwrap(),
            vec![1, 2, 3],
        )
        .await
        .unwrap();
    bucket(&store, &session, b"active", false).await;
    repository.begin(&session).await.unwrap();
    store
        .put_if_absent(
            MetadataKey::multipart_upload_index(&tenant(), session.bucket_id, b"00-corrupt", &[8; 16])
                .unwrap(),
            MetadataKey::multipart_session(&tenant(), session.bucket_id, &session.upload_id),
        )
        .await
        .unwrap();
    let mut sweep = MultipartExpirySweep::new(store, tenant());
    // Discovery uses only raw IDs; the bad canonical I binding still fails.
    assert_eq!(drain(&mut sweep, 20).await, (1, 1));
    assert_eq!(
        repository.load(&session).await.unwrap().unwrap().phase,
        MultipartPhase::Aborted
    );
}

#[tokio::test]
async fn a_bad_index_keeps_successful_partial_progress_and_does_not_reload_earlier_uploads() {
    let (repository, probe, store) = repository_with_probe().await;
    let mut first = upload(1);
    first.object_key = b"a".to_vec();
    let mut last = first.clone();
    last.object_key = b"c".to_vec();
    last.upload_id = [8; 16];
    bucket(&store, &first, b"active", false).await;
    repository.begin(&first).await.unwrap();
    repository.begin(&last).await.unwrap();
    store
        .put_if_absent(
            MetadataKey::multipart_upload_index(&tenant(), first.bucket_id, b"b", &[9; 16]).unwrap(),
            MetadataKey::multipart_session(&tenant(), first.bucket_id, &first.upload_id),
        )
        .await
        .unwrap();
    let mut sweep = MultipartExpirySweep::new(store, tenant());
    sweep.step(250).await.unwrap(); // Short name row.
    sweep.step(250).await.unwrap(); // First retained bucket ID.
    assert!(sweep.step(250).await.is_err());
    let before = probe.counts();
    assert_eq!(drain(&mut sweep, 10).await, (1, 0));
    let after = probe.counts();
    assert_eq!((after.0 - before.0, after.1 - before.1), (1, 1));
    assert_eq!(
        repository.load(&first).await.unwrap().unwrap().phase,
        MultipartPhase::Aborted
    );
    assert_eq!(
        repository.load(&last).await.unwrap().unwrap().phase,
        MultipartPhase::Aborted
    );
}

#[tokio::test]
async fn a_lost_abort_reply_preserves_local_progress_and_restart_rechecks_durable_facts() {
    let (repository, probe, store) = repository_with_probe().await;
    let first = upload(1);
    let mut second = first.clone();
    second.upload_id = [8; 16];
    bucket(&store, &first, b"active", false).await;
    repository.begin(&first).await.unwrap();
    repository.begin(&second).await.unwrap();
    let mut part = part();
    part.bucket_id = first.bucket_id;
    repository.put_stream_part(&first, &part, 110).await.unwrap();
    let mut sweep = MultipartExpirySweep::new(store.clone(), tenant());
    sweep.step(250).await.unwrap(); // Short name row.
    sweep.step(250).await.unwrap(); // First retained bucket ID.
    probe.lose_after(0);
    assert!(sweep.step(250).await.is_err());
    let before = probe.counts();
    assert_eq!(drain(&mut sweep, 10).await, (1, 0));
    let after = probe.counts();
    assert_eq!((after.0 - before.0, after.1 - before.1), (1, 1));
    let restarted = MultipartRepository::new(store.clone(), tenant());
    assert_eq!(
        restarted.load(&first).await.unwrap().unwrap().phase,
        MultipartPhase::Aborted
    );
    assert_eq!(
        restarted.load(&second).await.unwrap().unwrap().phase,
        MultipartPhase::Aborted
    );
    assert!(restarted.part_generation(&first, 1, 1).await.unwrap().is_some());
    let before = probe.counts();
    assert_eq!(
        drain(&mut MultipartExpirySweep::new(store, tenant()), 10).await,
        (0, 0)
    );
    let after = probe.counts();
    assert_eq!((after.0 - before.0, after.1 - before.1), (2, 0));
}

#[tokio::test]
async fn one_bucket_scan_failure_defers_only_that_bucket_and_next_round_rediscovers_it() {
    let (repository, probe, store) = repository_with_probe().await;
    let first = upload(1);
    let second = upload(2);
    for (name, session) in [(b"a".as_slice(), &first), (b"b".as_slice(), &second)] {
        bucket(&store, session, name, false).await;
        repository.begin(session).await.unwrap();
    }
    probe.fail_scan(MetadataKey::multipart_session_prefix(&tenant(), first.bucket_id));
    let mut sweep = MultipartExpirySweep::new(store, tenant());
    assert_eq!(drain(&mut sweep, 10).await, (1, 1));
    assert_eq!(
        repository.load(&first).await.unwrap().unwrap().phase,
        MultipartPhase::Open
    );
    assert_eq!(
        repository.load(&second).await.unwrap().unwrap().phase,
        MultipartPhase::Aborted
    );
    probe.resume_scans();
    assert_eq!(drain(&mut sweep, 10).await, (1, 0));
    assert_eq!(
        repository.load(&first).await.unwrap().unwrap().phase,
        MultipartPhase::Aborted
    );
}

#[tokio::test]
async fn cancelled_bucket_scan_is_discardable_and_restarting_recovers_the_skipped_bucket() {
    let (repository, probe, store) = repository_with_probe().await;
    let first = upload(1);
    let second = upload(2);
    for (name, session) in [(b"a".as_slice(), &first), (b"b".as_slice(), &second)] {
        bucket(&store, session, name, false).await;
        repository.begin(session).await.unwrap();
    }
    let mut sweep = MultipartExpirySweep::new(store.clone(), tenant());
    sweep.step(250).await.unwrap(); // First short name row.
    sweep.step(250).await.unwrap(); // Second short name row.
    sweep.step(250).await.unwrap(); // First retained bucket ID.
    let paused = probe.pause_scan(MetadataKey::multipart_session_prefix(&tenant(), first.bucket_id));
    let mut work = Box::pin(sweep.step(250));
    tokio::select! {
        result = &mut work => panic!("held scan completed: {result:?}"),
        result = paused => result.unwrap(),
    }
    drop(work);
    probe.resume_scans();
    assert_eq!(drain(&mut sweep, 10).await, (1, 0));
    assert_eq!(
        repository.load(&first).await.unwrap().unwrap().phase,
        MultipartPhase::Open
    );
    assert_eq!(
        repository.load(&second).await.unwrap().unwrap().phase,
        MultipartPhase::Aborted
    );
    assert_eq!(
        drain(&mut MultipartExpirySweep::new(store, tenant()), 10).await,
        (1, 0)
    );
}

#[tokio::test]
async fn large_upload_sets_keep_bounded_pages_and_binary_raw_continuation() {
    let (repository, probe, store) = repository_with_probe().await;
    let mut session = upload(1);
    session.object_key = b"object\0with-zero".to_vec();
    bucket(&store, &session, b"active", false).await;
    for index in 0_u32..43 {
        session.upload_id[12..].copy_from_slice(&index.to_be_bytes());
        repository.begin(&session).await.unwrap();
    }
    let mut sweep = MultipartExpirySweep::new(store, tenant());
    sweep.step(250).await.unwrap(); // Short name row.
    sweep.step(250).await.unwrap(); // First retained bucket ID.
    for expected in [20, 20, 3] {
        let before = probe.counts();
        let scans = probe.scan_counts().0;
        let step = sweep.step(250).await.unwrap();
        assert_eq!(step.expired, expected);
        let after = probe.counts();
        assert_eq!((after.0 - before.0, after.1 - before.1), (expected, expected));
        assert_eq!(probe.scan_counts().0 - scans, 1);
    }
    assert!(!sweep.step(250).await.unwrap().more);
    assert_eq!(probe.scan_counts().1, 20);
}
