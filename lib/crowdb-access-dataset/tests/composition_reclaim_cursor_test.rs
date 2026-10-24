// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[path = "common/composition_reclaim.rs"]
mod fixture;
#[path = "common/legacy_drafts.rs"]
#[allow(dead_code)]
mod legacy;
#[path = "common/publication.rs"]
mod publication;
#[path = "common/upload.rs"]
#[allow(dead_code)]
mod upload;

use bytes::Bytes;
use crowdb_access_dataset::{composition::*, *};
use md5::{Digest, Md5};
use sha2::Sha256;
use std::sync::{atomic::Ordering, Arc};

async fn staged_uploads(test: &fixture::TestCleanup, operation: OperationId) -> Vec<Vec<u8>> {
    let mut hash = Sha256::new();
    hash.update(b"crowdb-composition-staging-v1\0");
    hash.update(operation.as_bytes());
    let owner = OperationId::from_bytes(&hash.finalize()[..16]).unwrap();
    let mut locations = Vec::new();
    for _ in 0..41 {
        let location = DatasetUploads::location(&test.identity, OperationId::random()).unwrap();
        test.uploads
            .create(&location, 4, 4, Md5::digest(b"data").into(), u64::MAX)
            .await
            .unwrap();
        test.uploads
            .put_part(&location, 0, Bytes::from_static(b"data"), 1)
            .await
            .unwrap();
        test.uploads.complete(&location, 2).await.unwrap();
        test.uploads.claim(&location, owner, 3).await.unwrap();
        locations.push(location);
    }
    let mut ids: Vec<_> = locations
        .iter()
        .map(|location| DatasetUploads::upload_id(&test.identity, location).unwrap())
        .collect();
    ids.sort_unstable();
    let key = legacy::key(b"DS\0CD\0", &test.identity, operation);
    let old = test.store.get(&key).await.unwrap().unwrap();
    let mut state: serde_json::Value = serde_json::from_slice(&old.bytes).unwrap();
    state["uploads"] = serde_json::json!({"staged":ids, "staged_retired":0, "candidates":[]});
    legacy::replace(test.store.as_ref(), &key, &old.bytes, &state).await;
    locations
}

#[tokio::test]
async fn upload_retirement_is_bounded_and_restarts_without_mutating_the_draft() {
    let test = fixture::TestCleanup::new().await;
    let operation = OperationId::random();
    test.draft(operation).await;
    let locations = staged_uploads(&test, operation).await;
    test.uploads
        .claim(&locations[0], OperationId::random(), 4)
        .await
        .unwrap();
    let key = legacy::key(b"DS\0CD\0", &test.identity, operation);
    let original = test.store.get(&key).await.unwrap().unwrap().bytes;
    test.store.frozen.store(true, Ordering::Relaxed);
    let mut cursor = CompositionReclaimCursor::new(test.identity.clone(), operation);
    let first = test
        .authority
        .cleanup_composition_uploads_step_for_tests(&mut cursor, &test.uploads)
        .await
        .unwrap();
    assert_eq!(first.retired_owners, 20);
    assert!(!first.complete);
    cursor = CompositionReclaimCursor::new(test.identity.clone(), operation);
    let restarted = DatasetAuthority::new(test.store.clone());
    let mut complete = false;
    for _ in 0..3 {
        let progress = restarted
            .cleanup_composition_uploads_step_for_tests(&mut cursor, &test.uploads)
            .await
            .unwrap();
        if progress.complete {
            assert_eq!(progress.retired_owners, 41);
            complete = true;
            break;
        }
    }
    assert!(complete);
    assert_eq!(test.store.get(&key).await.unwrap().unwrap().bytes, original);
    assert!(matches!(
        test.uploads.abort(&locations[0]).await,
        Err(UploadError::Conflict)
    ));
    for location in &locations[1..] {
        test.uploads.abort(location).await.unwrap();
    }
}

async fn queued_drafts(test: &fixture::TestCleanup) -> Vec<(OperationId, Vec<u8>)> {
    let mut drafts = Vec::new();
    for marker in 1..=39 {
        let operation = OperationId::from_bytes(&[marker; 16]).unwrap();
        drafts.push((operation, test.draft(operation).await));
    }
    drafts
}

#[tokio::test(start_paused = true)]
async fn background_reclaims_all_legacy_domains_after_cursor_loss_and_a_stalled_draft() {
    let test = fixture::TestCleanup::new().await;
    let drafts = queued_drafts(&test).await;
    test.store.frozen.store(true, Ordering::Relaxed);
    test.store.stalled.store(Some(Arc::new(legacy::key(
        b"DS\0CD\0",
        &test.identity,
        drafts[0].0,
    ))));
    let restarted = DatasetAuthority::new(test.store.clone());
    let mut cursor = CompositionReclaimSweep::default();
    let mut timed_out = false;
    for turn in 0..1000 {
        if turn == 11 {
            cursor = CompositionReclaimSweep::default();
        }
        match restarted
            .reclaim_compositions_page(&test.uploads, &mut cursor, &ReadCancellation::new())
            .await
        {
            Ok(()) => {}
            Err(CompositionError::Limit) => timed_out = true,
            result => panic!("unexpected cleanup result: {result:?}"),
        }
        assert!(cursor.pending_drafts() <= 32);
        let mut remaining = 0;
        for (operation, _) in &drafts[1..] {
            remaining += usize::from(
                test.store
                    .get(&legacy::key(b"DS\0CEG\0", &test.identity, *operation))
                    .await
                    .unwrap()
                    .is_some(),
            );
        }
        if remaining == 0 {
            break;
        }
    }
    assert!(timed_out);
    for (operation, _) in &drafts[1..] {
        assert!(
            test.store
                .get(&legacy::key(b"DS\0CEG\0", &test.identity, *operation))
                .await
                .unwrap()
                .is_none(),
            "stalled draft starved {operation}"
        );
    }
    let cancel = ReadCancellation::new();
    cancel.cancel();
    let writes = test.store.inner.mutations.load(Ordering::Relaxed);
    assert!(restarted
        .reclaim_compositions_page(&test.uploads, &mut cursor, &cancel)
        .await
        .is_err());
    assert_eq!(test.store.inner.mutations.load(Ordering::Relaxed), writes);
    test.store.stalled.store(None);
    for _ in 0..100 {
        restarted
            .reclaim_compositions_page(&test.uploads, &mut cursor, &ReadCancellation::new())
            .await
            .unwrap();
        if !cursor.has_more() {
            break;
        }
    }
    assert!(!cursor.has_more());
    for (operation, original) in drafts {
        assert_eq!(
            test.store
                .get(&legacy::key(b"DS\0CD\0", &test.identity, operation))
                .await
                .unwrap()
                .unwrap()
                .bytes,
            original,
            "cleanup must retain immutable discovery facts without recording progress"
        );
        for prefix in [b"DS\0CCP\0".as_slice(), b"DS\0CCG\0", b"DS\0CEG\0"] {
            assert!(test
                .store
                .get(&legacy::key(prefix, &test.identity, operation))
                .await
                .unwrap()
                .is_none());
        }
        let mut page = legacy::key(b"DS\0CW\0", &test.identity, operation);
        page.extend(1u64.to_be_bytes());
        page.extend(1u64.to_be_bytes());
        assert!(matches!(
            test.store
                .compare_exchange(&page, None, b"late work")
                .await
                .unwrap(),
            CasOutcome::Conflict(_)
        ));
    }
}
