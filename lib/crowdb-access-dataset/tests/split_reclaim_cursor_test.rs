// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[path = "common/composition_reclaim.rs"]
#[allow(dead_code)]
mod fixture;
#[path = "common/legacy_drafts.rs"]
#[allow(dead_code)]
mod legacy;
#[path = "common/legacy_split.rs"]
mod legacy_split;
#[path = "common/publication.rs"]
mod publication;
#[path = "common/split_store.rs"]
#[allow(dead_code)]
mod split_store;
#[path = "common/upload.rs"]
#[allow(dead_code)]
mod upload;

use crowdb_access_dataset::{split::*, *};
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};

async fn finish(authority: &DatasetAuthority, cursor: &mut SplitReclaimCursor) {
    for _ in 0..32 {
        if authority
            .advance_split_reclaim(cursor, &ReadCancellation::new())
            .await
            .unwrap()
            .complete
        {
            return;
        }
    }
    panic!("Split cleanup did not complete");
}

#[tokio::test]
async fn readable_snapshot_members_wait_for_persisted_grace_and_keep_late_write_fences() {
    let test = fixture::TestCleanup::new().await;
    let clock = Arc::new(AtomicU64::new(1000));
    let authority = DatasetAuthority::new(test.store.clone()).with_reclaim_clock_for_tests(clock.clone());
    let record =
        legacy_split::prepared(test.store.as_ref(), &authority, &test.identity, test.parent, None).await;
    let member = record
        .splits
        .iter()
        .find(|split| !split.pages.is_empty())
        .unwrap();
    let key = key::membership_page(&test.identity, test.parent, record.revision, &member.name, 0).unwrap();
    let original = test.store.get(&key).await.unwrap().unwrap().bytes;
    let replacement = authority
        .prepare_snapshot_candidate(
            &test.identity,
            None,
            publication::publication_manifest(3),
            OperationId::random(),
        )
        .await
        .unwrap();
    split_store::publish_snapshot_head(&authority, &test.identity, replacement).await;
    authority
        .mark_snapshot_reclaim(&test.identity, test.parent)
        .await
        .unwrap();
    let checkpoint = legacy_split::state_key(b"DS\0SCG\0", &test.identity, record.operation);
    test.store
        .compare_exchange(&checkpoint, None, b"corrupt obsolete progress")
        .await
        .unwrap();
    let mut cursor = SplitReclaimCursor::new(test.identity.clone(), record.operation);
    test.store.frozen.store(true, Ordering::Relaxed);
    assert!(matches!(
        authority
            .advance_split_reclaim(&mut cursor, &ReadCancellation::new())
            .await,
        Err(SplitLifecycleError::Authority(AuthorityError::ReclaimPending {
            reclaim_after: 1300
        }))
    ));
    assert_eq!(test.store.get(&key).await.unwrap().unwrap().bytes, original);
    let mut sweep = SplitReclaimSweep::default();
    authority
        .reclaim_splits_page(&mut sweep, &ReadCancellation::new())
        .await
        .unwrap();
    assert!(
        !sweep.has_more(),
        "grace waiting must not keep a busy local queue"
    );
    clock.store(1300, Ordering::Release);
    cursor = SplitReclaimCursor::new(test.identity.clone(), record.operation);
    finish(&authority, &mut cursor).await;
    assert_eq!(
        test.store.get(&key).await.unwrap().unwrap().bytes,
        b"split-retired-v1"
    );
    assert!(test.store.get(&checkpoint).await.unwrap().is_none());
    assert!(matches!(
        test.store.compare_exchange(&key, None, &original).await.unwrap(),
        CasOutcome::Conflict(_)
    ));
}

#[tokio::test]
async fn sealed_incomplete_initial_members_cleanup_without_an_access_deadline() {
    let test = fixture::TestCleanup::new().await;
    let snapshot = test
        .authority
        .prepare_snapshot_candidate(
            &test.identity,
            Some(test.parent),
            publication::publication_manifest(2),
            OperationId::random(),
        )
        .await
        .unwrap();
    let record = legacy_split::prepared(
        test.store.as_ref(),
        &test.authority,
        &test.identity,
        snapshot,
        None,
    )
    .await;
    legacy_split::remove_root(test.store.as_ref(), &test.identity, &record).await;
    let sd = legacy_split::state_key(b"DS\0SD\0", &test.identity, record.operation);
    let old = test.store.get(&sd).await.unwrap().unwrap();
    let mut state: serde_json::Value = serde_json::from_slice(&old.bytes).unwrap();
    state["initial"] = true.into();
    legacy::replace(test.store.as_ref(), &sd, &old.bytes, &state).await;
    let cb = [
        b"DS\0CB\0".as_slice(),
        &test.identity.key().unwrap(),
        snapshot.as_bytes(),
    ]
    .concat();
    let old = test.store.get(&cb).await.unwrap().unwrap();
    let mut partial: serde_json::Value = serde_json::from_slice(&old.bytes).unwrap();
    partial["version"] = 3.into();
    partial["initial_split"] = serde_json::json!(record.revision);
    legacy::replace(test.store.as_ref(), &cb, &old.bytes, &partial).await;
    let operation = OperationId::random();
    legacy::editing_composition(
        test.store.as_ref(),
        &test.authority,
        &test.identity,
        operation,
        test.parent,
    )
    .await;
    legacy::prepared_composition(test.store.as_ref(), &test.identity, operation, snapshot).await;
    test.authority
        .abandon_composition(&test.identity, operation, 1)
        .await
        .unwrap();
    let admission = [
        b"DS\0L\0".as_slice(),
        &test.identity.key().unwrap(),
        snapshot.as_bytes(),
    ]
    .concat();
    test.store.delete(&admission).await.unwrap();
    assert_eq!(
        test.store.get(&cb).await.unwrap().unwrap().bytes,
        b"retired-snapshot-content-v1"
    );
    test.store.frozen.store(true, Ordering::Relaxed);
    let mut cursor = SplitReclaimCursor::new(test.identity.clone(), record.operation);
    finish(&test.authority, &mut cursor).await;
    assert!(test.store.get(&admission).await.unwrap().is_none());
    for split in &record.splits {
        for page in &split.pages {
            assert_eq!(
                test.store
                    .get(
                        &key::membership_page(
                            &test.identity,
                            snapshot,
                            record.revision,
                            &split.name,
                            page.page
                        )
                        .unwrap()
                    )
                    .await
                    .unwrap()
                    .unwrap()
                    .bytes,
                b"split-retired-v1"
            );
        }
    }
}

async fn queued_split_drafts(test: &fixture::TestCleanup) -> Vec<(OperationId, Vec<u8>)> {
    let mut drafts = Vec::new();
    for marker in 1..=39 {
        let operation = OperationId::from_bytes(&[marker; 16]).unwrap();
        legacy_split::state(
            test.store.as_ref(),
            &test.identity,
            &SplitDraftRecord {
                version: 1,
                operation,
                snapshot: test.parent,
                expected_revision: None,
                revision: 1,
                policy: SplitPolicy::default(),
                splits: Vec::new(),
                progress: composition::DraftProgress::Abandoned,
            },
        )
        .await;
        let key = legacy_split::state_key(b"DS\0SD\0", &test.identity, operation);
        drafts.push((operation, test.store.get(&key).await.unwrap().unwrap().bytes));
        test.store
            .compare_exchange(
                &legacy_split::state_key(b"DS\0SCG\0", &test.identity, operation),
                None,
                b"corrupt obsolete progress",
            )
            .await
            .unwrap();
    }
    drafts
}

#[tokio::test(start_paused = true)]
async fn bounded_split_sweep_restarts_and_rotates_a_slow_draft_without_checkpoint_writes() {
    let test = fixture::TestCleanup::new().await;
    let drafts = queued_split_drafts(&test).await;
    test.store.frozen.store(true, Ordering::Relaxed);
    test.store.stalled.store(Some(Arc::new(legacy_split::state_key(
        b"DS\0SD\0",
        &test.identity,
        drafts[0].0,
    ))));
    let authority = DatasetAuthority::new(test.store.clone());
    let mut cursor = SplitReclaimSweep::default();
    let mut stalled = false;
    for turn in 0..300 {
        if turn == 7 {
            cursor = SplitReclaimSweep::default();
        }
        match authority
            .reclaim_splits_page(&mut cursor, &ReadCancellation::new())
            .await
        {
            Ok(()) => {}
            Err(SplitLifecycleError::Limit) => stalled = true,
            result => panic!("unexpected cleanup result: {result:?}"),
        }
        assert!(cursor.pending_drafts() <= 32);
        let mut remaining = 0;
        for (operation, _) in &drafts[1..] {
            remaining += usize::from(
                test.store
                    .get(&legacy_split::state_key(b"DS\0SCG\0", &test.identity, *operation))
                    .await
                    .unwrap()
                    .is_some(),
            );
        }
        if remaining == 0 {
            break;
        }
    }
    assert!(stalled);
    let cancel = ReadCancellation::new();
    cancel.cancel();
    let writes = test.store.inner.mutations.load(Ordering::Relaxed);
    assert!(authority.reclaim_splits_page(&mut cursor, &cancel).await.is_err());
    assert_eq!(test.store.inner.mutations.load(Ordering::Relaxed), writes);
    test.store.stalled.store(None);
    for _ in 0..100 {
        authority
            .reclaim_splits_page(&mut cursor, &ReadCancellation::new())
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
                .get(&legacy_split::state_key(b"DS\0SD\0", &test.identity, operation))
                .await
                .unwrap()
                .unwrap()
                .bytes,
            original
        );
        assert!(test
            .store
            .get(&legacy_split::state_key(b"DS\0SCG\0", &test.identity, operation))
            .await
            .unwrap()
            .is_none());
        let mut end = legacy_split::state_key(b"DS\0SW\0", &test.identity, operation);
        end.extend(1u64.to_be_bytes());
        end.extend(0u64.to_be_bytes());
        assert!(matches!(
            test.store
                .compare_exchange(&end, None, b"late window")
                .await
                .unwrap(),
            CasOutcome::Conflict(_)
        ));
    }
}
