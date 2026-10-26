// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{budget, retirement_fixture::TestFixture};
use crowdb_access_dataset::{split::*, *};
use std::sync::atomic::Ordering;

#[tokio::test]
async fn modern_mark_replay_and_terminal_have_exact_metadata_costs() {
    let fixture = TestFixture::new().await;
    let before = fixture.store.counts();
    let marked = fixture
        .authority
        .delete_split_revision(
            &fixture.identity,
            fixture.snapshot,
            fixture.record.revision,
            &ReadCancellation::new(),
        )
        .await
        .unwrap();
    let after = fixture.store.counts();
    assert_eq!((after.0 - before.0, after.1 - before.1), (4, 3));
    let before = fixture.store.counts();
    assert_eq!(
        fixture
            .authority
            .delete_split_revision(
                &fixture.identity,
                fixture.snapshot,
                fixture.record.revision,
                &ReadCancellation::new()
            )
            .await
            .unwrap(),
        marked
    );
    let after = fixture.store.counts();
    assert_eq!((after.0 - before.0, after.1 - before.1), (1, 0));
    let mut cursor = fixture.cursor();
    let before = fixture.store.counts();
    assert!(
        !fixture
            .authority
            .advance_split_revision_reclaim(&mut cursor, &ReadCancellation::new())
            .await
            .unwrap()
            .complete
    );
    assert_eq!(fixture.store.counts().1, before.1);
    for page in fixture.pages() {
        assert_ne!(
            fixture.store.inner.get(&page).await.unwrap().unwrap().bytes,
            b"split-retired-v1"
        );
    }
    fixture.clock.store(2200, Ordering::Release);
    fixture.finish().await;
    fixture.assert_terminal().await;
    let before = fixture.store.counts();
    let terminal = fixture
        .authority
        .delete_split_revision(
            &fixture.identity,
            fixture.snapshot,
            fixture.record.revision,
            &ReadCancellation::new(),
        )
        .await
        .unwrap();
    assert!(terminal.complete);
    assert_eq!(terminal.reclaim_after, None);
    let after = fixture.store.counts();
    assert_eq!((after.0 - before.0, after.1 - before.1), (1, 0));
    assert!(fixture
        .authority
        .get_snapshot(&fixture.identity, fixture.snapshot)
        .await
        .is_ok());
}

#[tokio::test]
async fn every_unknown_mark_write_stops_and_restart_recovers_without_renewing_grace() {
    for applied in [false, true] {
        for offset in 0..3 {
            let fixture = TestFixture::new().await;
            fixture.store.unknown_write(offset, applied);
            let before = fixture.store.counts();
            assert!(matches!(
                fixture
                    .authority
                    .delete_split_revision(
                        &fixture.identity,
                        fixture.snapshot,
                        fixture.record.revision,
                        &ReadCancellation::new()
                    )
                    .await,
                Err(SplitLifecycleError::Store(StoreError::Rejected))
            ));
            let after = fixture.store.counts();
            assert_eq!((after.0 - before.0, after.1 - before.1), (4, offset + 1));
            fixture.store.resume();
            fixture.clock.store(1100, Ordering::Release);
            let restarted = DatasetAuthority::new(fixture.store.clone())
                .with_reclaim_clock_for_tests(fixture.clock.clone());
            let marked = restarted
                .delete_split_revision(
                    &fixture.identity,
                    fixture.snapshot,
                    fixture.record.revision,
                    &ReadCancellation::new(),
                )
                .await
                .unwrap();
            assert_eq!(
                marked.reclaim_after,
                Some(if offset == 0 && !applied { 2300 } else { 2200 })
            );
            assert!(restarted
                .get_split_revision(
                    &fixture.identity,
                    fixture.snapshot,
                    fixture.record.revision,
                    budget(),
                    &ReadCancellation::new()
                )
                .await
                .is_err());
        }
    }
}

#[tokio::test]
async fn current_head_is_protected_without_a_resource_write() {
    let fixture = TestFixture::new().await;
    let base = fixture
        .authority
        .inspect_split_head(&fixture.identity, fixture.snapshot)
        .await
        .unwrap();
    fixture
        .authority
        .update_split_head(
            &fixture.identity,
            fixture.snapshot,
            &SplitHeadUpdate {
                revision: fixture.record.revision,
                expected_revision: base.revision,
                expected_head: base.generation,
            },
        )
        .await
        .unwrap();
    let before = fixture.store.counts();
    assert!(matches!(
        fixture
            .authority
            .delete_split_revision(
                &fixture.identity,
                fixture.snapshot,
                fixture.record.revision,
                &ReadCancellation::new()
            )
            .await,
        Err(SplitLifecycleError::Frozen)
    ));
    let after = fixture.store.counts();
    assert_eq!((after.0 - before.0, after.1 - before.1), (4, 0));
    assert_eq!(
        fixture
            .authority
            .get_split_revision(
                &fixture.identity,
                fixture.snapshot,
                fixture.record.revision,
                budget(),
                &ReadCancellation::new()
            )
            .await
            .unwrap(),
        fixture.record
    );
}

#[tokio::test]
async fn cancelled_deletion_performs_no_storage_access() {
    let fixture = TestFixture::new().await;
    let cancel = ReadCancellation::new();
    cancel.cancel();
    let before = fixture.store.counts();
    assert!(fixture
        .authority
        .delete_split_revision(
            &fixture.identity,
            fixture.snapshot,
            fixture.record.revision,
            &cancel
        )
        .await
        .is_err());
    assert_eq!(fixture.store.counts(), before);
}
