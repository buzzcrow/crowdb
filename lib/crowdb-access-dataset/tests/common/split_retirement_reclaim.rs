// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::retirement_fixture::TestFixture;
use crowdb_access_dataset::{split::*, *};
use std::sync::atomic::Ordering;

#[tokio::test]
async fn terminal_roots_resweep_unknown_late_member_names_over_multiple_windows() {
    let fixture = TestFixture::new().await;
    fixture
        .authority
        .delete_split_revision(
            &fixture.identity,
            fixture.snapshot,
            fixture.record.revision,
            &ReadCancellation::new(),
        )
        .await
        .unwrap();
    fixture.clock.store(2200, Ordering::Release);
    fixture.finish().await;
    let keys = late_pages(&fixture, 45).await;
    let mut sweep = SplitRevisionReclaimSweep::default();
    for _ in 0..80 {
        fixture
            .authority
            .reclaim_split_revisions_page(&mut sweep, &ReadCancellation::new())
            .await
            .unwrap();
    }
    for key in keys {
        assert_eq!(
            fixture.store.inner.get(&key).await.unwrap().unwrap().bytes,
            b"split-retired-v1"
        );
    }
    fixture.assert_terminal().await;
}

async fn late_pages(fixture: &TestFixture, count: usize) -> Vec<Vec<u8>> {
    let mut keys = Vec::new();
    for index in 0..count {
        let name = SplitName::new(format!("late-{index:02}")).unwrap();
        let page = MembershipPage {
            version: 1,
            snapshot: fixture.snapshot,
            revision: fixture.record.revision,
            name: name.clone(),
            page: 0,
            members: vec![b"sample-2".to_vec()],
        };
        let key = key::membership_page(
            &fixture.identity,
            fixture.snapshot,
            fixture.record.revision,
            &name,
            0,
        )
        .unwrap();
        assert!(matches!(
            fixture
                .store
                .inner
                .compare_exchange(&key, None, &page.encode().unwrap())
                .await
                .unwrap(),
            CasOutcome::Applied(_)
        ));
        keys.push(key);
    }
    keys
}

#[tokio::test]
async fn reclaim_unknown_leaves_the_full_root_for_restart_and_cannot_reopen_pages() {
    for applied in [false, true] {
        let fixture = TestFixture::new().await;
        fixture
            .authority
            .delete_split_revision(
                &fixture.identity,
                fixture.snapshot,
                fixture.record.revision,
                &ReadCancellation::new(),
            )
            .await
            .unwrap();
        fixture.clock.store(2200, Ordering::Release);
        let original = fixture.record.encode().unwrap();
        fixture.store.unknown_write(0, applied);
        assert!(fixture
            .authority
            .advance_split_revision_reclaim(&mut fixture.cursor(), &ReadCancellation::new())
            .await
            .is_err());
        fixture.store.resume();
        assert!(fixture
            .store
            .inner
            .get(&fixture.root())
            .await
            .unwrap()
            .unwrap()
            .bytes
            .starts_with(b"split-deletion-v1\0"));
        fixture.finish().await;
        fixture.assert_terminal().await;
        assert!(matches!(
            fixture
                .store
                .inner
                .compare_exchange(&fixture.root(), None, &original)
                .await
                .unwrap(),
            CasOutcome::Conflict(_)
        ));
        for page in fixture.pages() {
            assert!(matches!(
                fixture
                    .store
                    .inner
                    .compare_exchange(&page, None, b"late page")
                    .await
                    .unwrap(),
                CasOutcome::Conflict(_)
            ));
        }
    }
}

#[tokio::test]
async fn cancelled_partial_cleanup_can_lose_its_cursor_and_rescan_sealed_pages() {
    let fixture = TestFixture::new().await;
    fixture
        .authority
        .delete_split_revision(
            &fixture.identity,
            fixture.snapshot,
            fixture.record.revision,
            &ReadCancellation::new(),
        )
        .await
        .unwrap();
    let keys = late_pages(&fixture, 45).await;
    fixture.clock.store(2200, Ordering::Release);
    let mut cursor = fixture.cursor();
    assert!(
        !fixture
            .authority
            .advance_split_revision_reclaim(&mut cursor, &ReadCancellation::new())
            .await
            .unwrap()
            .complete
    );
    let cancel = ReadCancellation::new();
    cancel.cancel();
    let before = fixture.store.counts();
    assert!(fixture
        .authority
        .advance_split_revision_reclaim(&mut cursor, &cancel)
        .await
        .is_err());
    assert_eq!(fixture.store.counts(), before);
    assert!(fixture
        .store
        .inner
        .get(&fixture.root())
        .await
        .unwrap()
        .unwrap()
        .bytes
        .starts_with(b"split-deletion-v1\0"));
    let restarted =
        DatasetAuthority::new(fixture.store.clone()).with_reclaim_clock_for_tests(fixture.clock.clone());
    let mut sweep = SplitRevisionReclaimSweep::default();
    for _ in 0..80 {
        restarted
            .reclaim_split_revisions_page(&mut sweep, &ReadCancellation::new())
            .await
            .unwrap();
    }
    for key in keys {
        assert_eq!(
            fixture.store.inner.get(&key).await.unwrap().unwrap().bytes,
            b"split-retired-v1"
        );
    }
    fixture.assert_terminal().await;
}

#[tokio::test]
async fn corrupt_discovery_keys_and_thirty_two_roots_do_not_hide_a_later_deletion() {
    let fixture = TestFixture::new().await;
    let revision = SplitRevisionId::from_bytes(&[255; 16]).unwrap();
    fixture
        .authority
        .prepare_split_revision(
            &fixture.identity,
            fixture.snapshot,
            &SplitPreparation {
                revision,
                policy: SplitPolicy::default(),
                splits: super::split_store::partition_splits(),
            },
            &ReadCancellation::new(),
        )
        .await
        .unwrap();
    fixture
        .authority
        .delete_split_revision(
            &fixture.identity,
            fixture.snapshot,
            revision,
            &ReadCancellation::new(),
        )
        .await
        .unwrap();
    for index in 1..=32 {
        let mut bytes = [0; 16];
        bytes[15] = index;
        let key = key::revision(
            &fixture.identity,
            fixture.snapshot,
            SplitRevisionId::from_bytes(&bytes).unwrap(),
        )
        .unwrap();
        assert!(matches!(
            fixture
                .store
                .inner
                .compare_exchange(&key, None, b"corrupt")
                .await
                .unwrap(),
            CasOutcome::Applied(_)
        ));
    }
    let mut malformed = key::revision(&fixture.identity, fixture.snapshot, revision).unwrap();
    malformed.truncate(malformed.len() - 15);
    assert!(matches!(
        fixture
            .store
            .inner
            .compare_exchange(&malformed, None, b"corrupt")
            .await
            .unwrap(),
        CasOutcome::Applied(_)
    ));
    fixture.clock.store(2200, Ordering::Release);
    let mut sweep = SplitRevisionReclaimSweep::default();
    let mut errors = 0;
    for _ in 0..300 {
        errors += usize::from(
            fixture
                .authority
                .reclaim_split_revisions_page(&mut sweep, &ReadCancellation::new())
                .await
                .is_err(),
        );
    }
    assert!(errors > 32);
    let root = key::revision(&fixture.identity, fixture.snapshot, revision).unwrap();
    assert_eq!(
        fixture.store.inner.get(&root).await.unwrap().unwrap().bytes,
        b"split-retired-v1"
    );
    assert_eq!(
        fixture.store.inner.get(&malformed).await.unwrap().unwrap().bytes,
        b"corrupt"
    );
}
