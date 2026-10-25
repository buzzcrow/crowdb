// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[path = "common/publication.rs"]
mod publication;
#[path = "common/split_store.rs"]
mod split_store;
#[path = "common/read_query_split_window.rs"]
mod window;

use crowdb_access_dataset::{split::*, *};
use std::sync::atomic::Ordering;
use window::{all, ids, range, TestWindowFixture};

#[tokio::test]
async fn twenty_candidates_in_one_membership_page_read_it_once() {
    let fixture = TestWindowFixture::new(ids(45), range(0, 20)).await;
    assert_eq!(fixture.record.splits[0].pages.len(), 1);
    let (reads, binding) = fixture.bind(fixture.spec(range(0, 20))).await;
    let page = reads
        .next(&fixture.identity, &binding.cursor, &ReadCancellation::new())
        .await
        .unwrap();
    assert_eq!(page.ids, ids(20));
    assert_eq!(fixture.store.page_reads(), 1);
    assert_eq!(fixture.store.writes.load(Ordering::Relaxed), 0);
    assert_eq!(*fixture.store.caps.lock().unwrap(), vec![1024 * 1024]);
}

#[tokio::test]
async fn one_window_crosses_pages_without_rereading_any_search_midpoint() {
    let mut fixture = TestWindowFixture::new(ids(45), all()).await;
    fixture.repage(7).await;
    let (reads, binding) = fixture.bind(fixture.spec(range(0, 20))).await;
    let page = reads
        .next(&fixture.identity, &binding.cursor, &ReadCancellation::new())
        .await
        .unwrap();
    assert_eq!(page.ids, ids(20));
    assert_eq!(fixture.store.page_reads(), 4);
    assert!(fixture
        .store
        .pages
        .lock()
        .unwrap()
        .values()
        .all(|count| *count == 1));
}

#[tokio::test]
async fn empty_and_sparse_membership_keep_nonmembers_outside_and_between_pages() {
    for members in [vec![], ids(45).into_iter().filter(|id| id[0] % 2 == 0).collect()] {
        let fixture = TestWindowFixture::new(ids(45), Selection::Explicit(members.clone())).await;
        let (reads, binding) = fixture.bind(fixture.spec(range(0, 20))).await;
        let page = reads
            .next(&fixture.identity, &binding.cursor, &ReadCancellation::new())
            .await
            .unwrap();
        assert_eq!(
            page.ids,
            members.into_iter().filter(|id| id[0] < 20).collect::<Vec<_>>()
        );
        if fixture.record.splits[0].pages.is_empty() {
            assert_eq!(fixture.store.page_reads(), 0);
        }
        assert_eq!(fixture.store.writes.load(Ordering::Relaxed), 0);
    }
    let fixture = TestWindowFixture::new(ids(45), range(0, 20)).await;
    let (reads, binding) = fixture.bind(fixture.spec(range(30, 45))).await;
    assert!(reads
        .next(&fixture.identity, &binding.cursor, &ReadCancellation::new())
        .await
        .unwrap()
        .ids
        .is_empty());
}

#[tokio::test]
async fn page_scope_digest_canonical_shape_and_presence_fail_closed() {
    for failure in 0..7 {
        let fixture = TestWindowFixture::new(ids(45), range(0, 20)).await;
        let key = fixture.page_key(0);
        let value = fixture.store.inner.get(&key).await.unwrap().unwrap();
        let mut page = MembershipPage::decode(&value.bytes).unwrap();
        match failure {
            0 => page.members[0].push(1),
            1 => page.snapshot = SnapshotId::random(),
            2 => page.revision = SplitRevisionId::random(),
            3 => page.name = SplitName::new("other").unwrap(),
            4 => page.page = 1,
            5 => page.members.swap(0, 1),
            _ => {}
        }
        if failure == 6 {
            fixture.store.inner.delete(&key).await.unwrap();
        } else {
            fixture.replace(&key, &serde_json::to_vec(&page).unwrap()).await;
        }
        let (reads, binding) = fixture.bind(fixture.spec(range(0, 20))).await;
        assert!(reads
            .next(&fixture.identity, &binding.cursor, &ReadCancellation::new())
            .await
            .is_err());
        assert_eq!(fixture.store.page_reads(), 1, "failure {failure}");
        assert_eq!(fixture.store.writes.load(Ordering::Relaxed), 0);
    }
}

#[tokio::test]
async fn aggregate_one_mib_budget_still_rejects_two_individually_legal_large_pages() {
    let ids = (0..100u8)
        .map(|index| {
            let mut id = vec![200; 7000];
            id[0] = index;
            id
        })
        .collect();
    let fixture = TestWindowFixture::new(ids, all()).await;
    assert_eq!(fixture.record.splits[0].pages.len(), 5);
    let (reads, binding) = fixture.bind(fixture.spec(all())).await;
    assert!(reads
        .next(&fixture.identity, &binding.cursor, &ReadCancellation::new())
        .await
        .is_err());
    let caps = fixture.store.caps.lock().unwrap();
    assert_eq!(caps.len(), 2);
    assert_eq!(caps[0], 1024 * 1024);
    assert!(caps[1] < 512 * 1024);
    assert_eq!(fixture.store.writes.load(Ordering::Relaxed), 0);
}

#[tokio::test]
async fn cancellation_before_and_during_a_page_read_never_writes_metadata() {
    let fixture = TestWindowFixture::new(ids(45), range(0, 20)).await;
    let (reads, binding) = fixture.bind(fixture.spec(range(0, 20))).await;
    let cancel = ReadCancellation::new();
    cancel.cancel();
    assert!(reads
        .next(&fixture.identity, &binding.cursor, &cancel)
        .await
        .is_err());
    assert_eq!(fixture.store.page_reads(), 0);
    fixture.store.cancel_page.store(true, Ordering::Relaxed);
    assert!(reads
        .next(&fixture.identity, &binding.cursor, &ReadCancellation::new())
        .await
        .is_err());
    assert_eq!(fixture.store.page_reads(), 1);
    assert_eq!(fixture.store.writes.load(Ordering::Relaxed), 0);
}

#[tokio::test(start_paused = true)]
async fn the_original_two_second_deadline_still_bounds_a_stalled_page() {
    let fixture = TestWindowFixture::new(ids(45), range(0, 20)).await;
    let (reads, binding) = fixture.bind(fixture.spec(range(0, 20))).await;
    fixture.store.stall_page.store(true, Ordering::Relaxed);
    let started = tokio::time::Instant::now();
    assert!(reads
        .next(&fixture.identity, &binding.cursor, &ReadCancellation::new())
        .await
        .is_err());
    assert_eq!(
        tokio::time::Instant::now() - started,
        std::time::Duration::from_secs(2)
    );
    assert_eq!(fixture.store.page_reads(), 1);
    assert_eq!(fixture.store.writes.load(Ordering::Relaxed), 0);
}

#[tokio::test]
async fn previously_bound_reads_recheck_split_and_snapshot_deletion() {
    for owner in [false, true] {
        let fixture = TestWindowFixture::new(ids(45), range(0, 20)).await;
        let (reads, binding) = fixture.bind(fixture.spec(range(0, 20))).await;
        if owner {
            fixture
                .authority
                .begin_reclaim(&fixture.identity, fixture.snapshot)
                .await
                .unwrap();
        } else {
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
        }
        fixture.store.reset();
        assert!(reads
            .next(&fixture.identity, &binding.cursor, &ReadCancellation::new())
            .await
            .is_err());
        assert_eq!(fixture.store.page_reads(), 0);
        assert_eq!(fixture.store.writes.load(Ordering::Relaxed), 0);
    }
}

#[tokio::test]
async fn original_shuffle_worker_partition_and_checkpoint_replay_stay_stateless() {
    let fixture = TestWindowFixture::new(ids(45), all()).await;
    let mut combined = std::collections::BTreeSet::new();
    for worker in 0..2 {
        let mut spec = fixture.spec(all());
        spec.batch_size = 7;
        spec.partition.workers = 2;
        spec.partition.worker = worker;
        spec.shuffle = Some(ReadShuffle {
            seed: 42,
            group_size: 20,
        });
        let (reads, binding) = fixture.bind(spec).await;
        let first = reads
            .next(&fixture.identity, &binding.cursor, &ReadCancellation::new())
            .await
            .unwrap();
        let replay = DatasetReads::new(fixture.authority.clone())
            .next(&fixture.identity, &binding.cursor, &ReadCancellation::new())
            .await
            .unwrap();
        assert_eq!(first.ids, replay.ids);
        assert_eq!(first.next, replay.next);
        let mut cursor = binding.cursor;
        loop {
            let page = reads
                .next(&fixture.identity, &cursor, &ReadCancellation::new())
                .await
                .unwrap();
            for id in page.ids {
                assert!(combined.insert(id));
            }
            if let Some(next) = page.next {
                cursor = next;
            } else {
                break;
            }
        }
        assert_eq!(fixture.store.writes.load(Ordering::Relaxed), 0);
    }
    assert_eq!(combined.into_iter().collect::<Vec<_>>(), ids(45));
}

#[tokio::test]
async fn legacy_snapshot_format_uses_the_same_window_and_never_backfills_state() {
    let fixture = TestWindowFixture::new(ids(45), range(0, 20)).await;
    let head = fixture.authority.inspect_head(&fixture.identity).await.unwrap();
    fixture
        .authority
        .update_snapshot_head(
            &fixture.identity,
            &SnapshotHeadUpdate {
                snapshot: fixture.snapshot,
                expected_head: head.generation,
            },
        )
        .await
        .unwrap();
    let record = fixture
        .authority
        .get_snapshot(&fixture.identity, fixture.snapshot)
        .await
        .unwrap();
    let scope = [&fixture.identity.key().unwrap()[..], fixture.snapshot.as_bytes()].concat();
    fixture
        .replace(
            &[b"DS\0S\0".as_slice(), &scope].concat(),
            &bincode::serialize(&(1u16, record)).unwrap(),
        )
        .await;
    fixture
        .store
        .inner
        .delete(&[b"DS\0CB\0".as_slice(), &scope].concat())
        .await
        .unwrap();
    let (reads, binding) = fixture.bind(fixture.spec(range(0, 20))).await;
    let page = reads
        .next(&fixture.identity, &binding.cursor, &ReadCancellation::new())
        .await
        .unwrap();
    assert_eq!(page.ids, ids(20));
    assert_eq!(fixture.store.page_reads(), 1);
    assert_eq!(fixture.store.writes.load(Ordering::Relaxed), 0);
}
