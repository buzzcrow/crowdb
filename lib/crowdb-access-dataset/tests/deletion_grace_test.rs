// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[path = "common/access_store.rs"]
mod access_store;
#[path = "common/deletion.rs"]
mod deletion;
#[path = "common/publication.rs"]
mod publication;
#[path = "common/snapshot_cleanup_stall.rs"]
mod stall;
#[path = "common/upload.rs"]
#[allow(dead_code)]
mod upload;
use crowdb_access_dataset::*;
use std::sync::{atomic::Ordering, Arc};

#[tokio::test(start_paused = true)]
async fn a_slow_snapshot_yields_the_sweep_and_keeps_its_durable_cleanup_obligation() {
    let fixture = deletion::TestDeletionFixture::new().await;
    fixture
        .authority
        .begin_reclaim(&fixture.identity, fixture.snapshot)
        .await
        .unwrap();
    fixture.clock.store(2200, Ordering::Release);
    let store = Arc::new(stall::TestSnapshotCleanupStall {
        inner: fixture.store.clone(),
        stall: std::sync::atomic::AtomicBool::new(true),
        permanent: false,
        target: None,
    });
    let authority = DatasetAuthority::new(store.clone()).with_reclaim_clock_for_tests(fixture.clock.clone());
    let uploads = DatasetUploads::new(store, Arc::new(upload::TestPayload::default()));
    let mut after = SnapshotReclaimSweep::default();
    assert!(matches!(
        authority
            .reclaim_due_page(&uploads, &mut after, &ReadCancellation::new())
            .await,
        Err(SnapshotReclaimError::Limit)
    ));
    assert!(
        after.pending_snapshots() != 0,
        "a timed-out row must yield to the next deletion mark"
    );
    // Losing the local cursor does not lose the persistent retirement fact.
    after = SnapshotReclaimSweep::default();
    for _ in 0..32 {
        authority
            .reclaim_due_page(&uploads, &mut after, &ReadCancellation::new())
            .await
            .unwrap();
        if authority
            .inspect_snapshot_lifecycle(&fixture.identity, fixture.snapshot, &ReadCancellation::new())
            .await
            .unwrap()
            .completed
        {
            return;
        }
    }
    panic!("snapshot cleanup did not recover after the stalled request was canceled");
}

#[tokio::test(start_paused = true)]
async fn a_permanently_slow_snapshot_does_not_block_other_pending_cleanup() {
    let fixture = deletion::TestDeletionFixture::new().await;
    let other = fixture
        .authority
        .prepare_snapshot_candidate(
            &fixture.identity,
            None,
            publication::publication_manifest(3),
            OperationId::random(),
        )
        .await
        .unwrap();
    for snapshot in [fixture.snapshot, other] {
        fixture
            .authority
            .mark_snapshot_reclaim(&fixture.identity, snapshot)
            .await
            .unwrap();
    }
    fixture.clock.store(2200, Ordering::Release);
    let mut target = b"DS\0S\0".to_vec();
    target.extend(fixture.identity.key().unwrap());
    target.extend(fixture.snapshot.as_bytes());
    let store = Arc::new(stall::TestSnapshotCleanupStall {
        inner: fixture.store.clone(),
        stall: std::sync::atomic::AtomicBool::new(true),
        permanent: true,
        target: Some(target),
    });
    let authority = DatasetAuthority::new(store.clone()).with_reclaim_clock_for_tests(fixture.clock);
    let uploads = DatasetUploads::new(store, Arc::new(upload::TestPayload::default()));
    let mut cursor = SnapshotReclaimSweep::default();
    let mut timed_out = false;
    for _ in 0..64 {
        match authority
            .reclaim_due_page(&uploads, &mut cursor, &ReadCancellation::new())
            .await
        {
            Ok(()) => {}
            Err(SnapshotReclaimError::Limit) => timed_out = true,
            result => panic!("unexpected cleanup result: {result:?}"),
        }
        if authority
            .inspect_snapshot_lifecycle(&fixture.identity, other, &ReadCancellation::new())
            .await
            .unwrap()
            .completed
        {
            assert!(timed_out);
            return;
        }
    }
    panic!("slow snapshot starved an unrelated cleanup");
}

#[tokio::test]
async fn deletion_rejects_new_reads_and_keeps_the_grace_deadline_across_restarts() {
    let deletion::TestDeletionFixture {
        store,
        clock,
        authority,
        identity,
        snapshot,
        next,
        held,
    } = deletion::TestDeletionFixture::new().await;
    authority.begin_reclaim(&identity, snapshot).await.unwrap();
    assert_eq!(
        authority
            .snapshot_reclaim_after(&identity, snapshot)
            .await
            .unwrap(),
        2200
    );
    assert!(matches!(
        authority.check_snapshot_readable(&identity, snapshot).await,
        Err(AuthorityError::SnapshotNotFound)
    ));
    assert!(!held.samples.is_empty());
    clock.store(2199, Ordering::Release);
    let restarted = DatasetAuthority::new(store.clone()).with_reclaim_clock_for_tests(clock.clone());
    restarted.begin_reclaim(&identity, snapshot).await.unwrap();
    assert_eq!(
        restarted
            .snapshot_reclaim_after(&identity, snapshot)
            .await
            .unwrap(),
        2200
    );
    let writes = store.writes.load(Ordering::Relaxed);
    let uploads = DatasetUploads::new(store.clone(), Arc::new(upload::TestPayload::default()));
    restarted
        .reclaim_due_page(
            &uploads,
            &mut SnapshotReclaimSweep::default(),
            &ReadCancellation::new(),
        )
        .await
        .unwrap();
    let mut metadata_cursor = MetadataCleanupProgress::new(snapshot);
    assert!(matches!(
        restarted
            .reclaim_snapshot_metadata_step_for_tests(
                &identity,
                snapshot,
                &mut metadata_cursor,
                &ReadCancellation::new()
            )
            .await,
        Err(AuthorityError::ReclaimPending { reclaim_after: 2200 })
    ));
    assert_eq!(store.writes.load(Ordering::Relaxed), writes);
    clock.store(2200, Ordering::Release);
    loop {
        if restarted
            .reclaim_snapshot_metadata_step_for_tests(
                &identity,
                snapshot,
                &mut metadata_cursor,
                &ReadCancellation::new(),
            )
            .await
            .unwrap()
            .complete
        {
            break;
        }
    }
    assert_eq!(
        restarted
            .reclaim_progress(&identity, snapshot)
            .await
            .unwrap()
            .state,
        ReclaimState::Completed
    );
    assert!(!held.samples.is_empty());
    assert!(matches!(
        restarted.get_snapshot(&identity, snapshot).await,
        Err(AuthorityError::SnapshotNotFound)
    ));
    assert!(restarted.get_snapshot(&identity, next).await.is_ok());
    // Metadata completion alone must not make the background worker skip owner/index cleanup.
    finish_worker_cleanup(&restarted, &uploads, &identity, snapshot).await;
    let writes = store.writes.load(Ordering::Relaxed);
    restarted
        .reclaim_due_page(
            &uploads,
            &mut SnapshotReclaimSweep::default(),
            &ReadCancellation::new(),
        )
        .await
        .unwrap();
    assert_eq!(store.writes.load(Ordering::Relaxed), writes);
}

async fn finish_worker_cleanup(
    authority: &DatasetAuthority,
    uploads: &DatasetUploads,
    identity: &DatasetIdentity,
    snapshot: SnapshotId,
) {
    let mut cursor = SnapshotReclaimSweep::default();
    for _ in 0..32 {
        authority
            .reclaim_due_page(uploads, &mut cursor, &ReadCancellation::new())
            .await
            .unwrap();
        if authority
            .inspect_snapshot_lifecycle(identity, snapshot, &ReadCancellation::new())
            .await
            .unwrap()
            .completed
        {
            return;
        }
    }
    panic!("background cleanup did not complete");
}

#[tokio::test]
async fn corrupt_deletion_marks_do_not_starve_other_snapshots() {
    let fixture = deletion::TestDeletionFixture::new().await;
    let authority = &fixture.authority;
    authority
        .begin_reclaim(&fixture.identity, fixture.snapshot)
        .await
        .unwrap();
    fixture.clock.store(2200, Ordering::Release);
    let corrupt_key = b"DS\0L\0".to_vec();
    fixture
        .store
        .compare_exchange(&corrupt_key, None, &[255])
        .await
        .unwrap();
    let uploads = DatasetUploads::new(fixture.store.clone(), Arc::new(upload::TestPayload::default()));
    let cancel = ReadCancellation::new();
    let mut after = SnapshotReclaimSweep::default();
    assert!(authority
        .reclaim_due_page(&uploads, &mut after, &cancel)
        .await
        .is_err());
    assert!(after.has_more());
    for _ in 0..32 {
        // A corrupt record is reported again each cycle without starving later records.
        let _ = authority.reclaim_due_page(&uploads, &mut after, &cancel).await;
        if authority
            .inspect_snapshot_lifecycle(&fixture.identity, fixture.snapshot, &cancel)
            .await
            .unwrap()
            .completed
        {
            return;
        }
    }
    panic!("unrelated corrupt deletion mark prevented cleanup");
}
