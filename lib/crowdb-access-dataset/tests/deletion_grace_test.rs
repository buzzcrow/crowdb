// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[path = "common/access_store.rs"]
mod access_store;
#[path = "common/deletion.rs"]
mod deletion;
#[path = "common/publication.rs"]
mod publication;
#[path = "common/upload.rs"]
#[allow(dead_code)]
mod upload;
use crowdb_access_dataset::*;
use std::sync::{atomic::Ordering, Arc};

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
        1300
    );
    assert!(matches!(
        authority.check_snapshot_readable(&identity, snapshot).await,
        Err(AuthorityError::SnapshotNotFound)
    ));
    assert!(!held.samples.is_empty());
    clock.store(1299, Ordering::Release);
    let restarted = DatasetAuthority::new(store.clone()).with_reclaim_clock_for_tests(clock.clone());
    restarted.begin_reclaim(&identity, snapshot).await.unwrap();
    assert_eq!(
        restarted
            .snapshot_reclaim_after(&identity, snapshot)
            .await
            .unwrap(),
        1300
    );
    let writes = store.writes.load(Ordering::Relaxed);
    let uploads = DatasetUploads::new(store.clone(), Arc::new(upload::TestPayload::default()));
    restarted
        .reclaim_due_page(&uploads, &mut None, &ReadCancellation::new())
        .await
        .unwrap();
    assert!(matches!(
        restarted
            .reclaim_snapshot_metadata_step(&identity, snapshot, &ReadCancellation::new())
            .await,
        Err(AuthorityError::ReclaimPending { reclaim_after: 1300 })
    ));
    assert_eq!(store.writes.load(Ordering::Relaxed), writes);
    clock.store(1300, Ordering::Release);
    loop {
        if restarted
            .reclaim_snapshot_metadata_step(&identity, snapshot, &ReadCancellation::new())
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
    for _ in 0..32 {
        restarted
            .reclaim_due_page(&uploads, &mut None, &ReadCancellation::new())
            .await
            .unwrap();
        if restarted
            .inspect_snapshot_lifecycle(&identity, snapshot, &ReadCancellation::new())
            .await
            .unwrap()
            .completed
        {
            break;
        }
    }
    assert!(
        restarted
            .inspect_snapshot_lifecycle(&identity, snapshot, &ReadCancellation::new())
            .await
            .unwrap()
            .completed
    );
    let writes = store.writes.load(Ordering::Relaxed);
    restarted
        .reclaim_due_page(&uploads, &mut None, &ReadCancellation::new())
        .await
        .unwrap();
    assert_eq!(store.writes.load(Ordering::Relaxed), writes);
}

#[tokio::test]
async fn corrupt_deletion_marks_do_not_starve_other_snapshots() {
    let fixture = deletion::TestDeletionFixture::new().await;
    let authority = &fixture.authority;
    authority
        .begin_reclaim(&fixture.identity, fixture.snapshot)
        .await
        .unwrap();
    fixture.clock.store(1300, Ordering::Release);
    let corrupt_key = b"DS\0L\0".to_vec();
    fixture
        .store
        .compare_exchange(&corrupt_key, None, &[255])
        .await
        .unwrap();
    let uploads = DatasetUploads::new(fixture.store.clone(), Arc::new(upload::TestPayload::default()));
    let cancel = ReadCancellation::new();
    let mut after = None;
    assert!(authority
        .reclaim_due_page(&uploads, &mut after, &cancel)
        .await
        .is_err());
    assert_eq!(after, Some(corrupt_key));
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
