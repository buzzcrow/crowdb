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
#[path = "common/retirement_pressure.rs"]
mod pressure;
#[path = "common/publication.rs"]
mod publication;
#[path = "common/split_store.rs"]
#[allow(dead_code)]
mod split_store;
#[path = "common/upload.rs"]
#[allow(dead_code)]
mod upload;

use crowdb_access_dataset::{composition::*, split::*, *};
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};

#[tokio::test(start_paused = true)]
async fn thirty_two_failed_compositions_keep_discovery_and_successful_edit_progress_moving() {
    let test = fixture::TestCleanup::new().await;
    let operations = pressure::compositions(&test).await;
    let observed = pressure::edits(&test, operations[38]).await;
    pressure::block(&test, b"DS\0CD\0", &operations[..32]);
    pressure::observe(&test, observed);
    let mut cursor = CompositionReclaimSweep::default();
    for turn in 0..2000 {
        if turn == 11 {
            cursor = CompositionReclaimSweep::default();
        }
        match test
            .authority
            .reclaim_compositions_page(&test.uploads, &mut cursor, &ReadCancellation::new())
            .await
        {
            Ok(()) | Err(CompositionError::Limit) => {}
            result => panic!("unexpected cleanup result: {result:?}"),
        }
        assert!(cursor.pending_drafts() <= 32);
        if pressure::remaining(&test, b"DS\0CEG\0", &operations[32..]).await == 0 {
            break;
        }
    }
    assert_eq!(
        pressure::remaining(&test, b"DS\0CEG\0", &operations[32..]).await,
        0
    );
    assert_eq!(
        test.store.observations.load(Ordering::Relaxed),
        1,
        "successful edit cursor was discarded under failure pressure"
    );
    let cancel = ReadCancellation::new();
    cancel.cancel();
    assert!(test
        .authority
        .reclaim_compositions_page(&test.uploads, &mut cursor, &cancel)
        .await
        .is_err());
    test.store.blocked.store(Arc::new(Vec::new()));
    for _ in 0..3000 {
        test.authority
            .reclaim_compositions_page(&test.uploads, &mut cursor, &ReadCancellation::new())
            .await
            .unwrap();
        if pressure::remaining(&test, b"DS\0CEG\0", &operations).await == 0 {
            break;
        }
    }
    assert_eq!(
        pressure::remaining(&test, b"DS\0CEG\0", &operations).await,
        0,
        "discarded failed hints were never rediscovered"
    );
}

#[tokio::test(start_paused = true)]
async fn thirty_two_failed_split_drafts_keep_discovery_and_successful_member_progress_moving() {
    let test = fixture::TestCleanup::new().await;
    let operations = pressure::splits(&test).await;
    let observed = pressure::members(&test, operations[38]).await;
    pressure::block(&test, b"DS\0SD\0", &operations[..32]);
    pressure::observe(&test, observed);
    let mut cursor = SplitReclaimSweep::default();
    for turn in 0..1000 {
        if turn == 11 {
            cursor = SplitReclaimSweep::default();
        }
        match test
            .authority
            .reclaim_splits_page(&mut cursor, &ReadCancellation::new())
            .await
        {
            Ok(()) | Err(SplitLifecycleError::Limit) => {}
            result => panic!("unexpected cleanup result: {result:?}"),
        }
        assert!(cursor.pending_drafts() <= 32);
        if pressure::remaining(&test, b"DS\0SCG\0", &operations[32..]).await == 0 {
            break;
        }
    }
    assert_eq!(
        pressure::remaining(&test, b"DS\0SCG\0", &operations[32..]).await,
        0
    );
    assert_eq!(
        test.store.observations.load(Ordering::Relaxed),
        1,
        "successful member cursor was discarded under failure pressure"
    );
    let cancel = ReadCancellation::new();
    cancel.cancel();
    assert!(test
        .authority
        .reclaim_splits_page(&mut cursor, &cancel)
        .await
        .is_err());
    test.store.blocked.store(Arc::new(Vec::new()));
    for _ in 0..1000 {
        test.authority
            .reclaim_splits_page(&mut cursor, &ReadCancellation::new())
            .await
            .unwrap();
        if pressure::remaining(&test, b"DS\0SCG\0", &operations).await == 0 {
            break;
        }
    }
    assert_eq!(
        pressure::remaining(&test, b"DS\0SCG\0", &operations).await,
        0,
        "discarded failed hints were never rediscovered"
    );
}

async fn unfinished_snapshots(
    test: &fixture::TestCleanup,
    authority: &DatasetAuthority,
    snapshots: &[SnapshotId],
) -> usize {
    let mut remaining = 0;
    for snapshot in snapshots {
        remaining += usize::from(
            !authority
                .inspect_snapshot_lifecycle(&test.identity, *snapshot, &ReadCancellation::new())
                .await
                .unwrap()
                .completed,
        );
    }
    remaining
}

#[tokio::test(start_paused = true)]
async fn thirty_two_failed_snapshots_keep_discovery_and_successful_index_progress_moving() {
    let test = fixture::TestCleanup::new().await;
    let clock = Arc::new(AtomicU64::new(1000));
    let authority = DatasetAuthority::new(test.store.clone()).with_reclaim_clock_for_tests(clock.clone());
    let snapshots = pressure::snapshots(&test, &authority).await;
    clock.store(2200, Ordering::Release);
    let operations: Vec<_> = snapshots
        .iter()
        .map(|snapshot| OperationId::from_bytes(snapshot.as_bytes()).unwrap())
        .collect();
    pressure::block(&test, b"DS\0S\0", &operations[..32]);
    let mut observed = legacy::key(b"DS\0I\0", &test.identity, operations[32]);
    observed.push(b'S');
    observed.extend(0u32.to_be_bytes());
    pressure::observe(&test, observed);
    let mut cursor = SnapshotReclaimSweep::default();
    for turn in 0..1500 {
        if turn == 11 {
            cursor = SnapshotReclaimSweep::default();
        }
        match authority
            .reclaim_due_page(&test.uploads, &mut cursor, &ReadCancellation::new())
            .await
        {
            Ok(())
            | Err(SnapshotReclaimError::Limit | SnapshotReclaimError::Upload(UploadReclaimError::Limit)) => {}
            result => panic!("unexpected cleanup result: {result:?}"),
        }
        assert!(cursor.pending_snapshots() <= 32);
        if unfinished_snapshots(&test, &authority, &snapshots[32..]).await == 0 {
            break;
        }
    }
    assert_eq!(unfinished_snapshots(&test, &authority, &snapshots[32..]).await, 0);
    assert_eq!(
        test.store.observations.load(Ordering::Relaxed),
        1,
        "successful index cursor was discarded under failure pressure"
    );
    let cancel = ReadCancellation::new();
    cancel.cancel();
    assert!(authority
        .reclaim_due_page(&test.uploads, &mut cursor, &cancel)
        .await
        .is_err());
    test.store.blocked.store(Arc::new(Vec::new()));
    for turn in 0..3000 {
        authority
            .reclaim_due_page(&test.uploads, &mut cursor, &ReadCancellation::new())
            .await
            .unwrap();
        if turn % 8 == 0 && unfinished_snapshots(&test, &authority, &snapshots).await == 0 {
            break;
        }
    }
    assert_eq!(
        unfinished_snapshots(&test, &authority, &snapshots).await,
        0,
        "discarded failed hints were never rediscovered"
    );
}
