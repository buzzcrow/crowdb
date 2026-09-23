#[path = "common/file_blocks.rs"]
mod blocks;
#[path = "common/manifest_entry.rs"]
mod fixture;
#[path = "common/manifest_list.rs"]
#[allow(dead_code)]
mod list_fixture;
#[path = "common/snapshot_manifest.rs"]
mod snapshot;
#[path = "common/manifest_stream.rs"]
#[allow(dead_code)]
mod stream;

use std::sync::atomic::Ordering;

use crowdb_access_iceberg::manifest::{SnapshotManifestError, SnapshotManifestReader};

#[tokio::test]
async fn enumeration_requires_list_and_every_manifest_eof() {
    for count in 0..=2 {
        let (store, source, record, selection) = snapshot::stored(count, false, false).await;
        let bytes: u64 = source.records.iter().map(|record| record.length).sum();
        let mut reader =
            SnapshotManifestReader::open(store, source.clone(), record, selection, snapshot::limits())
                .await
                .unwrap();
        assert!(reader.finish().is_err());
        for index in 0..count * 2 {
            let entry = reader.next_entry().await.unwrap().unwrap();
            assert_eq!(
                entry.inherited.first_row_id,
                Some(100 + i64::try_from(index).unwrap() * 10)
            );
            assert_eq!(
                reader.current_manifest().unwrap().0,
                &source.records[index / 2].location
            );
            assert_eq!(source.calls.load(Ordering::SeqCst), index / 2 + 1);
            assert!(reader.finish().is_err());
        }
        assert!(reader.next_entry().await.unwrap().is_none());
        let summary = reader.finish().unwrap();
        assert_eq!(summary.manifests, count as u64);
        assert_eq!(summary.entries, count as u64 * 2);
        assert_eq!(summary.manifest_bytes, bytes);
        assert!(reader.current_manifest().is_none());
        assert!(reader.next_entry().await.unwrap().is_none());
        assert_eq!(reader.finish().unwrap(), summary);
    }
}

#[tokio::test]
async fn missing_authority_and_bad_manifest_never_skip_to_the_next_reference() {
    for corrupt in [false, true] {
        let (store, source, record, selection) = snapshot::stored(2, corrupt, false).await;
        source.unavailable.store(!corrupt, Ordering::SeqCst);
        let mut reader =
            SnapshotManifestReader::open(store, source.clone(), record, selection, snapshot::limits())
                .await
                .unwrap();
        if corrupt {
            assert!(reader.next_entry().await.unwrap().is_some());
        }
        assert!(reader.next_entry().await.is_err());
        source.unavailable.store(false, Ordering::SeqCst);
        assert!(reader.next_entry().await.is_err());
        assert!(reader.finish().is_err());
        assert_eq!(source.calls.load(Ordering::SeqCst), 1);
    }
}

#[tokio::test]
async fn final_manifest_totals_failure_invalidates_all_prior_yielded_entries() {
    let (store, source, record, selection) = snapshot::stored(2, false, true).await;
    let mut reader = SnapshotManifestReader::open(store, source, record, selection, snapshot::limits())
        .await
        .unwrap();
    for _ in 0..4 {
        assert!(reader.next_entry().await.unwrap().is_some());
    }
    assert!(matches!(
        reader.next_entry().await,
        Err(SnapshotManifestError::Manifest(_))
    ));
    assert!(reader.finish().is_err());
    assert!(reader.next_entry().await.is_err());
}

#[tokio::test]
async fn independent_work_budgets_fail_without_a_completion_summary() {
    for budget in 0..3 {
        let (store, source, record, selection) = snapshot::stored(2, false, false).await;
        let mut limits = snapshot::limits();
        match budget {
            0 => limits.manifests = 1,
            1 => limits.entries = 2,
            _ => limits.manifest_bytes = source.records[0].length,
        }
        let mut reader = SnapshotManifestReader::open(store, source.clone(), record, selection, limits)
            .await
            .unwrap();
        for _ in 0..2 {
            assert!(reader.next_entry().await.unwrap().is_some());
        }
        assert!(matches!(
            reader.next_entry().await,
            Err(SnapshotManifestError::Bounds)
        ));
        assert!(reader.finish().is_err());
        assert!(reader.next_entry().await.is_err());
        assert_eq!(
            source.calls.load(Ordering::SeqCst),
            if budget == 1 { 2 } else { 1 }
        );
    }
}

#[tokio::test]
async fn cancellation_during_resolution_poisons_the_outer_cursor() {
    let (store, source, record, selection) = snapshot::stored(2, false, false).await;
    let mut reader = SnapshotManifestReader::open(
        store.clone(),
        source.clone(),
        record.clone(),
        selection.clone(),
        snapshot::limits(),
    )
    .await
    .unwrap();
    source.pause.store(true, Ordering::SeqCst);
    tokio::select! {
        () = source.entered.notified() => {},
        result = reader.next_entry() => panic!("unexpected completion: {result:?}"),
    }
    source.pause.store(false, Ordering::SeqCst);
    assert!(reader.finish().is_err());
    assert!(reader.next_entry().await.is_err());
    let mut fresh = SnapshotManifestReader::open(store, source, record, selection, snapshot::limits())
        .await
        .unwrap();
    while fresh.next_entry().await.unwrap().is_some() {}
    assert_eq!(fresh.finish().unwrap().entries, 4);
}

#[tokio::test]
async fn zero_work_budgets_fail_before_any_storage_reads() {
    let (store, source, record, selection) = snapshot::stored(0, false, false).await;
    for budget in 0..3 {
        let mut limits = snapshot::limits();
        match budget {
            0 => limits.manifests = 0,
            1 => limits.entries = 0,
            _ => limits.manifest_bytes = 0,
        }
        let before = store.reads.load(Ordering::SeqCst);
        assert!(SnapshotManifestReader::open(
            store.clone(),
            source.clone(),
            record.clone(),
            selection.clone(),
            limits
        )
        .await
        .is_err());
        assert_eq!(store.reads.load(Ordering::SeqCst), before);
    }
}

#[tokio::test]
async fn cancellation_inside_a_manifest_also_poisons_the_snapshot() {
    let (store, source, record, selection) = snapshot::stored(2, false, false).await;
    let mut reader = SnapshotManifestReader::open(
        store.clone(),
        source.clone(),
        record,
        selection,
        snapshot::limits(),
    )
    .await
    .unwrap();
    assert!(reader.next_entry().await.unwrap().is_some());
    store.pause_reads.store(true, Ordering::SeqCst);
    tokio::select! {
        () = store.read_entered.notified() => {},
        result = reader.next_entry() => panic!("unexpected completion: {result:?}"),
    }
    store.pause_reads.store(false, Ordering::SeqCst);
    assert!(reader.finish().is_err());
    assert!(reader.next_entry().await.is_err());
    assert_eq!(source.calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn resolved_authority_must_match_the_canonical_list_declaration() {
    let (store, mut source, record, selection) = snapshot::stored(2, false, false).await;
    std::sync::Arc::get_mut(&mut source).unwrap().records[0].length += 1;
    let mut reader =
        SnapshotManifestReader::open(store, source.clone(), record, selection, snapshot::limits())
            .await
            .unwrap();
    assert!(matches!(
        reader.next_entry().await,
        Err(SnapshotManifestError::Manifest(_))
    ));
    assert!(reader.finish().is_err());
    assert!(reader.next_entry().await.is_err());
    assert_eq!(source.calls.load(Ordering::SeqCst), 1);
}
