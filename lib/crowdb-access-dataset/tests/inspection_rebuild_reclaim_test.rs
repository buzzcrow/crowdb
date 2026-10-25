// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#[path = "common/dataset_store.rs"]
#[allow(dead_code)]
mod dataset_store;
#[path = "common/legacy_inspection.rs"]
mod legacy;
#[path = "common/upload.rs"]
#[allow(dead_code)]
mod upload;
use crowdb_access_dataset::{inspection::InspectionPreparationLimits, *};
use std::{
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};

async fn use_single_row_source(
    store: &legacy::TestLegacyStore,
    identity: &DatasetIdentity,
    snapshot: SnapshotId,
) {
    let key = legacy::snapshot_key(identity, snapshot);
    let old = store.inner.get(&key).await.unwrap().unwrap();
    let manifest = ManifestRecord {
        version: 1,
        snapshot,
        schema: SchemaRecord {
            version: 1,
            fields: vec![],
        },
        samples: vec![SampleRecord {
            sample_id: vec![0, 255],
            fields: vec![],
        }],
    };
    let record = SnapshotRecord {
        publication: SnapshotPublication {
            snapshot,
            parent: None,
            operation: OperationId::random(),
            manifest: bincode::serialize(&manifest).unwrap(),
            state: PublicationState::Published,
        },
    };
    store
        .inner
        .compare_exchange(
            &key,
            Some(&old.bytes),
            &bincode::serialize(&legacy::TestVersioned {
                version: 1,
                payload: record,
            })
            .unwrap(),
        )
        .await
        .unwrap();
}
async fn retire(
    authority: &DatasetAuthority,
    store: Arc<legacy::TestLegacyStore>,
    identity: &DatasetIdentity,
    snapshot: SnapshotId,
    clock: &AtomicU64,
) {
    authority.begin_reclaim(identity, snapshot).await.unwrap();
    clock.store(2200, Ordering::Release);
    let uploads = DatasetUploads::new(store, Arc::new(upload::TestPayload::default()));
    let mut cursor = SnapshotReclaimCursor::new(identity.clone(), snapshot);
    for _ in 0..64 {
        if authority
            .advance_snapshot_reclaim(&uploads, &mut cursor, &ReadCancellation::new())
            .await
            .unwrap()
            .complete
        {
            return;
        }
    }
    panic!("full Snapshot cleanup did not finish");
}
async fn delayed_writer(kind: u8) {
    let (store, _, identity, snapshot, _) = legacy::fixture(false).await;
    if kind == b'M' {
        use_single_row_source(&store, &identity, snapshot).await;
    }
    let clock = Arc::new(AtomicU64::new(1000));
    let authority =
        Arc::new(DatasetAuthority::new(store.clone()).with_reclaim_clock_for_tests(clock.clone()));
    store.allow_legacy_get.store(true, Ordering::Relaxed);
    store.pause_kind.store(kind, Ordering::Relaxed);
    let producer = tokio::spawn({
        let authority = authority.clone();
        let identity = identity.clone();
        async move {
            authority
                .prepare_inspection(
                    &identity,
                    snapshot,
                    InspectionPreparationLimits::default(),
                    &ReadCancellation::new(),
                )
                .await
        }
    });
    tokio::time::timeout(Duration::from_secs(1), store.observed.notified())
        .await
        .unwrap();
    retire(&authority, store.clone(), &identity, snapshot, &clock).await;
    assert!(
        !producer.is_finished(),
        "the delayed create must still be waiting after complete GC"
    );
    let prefix = legacy::index_key(&identity, snapshot, b'M');
    let mut prefix = prefix[..prefix.len() - 1].to_vec();
    let rows = store
        .inner
        .scan_page(
            &MetadataScan {
                prefix: prefix.clone(),
                after: None,
                budget: MetadataScanBudget {
                    items: 100,
                    bytes: 1024 * 1024,
                    timeout: Duration::from_secs(1),
                },
            },
            &ReadCancellation::new(),
        )
        .await
        .unwrap();
    assert_eq!(rows.entries.len(), if kind == b'M' { 2 } else { 11 });
    for (key, row) in &rows.entries {
        if key == &legacy::index_key(&identity, snapshot, b'H') {
            let summary: serde_json::Value = serde_json::from_slice(&row.bytes).unwrap();
            assert_eq!(summary["state"]["Unsupported"]["reason"], "snapshot is retired");
        } else {
            assert_eq!(row.bytes, b"snapshot-metadata-retired-v1");
        }
    }
    store.resume.notify_one();
    assert!(producer.await.unwrap().is_err());
    assert_eq!(store.released_conflicts.load(Ordering::Relaxed), 1);
    assert_eq!(store.pending.load(Ordering::Relaxed), 0);
    let after = store
        .inner
        .scan_page(
            &MetadataScan {
                prefix: std::mem::take(&mut prefix),
                after: None,
                budget: MetadataScanBudget {
                    items: 100,
                    bytes: 1024 * 1024,
                    timeout: Duration::from_secs(1),
                },
            },
            &ReadCancellation::new(),
        )
        .await
        .unwrap();
    assert_eq!(
        after.entries, rows.entries,
        "late create must not add an unseen index key or replace a fence"
    );
}
#[tokio::test]
async fn full_gc_seals_a_not_yet_created_first_index_row() {
    delayed_writer(b'M').await;
}
#[tokio::test]
async fn full_gc_wins_the_same_readiness_key_against_a_delayed_finalizer() {
    delayed_writer(b'H').await;
}
