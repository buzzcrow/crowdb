#[path = "common/file_blocks.rs"]
mod blocks;
#[path = "common/manifest_dv.rs"]
mod fixture;
#[path = "common/deletion_vector.rs"]
mod fixtures;
use crowdb_access_iceberg::file::{ContentFormat, FileKind};
use crowdb_access_iceberg::key::FileId;
use crowdb_access_iceberg::manifest::{EntryStatus, SnapshotDvError, SnapshotDvValidator};
use fixture::limits;
use std::sync::{atomic::Ordering, Arc};

#[tokio::test]
async fn snapshot_vectors_bind_multiple_puffin_blobs_and_exact_data_row_limits() {
    let store = Arc::new(blocks::TestBlocks::default());
    for (blob, count, rows, maximum) in [
        (fixtures::blob(&[(0, fixtures::array(0, &[0, 2]))]), 2, 3, Some(2)),
        (
            fixtures::blob(&[(0, fixtures::runs(0, 3, &[(0, 2)]))]),
            3,
            3,
            Some(2),
        ),
        (
            fixtures::blob(&[(0, fixtures::bitset(0))]),
            4097,
            4097,
            Some(4096),
        ),
        (fixtures::blob(&[]), 0, 0, None),
    ] {
        let pair = fixture::pair(store.clone(), &blob, count, rows).await;
        let mut checker = SnapshotDvValidator::new(store.clone(), pair.scope, 1, limits()).unwrap();
        assert_eq!(
            checker
                .check(pair.scope, pair.vector(), pair.data())
                .await
                .unwrap()
                .maximum_position,
            maximum
        );
        let summary = checker.finish().unwrap();
        assert_eq!(summary.scope, pair.scope);
        assert_eq!(summary.vectors, 1);
        assert_eq!(summary.blob_bytes, blob.len() as u64);
    }
    let blob = fixtures::blob(&[(0, fixtures::array(0, &[1]))]);
    let first = fixture::pair(store.clone(), &blob, 1, 2).await;
    let (record, references) = fixtures::record_with_references(
        store.clone(),
        first.scope.table,
        "multi.puffin",
        &[("a.parquet", &blob, 1), ("b.parquet", &blob, 1)],
    )
    .await;
    let mut checker = SnapshotDvValidator::new(store.clone(), first.scope, 2, limits()).unwrap();
    for reference in references {
        let pair = fixture::from_reference(store.clone(), record.clone(), reference, 2).await;
        checker
            .check(first.scope, pair.vector(), pair.data())
            .await
            .unwrap();
    }
    assert_eq!(checker.finish().unwrap().vectors, 2);
}

#[tokio::test]
async fn vector_binding_and_descriptor_mismatches_poison_without_advancing() {
    let store = Arc::new(blocks::TestBlocks::default());
    let blob = fixtures::blob(&[(0, fixtures::array(0, &[1, 2]))]);
    for fault in 0..17 {
        let mut pair = fixture::pair(store.clone(), &blob, 2, 3).await;
        match fault {
            0 => pair.data_entry.entry.status = EntryStatus::Deleted,
            1 => pair.vector_entry.entry.status = EntryStatus::Deleted,
            2 => pair.data.kind = FileKind::PositionDelete,
            3 => pair.vector.kind = FileKind::Statistics,
            4 => pair.vector_entry.file.length += 1,
            5 => pair.data_entry.file.format = ContentFormat::Orc,
            6 => pair.vector_entry.file.referenced_data_file = Some(pair.scope.table.file("other").unwrap()),
            7 => pair.vector_entry.file.deletion_vector.as_mut().unwrap().offset += 1,
            8 => pair.vector_entry.file.deletion_vector.as_mut().unwrap().length -= 1,
            9 => pair.vector_entry.entry.record_count = 1,
            10 => pair.data_entry.entry.record_count = 2,
            11 => pair.data_entry.entry.record_count = 1,
            12 => pair.data_entry.inherited.file_sequence = 10,
            13 => {
                pair.vector_entry.entry.data_sequence = Some(8);
                pair.vector_entry.inherited.data_sequence = 8;
            }
            14 => pair.vector_entry.file.partition = None,
            15 => pair.data_entry.entry.record_count = -1,
            _ => pair.data.location = pair.scope.table.file("wrong-file").unwrap(),
        }
        let mut checker = SnapshotDvValidator::new(store.clone(), pair.scope, 1, limits()).unwrap();
        assert!(
            checker
                .check(pair.scope, pair.vector(), pair.data())
                .await
                .is_err(),
            "fault {fault}"
        );
        assert_eq!(checker.checked(), 0);
        assert!(matches!(
            checker.check(pair.scope, pair.vector(), pair.data()).await,
            Err(SnapshotDvError::Incomplete)
        ));
        assert!(checker.finish().is_err());
    }
}

#[tokio::test]
async fn snapshot_uniqueness_order_and_eof_use_one_previous_reference() {
    let store = Arc::new(blocks::TestBlocks::default());
    let blob = fixtures::blob(&[(0, fixtures::array(0, &[1]))]);
    let first = fixture::pair(store.clone(), &blob, 1, 2).await;
    let scope = first.scope;
    let mut checker = SnapshotDvValidator::new(store.clone(), scope, 2, limits()).unwrap();
    checker.check(scope, first.vector(), first.data()).await.unwrap();
    let reads = store.reads.load(Ordering::SeqCst);
    assert!(matches!(
        checker.check(scope, first.vector(), first.data()).await,
        Err(SnapshotDvError::Order)
    ));
    assert_eq!(store.reads.load(Ordering::SeqCst), reads);
    assert_eq!(checker.checked(), 1);
    assert!(checker.finish().is_err());
    let (record, references) = fixtures::record_with_references(
        store.clone(),
        scope.table,
        "second.puffin",
        &[("a.parquet", &blob, 1)],
    )
    .await;
    let next = fixture::from_reference(store.clone(), record, references[0].clone(), 2).await;
    let mut checker = SnapshotDvValidator::new(store.clone(), scope, 2, limits()).unwrap();
    checker.check(scope, first.vector(), first.data()).await.unwrap();
    assert!(matches!(
        checker.check(scope, next.vector(), next.data()).await,
        Err(SnapshotDvError::Order)
    ));
    let checker = SnapshotDvValidator::new(store.clone(), scope, 1, limits()).unwrap();
    assert!(checker.finish().is_err());
    assert_eq!(
        SnapshotDvValidator::new(store, scope, 0, limits())
            .unwrap()
            .finish()
            .unwrap()
            .vectors,
        0
    );
}

#[tokio::test]
async fn snapshot_scope_and_independent_work_limits_fail_before_reads() {
    let store = Arc::new(blocks::TestBlocks::default());
    let blob = fixtures::blob(&[(0, fixtures::array(0, &[1]))]);
    let pair = fixture::pair(store.clone(), &blob, 1, 2).await;
    for fault in 0..8 {
        let mut scope = pair.scope;
        let mut budget = limits();
        let mut expected = 1;
        match fault {
            0 => scope.context.activation_epoch += 1,
            1 => scope.snapshot_id += 1,
            2 => scope.sequence += 1,
            3 => scope.manifest_list = FileId::random(),
            4 => budget.blob_bytes = blob.len() as u64 - 1,
            5 => budget.vector.blob_bytes = blob.len() as u64 - 1,
            6 => expected = 0,
            _ => scope.table.table = crowdb_access_iceberg::key::TableId::random(),
        }
        let mut checker = SnapshotDvValidator::new(store.clone(), pair.scope, expected, budget).unwrap();
        let reads = store.reads.load(Ordering::SeqCst);
        assert!(checker.check(scope, pair.vector(), pair.data()).await.is_err());
        assert_eq!(store.reads.load(Ordering::SeqCst), reads);
        assert!(checker.finish().is_err());
    }
    assert!(SnapshotDvValidator::new(store.clone(), pair.scope, limits().vectors + 1, limits()).is_err());
    let mut invalid = limits();
    invalid.vector.bitmaps = 0;
    assert!(SnapshotDvValidator::new(store, pair.scope, 0, invalid).is_err());
}

#[tokio::test]
async fn cancellation_and_canonical_crc_corruption_require_fresh_validation() {
    let store = Arc::new(blocks::TestBlocks::default());
    let blob = fixtures::blob(&[(0, fixtures::array(0, &[1]))]);
    let pair = fixture::pair(store.clone(), &blob, 1, 2).await;
    let mut checker = SnapshotDvValidator::new(store.clone(), pair.scope, 1, limits()).unwrap();
    store.pause_reads.store(true, Ordering::SeqCst);
    tokio::select! {
        ()=store.read_entered.notified()=>{},
        result=checker.check(pair.scope,pair.vector(),pair.data())=>panic!("unexpected completion: {result:?}"),
    }
    assert_eq!(checker.checked(), 0);
    assert!(checker.finish().is_err());
    store.pause_reads.store(false, Ordering::SeqCst);
    let mut checker = SnapshotDvValidator::new(store.clone(), pair.scope, 1, limits()).unwrap();
    checker
        .check(pair.scope, pair.vector(), pair.data())
        .await
        .unwrap();
    checker.finish().unwrap();
    let mut corrupt = blob;
    *corrupt.last_mut().unwrap() ^= 1;
    let pair = fixture::pair(store.clone(), &corrupt, 1, 2).await;
    let mut checker = SnapshotDvValidator::new(store, pair.scope, 1, limits()).unwrap();
    assert!(matches!(
        checker.check(pair.scope, pair.vector(), pair.data()).await,
        Err(SnapshotDvError::Vector(_))
    ));
    assert!(checker.finish().is_err());
}

#[tokio::test]
async fn distinct_puffin_files_cannot_duplicate_targets_or_exceed_aggregate_budget() {
    let store = Arc::new(blocks::TestBlocks::default());
    let blob = fixtures::blob(&[(0, fixtures::array(0, &[1]))]);
    let first = fixture::pair(store.clone(), &blob, 1, 2).await;
    for duplicate in [false, true] {
        let target = if duplicate {
            first.data.location.relative_key()
        } else {
            "z.parquet"
        };
        let (record, references) = fixtures::record_with_references(
            store.clone(),
            first.scope.table,
            "another.puffin",
            &[(target, &blob, 1)],
        )
        .await;
        let second = fixture::from_reference(store.clone(), record, references[0].clone(), 2).await;
        let mut budget = limits();
        budget.blob_bytes = blob.len() as u64 * 2 - 1;
        let mut checker = SnapshotDvValidator::new(store.clone(), first.scope, 2, budget).unwrap();
        checker
            .check(first.scope, first.vector(), first.data())
            .await
            .unwrap();
        let reads = store.reads.load(Ordering::SeqCst);
        let result = checker.check(first.scope, second.vector(), second.data()).await;
        assert!(matches!(
            (duplicate, result),
            (true, Err(SnapshotDvError::Order)) | (false, Err(SnapshotDvError::Bounds))
        ));
        assert_eq!(store.reads.load(Ordering::SeqCst), reads);
        assert_eq!(checker.checked(), 1);
        assert!(checker.finish().is_err());
    }
    let blob = fixtures::blob(&[(0x7fff_ffff, fixtures::array(65535, &[65535]))]);
    let pair = fixture::pair(store.clone(), &blob, 1, i64::MAX).await;
    let mut checker = SnapshotDvValidator::new(store, pair.scope, 1, limits()).unwrap();
    assert!(matches!(
        checker.check(pair.scope, pair.vector(), pair.data()).await,
        Err(SnapshotDvError::Position)
    ));
}
