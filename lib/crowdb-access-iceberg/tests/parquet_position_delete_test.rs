#[path = "common/file_blocks.rs"]
mod blocks;
#[path = "common/parquet_deletes.rs"]
mod deletes;
#[path = "common/parquet_metadata.rs"]
#[allow(dead_code)]
mod fixture;
#[path = "common/parquet_iceberg_fixture.rs"]
mod official;
#[path = "common/selected_parquet.rs"]
#[allow(dead_code)]
mod selected;

use async_trait::async_trait;
use crowdb_access_iceberg::file::{FileLocation, FileRecord};
use crowdb_access_iceberg::manifest::{
    validate_parquet_position_deletes, FileContentKind, ParquetSelection, PositionDeleteTargets,
    SelectedParquetError,
};
use deletes::{delete_limits, file, longs, strings, TestColumn};
use serde_json::json;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

struct TestTargets {
    rows: Option<u64>,
    calls: AtomicUsize,
}

#[tokio::test]
async fn official_iceberg_java_zstd_v1_and_v2_delete_files_decode() {
    use crowdb_access_iceberg::file::TableLocation;
    use crowdb_access_iceberg::key::{CatalogId, TableId};
    let table = TableLocation {
        catalog: CatalogId::from_bytes(&[1; 16]).unwrap(),
        table: TableId::from_bytes(&[2; 16]).unwrap(),
    };
    for bytes in official::files() {
        let (store, record) = fixture::stored_content(&bytes, table).await;
        let targets = TestTargets {
            rows: Some(100),
            calls: AtomicUsize::new(0),
        };
        let result = validate(store, &record, 100, &targets).await.unwrap();
        assert_eq!(
            (result.rows, result.applicable_rows, result.targets),
            (100, 100, 1)
        );
    }
}

#[tokio::test]
async fn canonical_storage_does_not_make_corrupted_sdk_pages_valid() {
    use crowdb_access_iceberg::file::TableLocation;
    use crowdb_access_iceberg::key::{CatalogId, TableId};
    let table = TableLocation {
        catalog: CatalogId::from_bytes(&[1; 16]).unwrap(),
        table: TableId::from_bytes(&[2; 16]).unwrap(),
    };
    for mut bytes in official::files() {
        let marker = bytes
            .windows(4)
            .position(|bytes| bytes == [0x28, 0xb5, 0x2f, 0xfd])
            .unwrap();
        bytes[marker] ^= 1;
        let (store, record) = fixture::stored_content(&bytes, table).await;
        let targets = TestTargets {
            rows: Some(100),
            calls: AtomicUsize::new(0),
        };
        assert!(validate(store, &record, 100, &targets).await.is_err());
        assert_eq!(targets.calls.load(Ordering::SeqCst), 0);
    }
}
#[async_trait]
impl PositionDeleteTargets for TestTargets {
    async fn rows(&self, _: &FileLocation) -> Result<Option<u64>, SelectedParquetError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(self.rows)
    }
}

async fn validate(
    store: Arc<blocks::TestBlocks>,
    record: &FileRecord,
    rows: i64,
    targets: &TestTargets,
) -> Result<crowdb_access_iceberg::manifest::PositionDeleteSummary, SelectedParquetError> {
    let mut entry = selected::entry(FileContentKind::PositionDeletes);
    entry.entry.record_count = rows;
    entry.file.location = record.location.clone();
    entry.file.length = record.length;
    let context = selected::context(json!([]));
    validate_parquet_position_deletes(
        store,
        record,
        ParquetSelection {
            entry: &entry,
            context: &context,
            table: record.location.table(),
            mapping: None,
        },
        targets,
        delete_limits(),
    )
    .await
}

#[tokio::test]
async fn canonical_pages_check_pairs_across_page_boundaries_and_common_codecs() {
    for codec in [0, 1, 2, 6, 7] {
        for v2 in [false, true] {
            let table = selected::entry(FileContentKind::Data).file.location.table();
            let path = table.file("data/target.parquet").unwrap().to_string();
            let columns = [
                TestColumn {
                    physical: 6,
                    encoding: 0,
                    dictionary: None,
                    pages: vec![
                        (1, strings(std::slice::from_ref(&path))),
                        (2, strings(&[path.clone(), path])),
                    ],
                },
                TestColumn {
                    physical: 2,
                    encoding: 0,
                    dictionary: None,
                    pages: vec![(2, longs(&[0, 1])), (1, longs(&[2]))],
                },
            ];
            let (store, record) = file(table, columns, v2, codec).await;
            let targets = TestTargets {
                rows: Some(3),
                calls: AtomicUsize::new(0),
            };
            let result = validate(store, &record, 3, &targets).await.unwrap();
            assert_eq!((result.rows, result.applicable_rows, result.targets), (3, 3, 1));
            assert_eq!(targets.calls.load(Ordering::SeqCst), 1);
        }
    }
}

#[tokio::test]
async fn dictionary_pages_preserve_rle_indices_and_allow_duplicate_deletes() {
    let table = selected::entry(FileContentKind::Data).file.location.table();
    let path = table.file("data/target.parquet").unwrap().to_string();
    let columns = [
        TestColumn {
            physical: 6,
            encoding: 8,
            dictionary: Some((1, strings(&[path]))),
            pages: vec![(3, vec![0, 6])],
        },
        TestColumn {
            physical: 2,
            encoding: 8,
            dictionary: Some((1, longs(&[1]))),
            pages: vec![(3, vec![0, 6])],
        },
    ];
    let (store, record) = file(table, columns, false, 6).await;
    let targets = TestTargets {
        rows: Some(2),
        calls: AtomicUsize::new(0),
    };
    assert_eq!(validate(store, &record, 3, &targets).await.unwrap().rows, 3);
}

#[tokio::test]
async fn negative_unsorted_foreign_and_out_of_range_deletes_fail() {
    for invalid in 0..5 {
        let table = selected::entry(FileContentKind::Data).file.location.table();
        let mut path = table.file("data/target.parquet").unwrap().to_string();
        let positions = match invalid {
            0 => vec![-1, 0],
            1 => vec![1, 0],
            2 => vec![0, 2],
            _ => vec![0, 1],
        };
        if invalid == 3 {
            path = "s3://foreign/key".into();
        }
        if invalid == 4 {
            path = selected::entry(FileContentKind::Data).file.location.to_string();
        }
        let columns = [
            TestColumn {
                physical: 6,
                encoding: 0,
                dictionary: None,
                pages: vec![(2, strings(&[path.clone(), path]))],
            },
            TestColumn {
                physical: 2,
                encoding: 0,
                dictionary: None,
                pages: vec![(2, longs(&positions))],
            },
        ];
        let (store, record) = file(table, columns, false, 0).await;
        let targets = TestTargets {
            rows: Some(2),
            calls: AtomicUsize::new(0),
        };
        assert!(validate(store, &record, 2, &targets).await.is_err());
    }
}

#[tokio::test]
async fn old_delete_files_may_reference_unselected_data_files() {
    let table = selected::entry(FileContentKind::Data).file.location.table();
    let path = table.file("data/old.parquet").unwrap().to_string();
    let columns = [
        TestColumn {
            physical: 6,
            encoding: 0,
            dictionary: None,
            pages: vec![(1, strings(&[path]))],
        },
        TestColumn {
            physical: 2,
            encoding: 0,
            dictionary: None,
            pages: vec![(1, longs(&[999]))],
        },
    ];
    let (store, record) = file(table, columns, false, 0).await;
    let targets = TestTargets {
        rows: None,
        calls: AtomicUsize::new(0),
    };
    let result = validate(store, &record, 1, &targets).await.unwrap();
    assert_eq!((result.rows, result.applicable_rows), (1, 0));
}
