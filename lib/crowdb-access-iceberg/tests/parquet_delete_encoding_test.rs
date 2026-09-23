#[path = "common/file_blocks.rs"]
mod blocks;
#[path = "common/parquet_deletes.rs"]
#[allow(dead_code)]
mod deletes;
#[path = "common/parquet_metadata.rs"]
#[allow(dead_code)]
mod fixture;
#[path = "common/selected_parquet.rs"]
#[allow(dead_code)]
mod selected;

use async_trait::async_trait;
use crowdb_access_iceberg::file::{FileLocation, TableLocation};
use crowdb_access_iceberg::key::{CatalogId, TableId};
use crowdb_access_iceberg::manifest::{
    validate_parquet_position_deletes, FileContentKind, ParquetSelection, PositionDeleteLimits,
    PositionDeleteSummary, PositionDeleteTargets, SelectedParquetError,
};
use deletes::{delete_limits, file, longs, strings, TestColumn};
use serde_json::json;

struct TestTargets;
#[async_trait]
impl PositionDeleteTargets for TestTargets {
    async fn rows(&self, _: &FileLocation) -> Result<Option<u64>, SelectedParquetError> {
        Ok(Some(100))
    }
}

fn table() -> TableLocation {
    TableLocation {
        catalog: CatalogId::from_bytes(&[1; 16]).unwrap(),
        table: TableId::from_bytes(&[2; 16]).unwrap(),
    }
}
fn path() -> String {
    table().file("data/target.parquet").unwrap().to_string()
}

async fn run(
    columns: [TestColumn; 2],
    limits: PositionDeleteLimits,
) -> Result<PositionDeleteSummary, SelectedParquetError> {
    let rows = columns[0].pages.iter().map(|(count, _)| count).sum();
    let (store, record) = file(table(), columns, true, 6).await;
    let mut entry = selected::entry(FileContentKind::PositionDeletes);
    entry.entry.record_count = rows;
    entry.file.location = record.location.clone();
    entry.file.length = record.length;
    let context = selected::context(json!([]));
    validate_parquet_position_deletes(
        store,
        &record,
        ParquetSelection {
            entry: &entry,
            context: &context,
            table: table(),
            mapping: None,
        },
        &TestTargets,
        limits,
    )
    .await
}

fn column(physical: i64, encoding: i64, bytes: Vec<u8>) -> TestColumn {
    TestColumn {
        physical,
        encoding,
        pages: vec![(3, bytes)],
        dictionary: None,
    }
}

fn delta(first: i64, increment: i64) -> Vec<u8> {
    let mut bytes = fixture::unsigned(128);
    bytes.extend([4, 3]);
    bytes.extend(fixture::number(first));
    bytes.extend(fixture::number(increment));
    bytes.extend([0; 4]);
    bytes
}

#[tokio::test]
async fn delta_length_prefix_and_byte_stream_split_decode_exact_values() {
    for encoding in [6, 7] {
        let mut bytes = if encoding == 7 { delta(0, 0) } else { vec![] };
        bytes.extend(delta(i64::try_from(path().len()).unwrap(), 0));
        bytes.extend(path().repeat(3).as_bytes());
        let result = run(
            [column(6, encoding, bytes), column(2, 5, delta(0, 1))],
            delete_limits(),
        )
        .await
        .unwrap();
        assert_eq!(result.rows, 3);
    }
    let values = [0_i64, 1, 2];
    let mut split = vec![];
    for byte in 0..8 {
        for value in values {
            split.push(value.to_le_bytes()[byte]);
        }
    }
    assert!(run(
        [
            column(6, 0, strings(&[path(), path(), path()])),
            column(2, 9, split)
        ],
        delete_limits()
    )
    .await
    .is_ok());
}

#[tokio::test]
async fn bitpacked_dictionary_indices_and_padding_are_bounded() {
    let mut paths = column(6, 8, vec![1, 3, 0]);
    paths.dictionary = Some((1, strings(&[path()])));
    let mut positions = column(2, 8, vec![2, 3, 0b0010_0100, 0]);
    positions.dictionary = Some((3, longs(&[0, 1, 2])));
    assert!(run([paths, positions], delete_limits()).await.is_ok());
    for indices in [
        vec![33, 6],
        vec![1, 8, 0],
        vec![1, 6, 2],
        vec![1, 3, 255],
        vec![0, 0],
    ] {
        let mut paths = column(6, 8, indices);
        paths.dictionary = Some((1, strings(&[path()])));
        assert!(run([paths, column(2, 0, longs(&[0, 1, 2]))], delete_limits())
            .await
            .is_err());
    }
}

#[tokio::test]
async fn malformed_delta_headers_lengths_suffixes_and_plain_values_fail() {
    let invalid = [
        (5, vec![0, 4, 3, 0]),
        (5, vec![128, 1, 0, 3, 0]),
        (5, {
            let mut bytes = delta(0, 1);
            bytes[3] = 4;
            bytes
        }),
        (0, vec![0; 23]),
        (0, vec![0; 25]),
        (9, vec![0; 23]),
    ];
    for (encoding, bytes) in invalid {
        assert!(run(
            [
                column(6, 0, strings(&[path(), path(), path()])),
                column(2, encoding, bytes)
            ],
            delete_limits()
        )
        .await
        .is_err());
    }
    for bytes in [delta(-1, 0), delta(2000, 0)] {
        assert!(run(
            [column(6, 6, bytes), column(2, 0, longs(&[0, 1, 2]))],
            delete_limits()
        )
        .await
        .is_err());
    }
}

#[tokio::test]
async fn independent_row_value_and_decoded_byte_limits_fail_closed() {
    for bound in 0..3 {
        let mut limits = delete_limits();
        match bound {
            0 => limits.rows = 2,
            1 => limits.page.values = 2,
            _ => limits.page.bytes = 64,
        }
        assert!(run(
            [
                column(6, 0, strings(&[path(), path(), path()])),
                column(2, 0, longs(&[0, 1, 2]))
            ],
            limits
        )
        .await
        .is_err());
    }
}
