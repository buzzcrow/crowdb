#[path = "common/file_blocks.rs"]
mod blocks;
#[path = "common/manifest_entry.rs"]
#[allow(dead_code)]
mod fixture;
#[path = "common/manifest_list.rs"]
#[allow(dead_code)]
mod list_fixture;
#[path = "common/statistics_manifest.rs"]
mod manifests;
#[path = "common/table_metadata.rs"]
#[allow(dead_code)]
mod metadata;
#[path = "common/parquet_metadata.rs"]
#[allow(dead_code)]
mod parquet;
#[path = "common/partition_statistics_rows.rs"]
#[allow(dead_code)]
mod rows;
#[path = "common/snapshot_files.rs"]
#[allow(dead_code)]
mod snapshot;

use crowdb_access_iceberg::{
    file::{read_parquet_metadata, ContentFormat, ParquetPageLimits},
    manifest::{
        validate_partition_statistics_inventory, PartitionStatisticsRowLimits, SnapshotManifestReader,
        SnapshotValidationError,
    },
};
use std::sync::Arc;

async fn validate(
    data_rows: i64,
    counts: &[rows::TestColumn],
    work: &mut usize,
) -> Result<(), SnapshotValidationError> {
    validate_with_delete(data_rows, counts, work, None).await
}

async fn validate_with_delete(
    data_rows: i64,
    counts: &[rows::TestColumn],
    work: &mut usize,
    delete: Option<(i64, bool)>,
) -> Result<(), SnapshotValidationError> {
    let store = Arc::new(blocks::TestBlocks::default());
    let data = snapshot::data(store.clone(), "data/input.parquet").await;
    let mut entry = snapshot::entry(&data, 0, data_rows);
    entry.set(104, serde_json::json!(100));
    let mut groups = vec![vec![entry]];
    if let Some((kind, vector)) = delete {
        let record = snapshot::store(
            store.clone(),
            "deletes/rows",
            if vector {
                ContentFormat::Puffin
            } else {
                ContentFormat::Parquet
            },
            &[],
        )
        .await;
        let mut entry = snapshot::entry(&record, kind, 3);
        entry.set(104, serde_json::json!(100));
        if kind == 2 {
            entry.set(135, serde_json::json!([3]));
        }
        if vector {
            entry.set(143, serde_json::json!(data.location.to_string()));
            entry.set(144, serde_json::json!(4));
            entry.set(145, serde_json::json!(20));
        }
        groups.push(vec![entry]);
    }
    let input = snapshot::input(store.clone(), groups, vec![data]).await;
    let mut reader = SnapshotManifestReader::open(
        store.clone(),
        input.manifests,
        input.list,
        input.selection,
        snapshot::limits().manifests,
    )
    .await
    .unwrap();
    let statistics = snapshot::store(
        store.clone(),
        "metadata/statistics.parquet",
        ContentFormat::Parquet,
        &rows::bytes(counts, 1, 1),
    )
    .await;
    let metadata = read_parquet_metadata(store.clone(), &statistics, parquet::limits())
        .await
        .unwrap();
    let mut document = serde_json::Value::Object(rows::table(&[]).fields().clone());
    if delete.is_some_and(|(_, vector)| vector) {
        document["format-version"] = serde_json::json!(3);
        document["next-row-id"] = serde_json::json!(0);
    }
    validate_partition_statistics_inventory(
        store,
        &statistics,
        &metadata,
        &metadata::parse(&document).unwrap(),
        &mut reader,
        PartitionStatisticsRowLimits {
            page: ParquetPageLimits {
                bytes: 8192,
                values: 100,
                pages: 100,
            },
            rows: 100,
            buffered_bytes: 4 * 1024 * 1024,
        },
        work,
    )
    .await
}

#[tokio::test]
async fn statistics_cannot_invent_manifest_record_file_or_byte_counts() {
    assert!(validate(10, &rows::counts(1), &mut 100_000).await.is_ok());
    for (id, value) in [(3, 11), (4, 2), (5, 101)] {
        let mut counts = rows::counts(1);
        *counts.iter_mut().find(|column| column.id == id).unwrap() = rows::integers(id, &[value]);
        assert!(validate(10, &counts, &mut 100_000).await.is_err());
    }
}

#[tokio::test]
async fn unknown_optional_counts_remain_unknown_but_present_counts_are_checked() {
    for (delete_count, valid) in [(None, true), (Some(0_i64), true), (Some(1), false)] {
        let mut counts = rows::counts(1);
        let mut column = rows::column(8, 2, vec![delete_count.map(|value| value.to_le_bytes().to_vec())]);
        column.optional = true;
        counts.push(column);
        assert_eq!(validate(10, &counts, &mut 100_000).await.is_ok(), valid);
    }
}

#[tokio::test]
async fn total_counts_without_deletes_are_metadata_derivable() {
    for total in [9, 10, 11] {
        let mut counts = rows::counts(1);
        counts.push(rows::integers(10, &[total]));
        assert_eq!(validate(10, &counts, &mut 100_000).await.is_ok(), total == 10);
    }
}

#[tokio::test]
async fn delete_and_vector_counters_are_separate_and_do_not_recompute_engine_totals() {
    for (kind, vector, record_id, file_id) in [(1, false, 6, 7), (2, false, 8, 9), (1, true, 6, 13)] {
        let mut counts = rows::counts(1);
        for id in [6, 7, 8, 9, 13] {
            if id == 13 && !vector {
                continue;
            }
            counts.push(rows::integers(
                id,
                &[if id == record_id {
                    3
                } else {
                    i64::from(id == file_id)
                }],
            ));
        }
        counts.push(rows::integers(10, &[if vector { 7 } else { 8 }]));
        validate_with_delete(10, &counts, &mut 100_000, Some((kind, vector)))
            .await
            .unwrap();
        *counts.iter_mut().find(|column| column.id == file_id).unwrap() = rows::integers(file_id, &[2]);
        assert!(
            validate_with_delete(10, &counts, &mut 100_000, Some((kind, vector)))
                .await
                .is_err()
        );
    }
}

#[tokio::test]
async fn manifest_inventory_work_is_charged_to_the_same_budget() {
    assert!(validate(10, &rows::counts(1), &mut 1).await.is_err());
    let mut work = 100_000;
    validate(10, &rows::counts(1), &mut work).await.unwrap();
    assert!(validate(10, &rows::counts(1), &mut (99_999 - work))
        .await
        .is_err());
}

#[tokio::test]
async fn reconciliation_binds_counts_to_partition_tuples_not_just_snapshot_totals() {
    for (values, records, valid) in [
        (vec![1, 2], vec![10, 20], true),
        (vec![1, 2], vec![20, 10], false),
        (vec![1], vec![10], false),
        (vec![1, 3], vec![10, 20], false),
        (vec![], vec![], false),
    ] {
        let store = Arc::new(blocks::TestBlocks::default());
        let document = rows::table(&["long"]);
        let mut reader = manifests::reader(
            store.clone(),
            &document,
            &[manifests::entry(0, 1, 10), manifests::entry(1, 2, 20)],
        )
        .await;
        let mut columns = vec![rows::column(
            1000,
            2,
            values
                .iter()
                .map(|value: &i64| Some(value.to_le_bytes().to_vec()))
                .collect(),
        )];
        columns.extend(rows::counts(values.len()));
        columns[2] = rows::integers(3, &records);
        let record = snapshot::store(
            store.clone(),
            "metadata/stats.parquet",
            ContentFormat::Parquet,
            &rows::bytes(&columns, 1, 1),
        )
        .await;
        let metadata = read_parquet_metadata(store.clone(), &record, parquet::limits())
            .await
            .unwrap();
        let result = validate_partition_statistics_inventory(
            store,
            &record,
            &metadata,
            &document,
            &mut reader,
            PartitionStatisticsRowLimits {
                page: ParquetPageLimits {
                    bytes: 8192,
                    values: 100,
                    pages: 100,
                },
                rows: 100,
                buffered_bytes: 4 * 1024 * 1024,
            },
            &mut 100_000,
        )
        .await;
        assert_eq!(result.is_ok(), valid, "{result:?}");
    }
}

#[tokio::test]
async fn omitted_historical_fields_reconcile_collapsed_tuple_counts_without_inventing_values() {
    let original = rows::table(&["long", "long"]);
    let mut fields = serde_json::Value::Object(original.fields().clone());
    let mut current = fields["schemas"][0].clone();
    current["schema-id"] = 1.into();
    current["fields"].as_array_mut().unwrap().pop();
    fields["schemas"].as_array_mut().unwrap().push(current);
    fields["current-schema-id"] = 1.into();
    let mut spec = fields["partition-specs"][0].clone();
    spec["spec-id"] = 1.into();
    spec["fields"].as_array_mut().unwrap().pop();
    fields["partition-specs"].as_array_mut().unwrap().push(spec);
    fields["default-spec-id"] = 1.into();
    let document = metadata::parse(&fields).unwrap();
    for correct in [true, false] {
        let store = Arc::new(blocks::TestBlocks::default());
        let mut entries = vec![manifests::entry(0, 1, 10), manifests::entry(1, 1, 20)];
        for (index, entry) in entries.iter_mut().enumerate() {
            entry
                .partition_fields
                .push(serde_json::json!({"name":"deleted_source", "field-id":1001,
                "type":["null","long"]}));
            entry.partition_bytes.push(2);
            entry
                .partition_bytes
                .extend(parquet::number(i64::try_from(index).unwrap()));
        }
        let mut reader = manifests::reader(store.clone(), &document, &entries).await;
        let mut columns = vec![rows::column(1000, 2, vec![Some(1_i64.to_le_bytes().to_vec()); 2])];
        columns.extend(rows::counts(2));
        columns[2] = rows::integers(3, &[10, if correct { 20 } else { 19 }]);
        let record = snapshot::store(
            store.clone(),
            "metadata/stats.parquet",
            ContentFormat::Parquet,
            &rows::bytes(&columns, 1, 1),
        )
        .await;
        let metadata = read_parquet_metadata(store.clone(), &record, parquet::limits())
            .await
            .unwrap();
        let result = validate_partition_statistics_inventory(
            store,
            &record,
            &metadata,
            &document,
            &mut reader,
            PartitionStatisticsRowLimits {
                page: ParquetPageLimits {
                    bytes: 8192,
                    values: 100,
                    pages: 100,
                },
                rows: 100,
                buffered_bytes: 4 * 1024 * 1024,
            },
            &mut 100_000,
        )
        .await;
        assert_eq!(result.is_ok(), correct, "{result:?}");
    }
}
