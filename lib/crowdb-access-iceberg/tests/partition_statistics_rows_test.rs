#[path = "common/file_blocks.rs"]
mod blocks;
#[path = "common/table_metadata.rs"]
#[allow(dead_code)]
mod fixture;
use fixture as metadata;
#[path = "common/parquet_metadata.rs"]
#[allow(dead_code)]
mod parquet;
#[path = "common/partition_statistics_rows.rs"]
mod rows;

use crowdb_access_iceberg::{
    file::{read_parquet_metadata, ParquetPageLimits},
    manifest::{validate_partition_statistics_rows, PartitionStatisticsRowLimits, SelectedParquetError},
    table::TableMetadataDocument,
};

fn limits() -> PartitionStatisticsRowLimits {
    PartitionStatisticsRowLimits {
        page: ParquetPageLimits {
            bytes: 8192,
            values: 100,
            pages: 100,
        },
        rows: 100,
        buffered_bytes: 4 * 1024 * 1024,
    }
}

async fn validate(
    columns: &[rows::TestColumn],
    table: &TableMetadataDocument,
    group: usize,
    page: usize,
) -> Result<(), SelectedParquetError> {
    let (store, record) = rows::file(columns, group, page).await;
    let metadata = read_parquet_metadata(store.clone(), &record, parquet::limits())
        .await
        .unwrap();
    validate_partition_statistics_rows(store, &record, &metadata, table, limits(), &mut 100_000).await
}

fn longs(values: &[Option<i64>]) -> rows::TestColumn {
    rows::column(
        1000,
        2,
        values
            .iter()
            .map(|value| value.map(|value| value.to_le_bytes().to_vec()))
            .collect(),
    )
}

#[tokio::test]
async fn null_first_tuple_order_and_duplicates_cross_pages_and_groups() {
    for (values, valid) in [
        (vec![None, Some(-2), Some(0), Some(1)], true),
        (vec![Some(-2), None, Some(0), Some(1)], false),
        (vec![None, Some(0), Some(1), Some(0)], false),
        (vec![None, Some(0), Some(0), Some(1)], false),
    ] {
        let mut columns = vec![longs(&values)];
        columns.extend(rows::counts(values.len()));
        for group in [2, 4] {
            for page in [1, 2] {
                assert_eq!(
                    validate(&columns, &rows::table(&["long"]), group, page)
                        .await
                        .is_ok(),
                    valid
                );
            }
        }
    }
}

#[tokio::test]
async fn ordering_is_lexicographic_not_independently_sorted_columns() {
    let mut first = longs(&[Some(0), Some(0), Some(1), Some(1)]);
    let mut second = longs(&[Some(5), Some(9), Some(-5), Some(-1)]);
    second.id = 1001;
    let mut columns = vec![first, second];
    columns.extend(rows::counts(4));
    assert!(validate(&columns, &rows::table(&["long", "long"]), 2, 1)
        .await
        .is_ok());
    first = longs(&[Some(0), Some(1), Some(0), Some(1)]);
    columns[0] = first;
    assert!(validate(&columns, &rows::table(&["long", "long"]), 2, 1)
        .await
        .is_err());
}

#[tokio::test]
async fn counters_and_spec_ids_are_checked_without_requiring_expired_snapshot_history() {
    for (id, values) in [
        (2, [0, 99]),
        (3, [10, -1]),
        (4, [1, 0]),
        (5, [100, -1]),
        (10, [10, 11]),
    ] {
        let mut columns = vec![longs(&[None, Some(1)])];
        columns.extend(rows::counts(2));
        let replacement = rows::integers(id, &values);
        if let Some(column) = columns.iter_mut().find(|column| column.id == id) {
            *column = replacement;
        } else {
            columns.push(replacement);
        }
        assert!(validate(&columns, &rows::table(&["long"]), 1, 1).await.is_err());
    }
}

#[tokio::test]
async fn float_order_matches_java_nan_last_and_negative_zero_first() {
    let bits = [
        0xfff0_0000_0000_0000_u64,
        0x8000_0000_0000_0000,
        0,
        0x7ff0_0000_0000_0000,
        0xfff8_0000_0000_0001,
    ];
    let mut columns = vec![rows::column(
        1000,
        5,
        bits.iter()
            .map(|value| Some(value.to_le_bytes().to_vec()))
            .collect(),
    )];
    columns.extend(rows::counts(5));
    assert!(validate(&columns, &rows::table(&["double"]), 2, 1).await.is_ok());
    columns[0].values.swap(1, 2);
    assert!(validate(&columns, &rows::table(&["double"]), 2, 1).await.is_err());
    columns[0].values.swap(1, 2);
    columns[0].values[3] = Some(f64::NAN.to_bits().to_le_bytes().to_vec());
    assert!(validate(&columns, &rows::table(&["double"]), 2, 1).await.is_err());
}

#[tokio::test]
async fn strings_use_unicode_order_and_reject_invalid_utf8() {
    let mut columns = vec![rows::column(
        1000,
        6,
        ["", "a", "\u{ffff}", "\u{10000}"]
            .map(|value| Some(value.as_bytes().to_vec()))
            .to_vec(),
    )];
    columns.extend(rows::counts(4));
    assert!(validate(&columns, &rows::table(&["string"]), 2, 1).await.is_ok());
    columns[0].values[3] = Some(vec![255]);
    assert!(validate(&columns, &rows::table(&["string"]), 2, 1).await.is_err());
}

#[tokio::test]
async fn decimal_sign_extension_and_declared_precision_are_enforced() {
    let mut column = rows::column(1000, 6, [vec![255, 255], vec![0], vec![0, 99]].map(Some).to_vec());
    column.annotations = vec![
        (6, 5, parquet::number(5)),
        (7, 5, parquet::number(0)),
        (8, 5, parquet::number(2)),
    ];
    let mut columns = vec![column];
    columns.extend(rows::counts(3));
    assert!(validate(&columns, &rows::table(&["decimal(2,0)"]), 2, 1)
        .await
        .is_ok());
    columns[0].values[2] = Some(vec![100]);
    assert!(validate(&columns, &rows::table(&["decimal(2,0)"]), 2, 1)
        .await
        .is_err());
}

#[tokio::test]
async fn time_values_check_unit_specific_day_bounds() {
    for (physical, converted, upper) in [(1, 7, 86_400_000_i64), (2, 8, 86_400_000_000)] {
        let encoded = |value: i64| {
            if physical == 1 {
                i32::try_from(value).unwrap().to_le_bytes().to_vec()
            } else {
                value.to_le_bytes().to_vec()
            }
        };
        let mut column = rows::column(1000, physical, vec![Some(encoded(0)), Some(encoded(upper - 1))]);
        column.annotations = vec![(6, 5, parquet::number(converted))];
        let mut columns = vec![column];
        columns.extend(rows::counts(2));
        assert!(validate(&columns, &rows::table(&["time"]), 1, 1).await.is_ok());
        columns[0].values[1] = Some(encoded(upper));
        assert!(validate(&columns, &rows::table(&["time"]), 1, 1).await.is_err());
    }
}

#[tokio::test]
async fn row_work_and_buffer_limits_fail_before_unbounded_materialization() {
    let mut columns = vec![longs(&[None, Some(1)])];
    columns.extend(rows::counts(2));
    let (store, record) = rows::file(&columns, 2, 1).await;
    let metadata = read_parquet_metadata(store.clone(), &record, parquet::limits())
        .await
        .unwrap();
    for limited in [
        PartitionStatisticsRowLimits { rows: 1, ..limits() },
        PartitionStatisticsRowLimits {
            buffered_bytes: 1,
            ..limits()
        },
    ] {
        assert!(validate_partition_statistics_rows(
            store.clone(),
            &record,
            &metadata,
            &rows::table(&["long"]),
            limited,
            &mut 100_000
        )
        .await
        .is_err());
    }
    let mut work = 100_000;
    validate_partition_statistics_rows(
        store.clone(),
        &record,
        &metadata,
        &rows::table(&["long"]),
        limits(),
        &mut work,
    )
    .await
    .unwrap();
    assert!(validate_partition_statistics_rows(
        store,
        &record,
        &metadata,
        &rows::table(&["long"]),
        limits(),
        &mut (99_999 - work)
    )
    .await
    .is_err());
}

#[tokio::test]
async fn wide_statistics_share_the_existing_buffer_cap_without_reserving_full_pages_per_column() {
    for fields in [2, 10] {
        let mut columns: Vec<_> = (0..fields)
            .map(|index| rows::column(1000 + index, 2, vec![Some(7_i64.to_le_bytes().to_vec())]))
            .collect();
        columns.extend(rows::counts(1));
        for id in [6, 7, 8, 9, 13] {
            columns.push(rows::integers(id, &[0]));
        }
        columns.extend([
            rows::integers(10, &[10]),
            rows::integers(11, &[100]),
            rows::integers(12, &[99]),
        ]);
        let mut table = serde_json::Value::Object(
            rows::table(&vec!["long"; usize::try_from(fields).unwrap()])
                .fields()
                .clone(),
        );
        table["format-version"] = serde_json::json!(3);
        table["next-row-id"] = serde_json::json!(0);
        let document = fixture::parse(&table).unwrap();
        let (store, record) = rows::file(&columns, 1, 1).await;
        let metadata = read_parquet_metadata(store.clone(), &record, parquet::limits())
            .await
            .unwrap();
        let mut bounded = limits();
        bounded.page.bytes = 1024 * 1024;
        bounded.buffered_bytes = 64 * 1024 * 1024;
        validate_partition_statistics_rows(
            store.clone(),
            &record,
            &metadata,
            &document,
            bounded,
            &mut 100_000,
        )
        .await
        .unwrap();
        bounded.buffered_bytes = 64 * 1024;
        assert!(validate_partition_statistics_rows(
            store.clone(),
            &record,
            &metadata,
            &document,
            bounded,
            &mut 100_000
        )
        .await
        .is_err());
        bounded.buffered_bytes = 64 * 1024 * 1024;
        bounded.page.bytes = 1;
        assert!(validate_partition_statistics_rows(
            store,
            &record,
            &metadata,
            &document,
            bounded,
            &mut 100_000
        )
        .await
        .is_err());
    }
}

#[tokio::test]
async fn uuid_order_uses_signed_java_halves() {
    let values = [(-1_i64, 0_i64), (0, -1), (0, 0), (1, 0)];
    let mut column = rows::column(
        1000,
        7,
        values
            .into_iter()
            .map(|(high, low)| Some([high.to_be_bytes(), low.to_be_bytes()].concat()))
            .collect(),
    );
    column.annotations = vec![
        (2, 5, parquet::number(16)),
        (
            10,
            12,
            parquet::structure(&vec![(14, 12, parquet::structure(&vec![]))]),
        ),
    ];
    let mut columns = vec![column];
    columns.extend(rows::counts(4));
    assert!(validate(&columns, &rows::table(&["uuid"]), 2, 1).await.is_ok());
    columns[0].values.swap(0, 1);
    assert!(validate(&columns, &rows::table(&["uuid"]), 2, 1).await.is_err());
}

#[tokio::test]
async fn transforms_are_checked_against_each_rows_spec() {
    for (transform, physical, valid, invalid) in
        [("truncate[10]", 2, -10_i64, -9_i64), ("bucket[10]", 1, 9, 10)]
    {
        let table = rows::table(&["long"]);
        let mut fields = serde_json::to_value(table.fields()).unwrap();
        fields["partition-specs"][0]["fields"][0]["transform"] = transform.into();
        let table = fixture::parse(&fields).unwrap();
        let encode = |value: i64| {
            Some(if physical == 1 {
                i32::try_from(value).unwrap().to_le_bytes().to_vec()
            } else {
                value.to_le_bytes().to_vec()
            })
        };
        let mut columns = vec![rows::column(1000, physical, vec![None, encode(valid)])];
        columns.extend(rows::counts(2));
        assert!(validate(&columns, &table, 1, 1).await.is_ok());
        columns[0].values[1] = encode(invalid);
        assert!(validate(&columns, &table, 1, 1).await.is_err());
    }
}

#[tokio::test]
async fn equal_projected_rows_do_not_invent_omitted_historical_values() {
    let table = rows::table(&["long", "long"]);
    let mut fields = serde_json::to_value(table.fields()).unwrap();
    let mut current_spec = fields["partition-specs"][0].clone();
    current_spec["spec-id"] = 1.into();
    current_spec["fields"].as_array_mut().unwrap().pop();
    fields["partition-specs"]
        .as_array_mut()
        .unwrap()
        .push(current_spec);
    fields["default-spec-id"] = 1.into();
    fields["schemas"][0]["fields"].as_array_mut().unwrap().pop();
    let table = fixture::parse(&fields).unwrap();
    let mut columns = vec![longs(&[Some(1), Some(1)])];
    columns.extend(rows::counts(2));
    assert!(validate(&columns, &table, 1, 1).await.is_ok());
    assert!(validate(&columns, &rows::table(&["long"]), 1, 1).await.is_err());
    columns[0].values[1] = Some(0_i64.to_le_bytes().to_vec());
    assert!(validate(&columns, &table, 1, 1).await.is_err());
}

#[tokio::test]
async fn row_group_metadata_must_match_canonical_leaf_and_total_counts() {
    let mut columns = vec![longs(&[None, Some(1)])];
    columns.extend(rows::counts(2));
    let (store, record) = rows::file(&columns, 1, 1).await;
    for mutation in 0..3 {
        let mut altered = read_parquet_metadata(store.clone(), &record, parquet::limits())
            .await
            .unwrap();
        match mutation {
            0 => altered.rows += 1,
            1 => altered.groups[0].columns.swap(0, 1),
            _ => altered.groups[0].columns[0].values += 1,
        }
        assert!(validate_partition_statistics_rows(
            store.clone(),
            &record,
            &altered,
            &rows::table(&["long"]),
            limits(),
            &mut 100_000
        )
        .await
        .is_err());
    }
}
