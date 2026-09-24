#[path = "common/file_blocks.rs"]
mod blocks;
#[path = "common/table_metadata.rs"]
#[allow(dead_code)]
mod fixture;
#[path = "common/partition_statistics_official.rs"]
mod official;
#[path = "common/parquet_metadata.rs"]
#[allow(dead_code)]
mod parquet;

use crowdb_access_iceberg::{
    file::{ParquetMetadata, ParquetMetadataError, ParquetSchemaElement},
    manifest::{validate_partition_statistics_schema, SelectedParquetError},
};
use serde_json::{json, Value};

fn table(version: u8, delete_source: bool) -> Value {
    let mut value = fixture::metadata(version);
    let old_schema = json!({"schema-id":0,"type":"struct","fields":[
        {"id":1,"name":"old","type":"long","required":false},
        {"id":2,"name":"kept","type":"int","required":false}]});
    let mut current_schema = old_schema.clone();
    current_schema["schema-id"] = json!(1);
    if delete_source {
        current_schema["fields"].as_array_mut().unwrap().remove(0);
    }
    value["schemas"] = json!([old_schema, current_schema]);
    value["current-schema-id"] = json!(1);
    value["last-column-id"] = json!(2);
    value["partition-specs"] = json!([
        {"spec-id":0,"fields":[
            {"source-id":1,"field-id":1000,"name":"old_part","transform":"identity"},
            {"source-id":2,"field-id":1001,"name":"kept_part","transform":"identity"}]},
        {"spec-id":1,"fields":[
            {"source-id":2,"field-id":1001,"name":"renamed","transform":"identity"}]}]);
    value["default-spec-id"] = json!(1);
    value["last-partition-id"] = json!(1001);
    if version == 1 {
        value["schema"] = current_schema;
        value["partition-spec"] = value["partition-specs"][1]["fields"].clone();
    }
    value
}

fn field(id: i32, physical: Option<i32>, required: bool, children: usize) -> ParquetSchemaElement {
    ParquetSchemaElement {
        name: format!("field_{id}"),
        field_id: Some(id),
        physical_type: physical,
        repetition: Some(i32::from(!required)),
        children,
        type_length: None,
        converted_type: None,
        scale: None,
        precision: None,
        logical_type: None,
    }
}

fn metadata(version: u8, omit: bool) -> ParquetMetadata {
    let mut schema = vec![
        field(0, None, true, if version == 3 { 13 } else { 12 }),
        field(1, None, true, if omit { 1 } else { 2 }),
    ];
    if !omit {
        schema.push(field(1000, Some(2), false, 0));
    }
    schema.push(field(1001, Some(1), false, 0));
    for id in 2..=if version == 3 { 13 } else { 12 } {
        schema.push(field(
            id,
            Some(if matches!(id, 2 | 4 | 7 | 9 | 13) { 1 } else { 2 }),
            id <= 5 || (version == 3 && matches!(id, 6..=9 | 13)),
            0,
        ));
    }
    ParquetMetadata {
        rows: 0,
        row_groups: 0,
        schema,
        groups: vec![],
    }
}

fn validate(metadata: &ParquetMetadata, table: &Value) -> Result<(), SelectedParquetError> {
    validate_partition_statistics_schema(metadata, &fixture::parse(table).unwrap(), &mut 100_000)
}

#[test]
fn union_retains_dropped_partition_fields_when_the_source_still_exists() {
    for version in 1..=3 {
        assert!(validate(&metadata(version, false), &table(version, false)).is_ok());
        assert!(validate(&metadata(version, true), &table(version, false)).is_err());
    }
}

#[test]
fn deleted_sources_may_be_omitted_but_retained_values_still_require_their_types() {
    for version in 1..=3 {
        let table = table(version, true);
        assert!(validate(&metadata(version, true), &table).is_ok());
        assert!(validate(&metadata(version, false), &table).is_ok());
        let mut invalid = metadata(version, false);
        invalid.schema[2].physical_type = Some(6);
        assert!(validate(&invalid, &table).is_err());
        let mut invalid = metadata(version, true);
        invalid.schema[2].field_id = Some(1002);
        assert!(validate(&invalid, &table).is_err());
    }
}

#[test]
fn absent_source_history_cannot_be_invented_from_the_file() {
    let mut table = table(2, true);
    table["schemas"].as_array_mut().unwrap().remove(0);
    assert!(validate(&metadata(2, true), &table).is_ok());
    assert!(validate(&metadata(2, false), &table).is_err());
}

#[test]
fn partition_ids_are_ordered_unique_and_optional_primitive_fields() {
    let table = table(2, false);
    for change in 0..5 {
        let mut invalid = metadata(2, false);
        match change {
            0 => invalid.schema.swap(2, 3),
            1 => invalid.schema[3].field_id = Some(1000),
            2 => invalid.schema[2].repetition = Some(0),
            3 => invalid.schema[2].children = 1,
            _ => invalid.schema[2].field_id = None,
        }
        assert!(validate(&invalid, &table).is_err());
    }
}

#[test]
fn required_statistics_fields_depend_on_table_version() {
    for version in 1..=3 {
        for id in [1, 2, 3, 4, 5, 6, 7, 8, 9, 13] {
            let mut invalid = metadata(version, false);
            let Some(index) = invalid.schema.iter().position(|field| field.field_id == Some(id)) else {
                continue;
            };
            invalid.schema[index].repetition = Some(1);
            assert_eq!(
                validate(&invalid, &table(version, false)).is_ok(),
                version < 3 && id > 5
            );
        }
        let mut minimal = metadata(version, false);
        minimal
            .schema
            .retain(|field| !matches!(field.field_id, Some(10..=12)));
        minimal.schema[0].children -= 3;
        assert!(validate(&minimal, &table(version, false)).is_ok());
        minimal
            .schema
            .retain(|field| !matches!(field.field_id, Some(6..=9)));
        minimal.schema[0].children -= 4;
        assert_eq!(validate(&minimal, &table(version, false)).is_ok(), version < 3);
    }
}

#[test]
fn reused_partition_ids_must_have_compatible_sources_and_transforms() {
    for (key, value) in [("source-id", json!(1)), ("transform", json!("bucket[16]"))] {
        let mut table = table(2, false);
        table["partition-specs"][0]["fields"][1][key] = value;
        assert!(validate(&metadata(2, false), &table).is_err());
    }
    let mut table = table(1, false);
    table["partition-specs"][1]["fields"][0]["transform"] = json!("void");
    table["partition-spec"] = table["partition-specs"][1]["fields"].clone();
    assert!(validate(&metadata(1, false), &table).is_ok());
}

#[test]
fn projection_and_schema_checks_share_one_work_budget() {
    let table = fixture::parse(&table(2, true)).unwrap();
    let metadata = metadata(2, true);
    let mut work = 100_000;
    validate_partition_statistics_schema(&metadata, &table, &mut work).unwrap();
    let consumed = 100_000 - work;
    assert!(consumed > metadata.schema.len());
    assert!(validate_partition_statistics_schema(&metadata, &table, &mut consumed.clone()).is_ok());
    assert!(matches!(
        validate_partition_statistics_schema(&metadata, &table, &mut (consumed - 1)),
        Err(SelectedParquetError::Metadata(ParquetMetadataError::Bounds))
    ));
}

#[tokio::test]
async fn official_sdk_unified_partition_and_v2_v3_statistics_schemas_match() {
    for (version, deleted, encoded) in [
        (2, false, official::STATS_2_FALSE),
        (2, true, official::STATS_2_TRUE),
        (3, false, official::STATS_3_FALSE),
        (3, true, official::STATS_3_TRUE),
    ] {
        let bytes = data_encoding::BASE64.decode(encoded.as_bytes()).unwrap();
        let (store, record) = parquet::stored_content(&bytes, fixture::table()).await;
        let metadata = crowdb_access_iceberg::file::read_parquet_metadata(store, &record, parquet::limits())
            .await
            .unwrap();
        assert_eq!(metadata.rows, 1);
        assert!(validate(&metadata, &table(version, deleted)).is_ok());
        if deleted {
            assert!(validate(&metadata, &table(version, false)).is_err());
        }
    }
}
