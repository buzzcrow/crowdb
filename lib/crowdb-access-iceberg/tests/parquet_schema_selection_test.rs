#[path = "common/selected_parquet.rs"]
mod fixture;

use crowdb_access_iceberg::file::{ParquetLogicalType as Logical, ParquetTimeUnit as Unit};
use crowdb_access_iceberg::manifest::{validate_parquet_schema, FileContentKind, ParquetFieldMapping};
use fixture::{context, entry, field, metadata, node};
use serde_json::json;

#[test]
fn field_identity_survives_renames_reordering_and_numeric_promotion() {
    let context = context(json!([
        field(1, "renamed", json!("long")),
        field(2, "new", json!("double")),
        field(3, "absent", json!("string"))
    ]));
    let metadata = metadata(
        2,
        vec![
            node(Some(2), "old_float", Some(4), 0),
            node(Some(1), "old_int", Some(1), 0),
        ],
    );
    let selected = validate_parquet_schema(&metadata, &context, &entry(FileContentKind::Data), None).unwrap();
    assert_eq!(selected.field_index(1), Some(2));
    assert_eq!(selected.field_index(2), Some(1));
    assert_eq!(selected.field_index(3), None);
    let incompatible = self::context(json!([
        field(1, "renamed", json!("int")),
        field(2, "new", json!("float"))
    ]));
    let mut metadata = metadata;
    metadata.schema[2].physical_type = Some(2);
    assert!(validate_parquet_schema(&metadata, &incompatible, &entry(FileContentKind::Data), None).is_err());
}

#[test]
#[allow(clippy::too_many_lines)]
fn decimal_uuid_time_and_timestamp_annotations_bind_their_parameters() {
    for (kind, physical, logical, length, valid) in [
        (
            "decimal(20,2)",
            2,
            Logical::Decimal {
                precision: 18,
                scale: 2,
            },
            None,
            true,
        ),
        (
            "decimal(20,2)",
            2,
            Logical::Decimal {
                precision: 19,
                scale: 2,
            },
            None,
            false,
        ),
        (
            "decimal(20,2)",
            7,
            Logical::Decimal {
                precision: 20,
                scale: 2,
            },
            Some(9),
            true,
        ),
        (
            "decimal(20,2)",
            7,
            Logical::Decimal {
                precision: 20,
                scale: 2,
            },
            Some(8),
            false,
        ),
        (
            "decimal(20,2)",
            6,
            Logical::Decimal {
                precision: 18,
                scale: 3,
            },
            None,
            false,
        ),
        ("uuid", 7, Logical::Uuid, Some(16), true),
        ("uuid", 7, Logical::Uuid, Some(15), false),
        (
            "time",
            1,
            Logical::Time {
                adjusted_to_utc: false,
                unit: Unit::Millis,
            },
            None,
            true,
        ),
        (
            "time",
            1,
            Logical::Time {
                adjusted_to_utc: false,
                unit: Unit::Micros,
            },
            None,
            false,
        ),
        (
            "timestamp_ns",
            2,
            Logical::Timestamp {
                adjusted_to_utc: false,
                unit: Unit::Nanos,
            },
            None,
            true,
        ),
        (
            "timestamp",
            2,
            Logical::Timestamp {
                adjusted_to_utc: true,
                unit: Unit::Micros,
            },
            None,
            false,
        ),
    ] {
        let context = context(json!([field(1, "value", json!(kind))]));
        let mut column = node(Some(1), "value", Some(physical), 0);
        column.logical_type = Some(logical);
        column.type_length = length;
        assert_eq!(
            validate_parquet_schema(
                &metadata(1, vec![column]),
                &context,
                &entry(FileContentKind::Data),
                None
            )
            .is_ok(),
            valid,
            "{kind}"
        );
    }
}

#[test]
fn trusted_history_retains_dropped_fields_and_parent_identity_is_checked() {
    let old = context(json!([field(1, "old", json!("int"))]));
    let current = context(json!([]));
    let metadata = metadata(1, vec![node(Some(1), "old", Some(1), 0)]);
    assert!(validate_parquet_schema(&metadata, &current, &entry(FileContentKind::Data), None).is_err());
    assert!(validate_parquet_schema(
        &metadata,
        &current.with_schema_history(&[old]).unwrap(),
        &entry(FileContentKind::Data),
        None
    )
    .is_ok());
    let context = context(json!([field(
        2,
        "parent",
        json!({"type":"struct","fields":[field(1,"child",json!("int"))]})
    )]));
    assert!(validate_parquet_schema(&metadata, &context, &entry(FileContentKind::Data), None).is_err());
}

#[test]
fn name_mapping_is_explicit_bounded_and_never_overrides_a_physical_id() {
    let context = context(json!([field(7, "renamed", json!("long"))]));
    let mut metadata = metadata(1, vec![node(None, "old", Some(1), 0)]);
    let mapping = ParquetFieldMapping::from([(vec!["old".into()], 7)]);
    assert!(
        validate_parquet_schema(&metadata, &context, &entry(FileContentKind::Data), Some(&mapping)).is_ok()
    );
    assert!(validate_parquet_schema(&metadata, &context, &entry(FileContentKind::Data), None).is_err());
    metadata.schema[1].field_id = Some(8);
    assert!(
        validate_parquet_schema(&metadata, &context, &entry(FileContentKind::Data), Some(&mapping)).is_err()
    );
    metadata.schema[1].field_id = None;
    let fallback = self::context(json!([field(1, "unrelated_name", json!("long"))]));
    assert!(validate_parquet_schema(&metadata, &fallback, &entry(FileContentKind::Data), None).is_ok());
}

#[test]
fn legacy_annotations_and_unknown_columns_do_not_force_native_writer_layouts() {
    let context = context(json!([
        field(1, "text", json!("string")),
        field(2, "ignored", json!("unknown"))
    ]));
    let mut text = node(Some(1), "text", Some(6), 0);
    text.converted_type = Some(0);
    let metadata = metadata(2, vec![text, node(Some(2), "ignored", Some(2), 0)]);
    assert!(validate_parquet_schema(&metadata, &context, &entry(FileContentKind::Data), None).is_ok());
}

#[test]
fn required_columns_need_initial_defaults_only_when_the_parent_is_materialized() {
    let missing = metadata(0, vec![]);
    let with_default =
        context(json!([{"id":1,"name":"new","type":"long","required":true,"initial-default":42}]));
    assert!(validate_parquet_schema(&missing, &with_default, &entry(FileContentKind::Data), None).is_ok());
    let required = context(json!([{"id":1,"name":"new","type":"long","required":true}]));
    assert!(validate_parquet_schema(&missing, &required, &entry(FileContentKind::Data), None).is_err());
    let optional_parent = context(json!([field(
        1,
        "parent",
        json!({"type":"struct","fields":[{"id":2,"name":"child","type":"long","required":true}]})
    )]));
    assert!(validate_parquet_schema(&missing, &optional_parent, &entry(FileContentKind::Data), None).is_ok());
    let present_parent = metadata(1, vec![node(Some(1), "parent", None, 0)]);
    assert!(validate_parquet_schema(
        &present_parent,
        &optional_parent,
        &entry(FileContentKind::Data),
        None
    )
    .is_err());
}
