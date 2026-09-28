#[path = "common/manifest_entry.rs"]
mod fixture;
use crowdb_access_iceberg::file::{AvroDatumLimits, AvroSchema};
use crowdb_access_iceberg::manifest::{
    ManifestContent, ManifestContext, ManifestEntryProjection, ManifestEntryState, ManifestVersion,
    PartitionValue,
};
use fixture::{table, TestManifestEntry};
use serde_json::{json, Value};

fn context(kind: &str, transform: Option<&str>) -> ManifestContext {
    let spec = transform.map_or_else(
        || json!([]),
        |transform| json!([{"field-id":1000,"source-id":3,"name":"p","transform":transform}]),
    );
    ManifestContext::parse(ManifestVersion::V3,0,0,&serde_json::to_vec(&json!({"type":"struct","schema-id":0,"fields":[{"id":3,"name":"value","required":false,"type":kind}]})).unwrap(),&serde_json::to_vec(&spec).unwrap()).unwrap()
}

fn decode(
    fixture: &TestManifestEntry,
    context: &ManifestContext,
    manifest_kind: ManifestContent,
) -> Result<
    crowdb_access_iceberg::manifest::ManifestScalarEntry,
    crowdb_access_iceberg::manifest::ManifestEntryError,
> {
    let schema = fixture.schema();
    let projection = ManifestEntryProjection::with_context(&schema, ManifestVersion::V3, table(), context)?;
    let bytes = fixture.bytes();
    let mut state = ManifestEntryState::new(
        ManifestVersion::V3,
        table(),
        manifest_kind,
        99,
        9,
        if manifest_kind == ManifestContent::Data {
            Some(100)
        } else {
            None
        },
    )
    .unwrap();
    let before = state.next_row_id();
    let result = projection
        .records(
            &bytes,
            1,
            AvroDatumLimits {
                depth: 64,
                values: 10000,
                value_bytes: 1024 * 1024,
            },
            &mut state,
        )?
        .next_entry();
    if result.is_err() {
        assert_eq!(state.next_row_id(), before);
    }
    result.map(Option::unwrap)
}

#[test]
fn partition_values_follow_spec_ids_and_transform_domains_before_inheritance() {
    let mut fixture = TestManifestEntry::new(ManifestVersion::V3);
    fixture
        .partition_fields
        .push(json!({"field-id":1000,"name":"renamed","type":["null","int"]}));
    fixture.partition_bytes = vec![2, 6];
    let context = context("long", Some("bucket[8]"));
    assert_eq!(
        decode(&fixture, &context, ManifestContent::Data)
            .unwrap()
            .file
            .partition,
        Some(vec![(1000, PartitionValue::Int(3))])
    );
    fixture.partition_bytes = vec![2, 16];
    assert!(decode(&fixture, &context, ManifestContent::Data).is_err());
    fixture.partition_bytes = vec![0];
    assert_eq!(
        decode(&fixture, &context, ManifestContent::Data)
            .unwrap()
            .file
            .partition,
        Some(vec![(1000, PartitionValue::Null)])
    );
}

#[test]
fn partition_layout_rejects_extra_missing_and_incompatible_logical_fields() {
    let context = context("timestamp", Some("identity"));
    for kind in [
        json!("long"),
        json!({"type":"long","logicalType":"timestamp-micros","adjust-to-utc":true}),
        json!("int"),
    ] {
        let mut fixture = TestManifestEntry::new(ManifestVersion::V3);
        fixture
            .partition_fields
            .push(json!({"field-id":1000,"name":"p","type":["null",kind]}));
        let schema = fixture.schema();
        assert!(
            ManifestEntryProjection::with_context(&schema, ManifestVersion::V3, table(), &context).is_err()
        );
    }
    let fixture = TestManifestEntry::new(ManifestVersion::V3);
    assert!(
        ManifestEntryProjection::with_context(&fixture.schema(), ManifestVersion::V3, table(), &context)
            .is_err()
    );
}

#[test]
fn unknown_transforms_preserve_values_and_void_requires_null() {
    let mut fixture = TestManifestEntry::new(ManifestVersion::V3);
    fixture
        .partition_fields
        .push(json!({"field-id":1000,"name":"p","type":["null","string"]}));
    fixture.partition_bytes = vec![2, 2, b'x'];
    assert_eq!(
        decode(&fixture, &context("long", Some("future")), ManifestContent::Data)
            .unwrap()
            .file
            .partition,
        Some(vec![(1000, PartitionValue::String("x".into()))])
    );
    fixture.partition_fields[0]["type"] = json!(["null", "int"]);
    fixture.partition_bytes = vec![2, 0];
    assert!(decode(&fixture, &context("long", Some("void")), ManifestContent::Data).is_err());
    fixture.partition_bytes = vec![0];
    assert!(decode(&fixture, &context("long", Some("void")), ManifestContent::Data).is_ok());
}

#[test]
fn numeric_bounds_compare_values_and_accept_historical_promoted_encodings() {
    for (kind, lower, upper, valid) in [
        (
            "int",
            (-2_i32).to_le_bytes().to_vec(),
            1_i32.to_le_bytes().to_vec(),
            true,
        ),
        (
            "int",
            2_i32.to_le_bytes().to_vec(),
            1_i32.to_le_bytes().to_vec(),
            false,
        ),
        (
            "long",
            (-2_i32).to_le_bytes().to_vec(),
            1_i64.to_le_bytes().to_vec(),
            true,
        ),
        (
            "double",
            (-2_f32).to_le_bytes().to_vec(),
            1_f64.to_le_bytes().to_vec(),
            true,
        ),
        (
            "double",
            f64::NAN.to_le_bytes().to_vec(),
            1_f64.to_le_bytes().to_vec(),
            false,
        ),
        ("decimal(3,1)", vec![255], vec![1], true),
        ("decimal(3,1)", vec![3, 232], vec![3, 233], false),
        ("string", vec![255], vec![255], false),
        ("uuid", vec![0; 15], vec![0; 16], false),
    ] {
        let mut fixture = TestManifestEntry::new(ManifestVersion::V3);
        fixture.file.extend([
            (125, "bytes-map", json!([[3, lower]])),
            (128, "bytes-map", json!([[3, upper]])),
        ]);
        assert_eq!(
            decode(&fixture, &context(kind, None), ManifestContent::Data).is_ok(),
            valid,
            "{kind}"
        );
    }
}

#[test]
fn metrics_and_equality_ids_require_eligible_schema_fields() {
    let mut fixture = TestManifestEntry::new(ManifestVersion::V3);
    fixture.file.push((137, "long-map", json!([[3, 0]])));
    assert!(decode(&fixture, &context("long", None), ManifestContent::Data).is_err());
    assert!(decode(&fixture, &context("double", None), ManifestContent::Data).is_ok());
    fixture.set(137, json!([[4, 0]]));
    assert!(decode(&fixture, &context("double", None), ManifestContent::Data).is_err());
    fixture.file.retain(|field| field.0 != 137);
    fixture.set(134, json!(2));
    fixture.set(135, json!([3]));
    assert!(decode(&fixture, &context("double", None), ManifestContent::Deletes).is_err());
    assert!(decode(&fixture, &context("long", None), ManifestContent::Deletes).is_ok());
}

#[test]
fn null_partition_record_cannot_masquerade_as_an_all_null_tuple() {
    let fixture = TestManifestEntry::new(ManifestVersion::V3);
    let mut schema: Value = serde_json::from_slice(&fixture.schema_bytes()).unwrap();
    let fields = schema["fields"].as_array_mut().unwrap();
    let file = fields.iter_mut().find(|field| field["field-id"] == 2).unwrap();
    let fields = file["type"][1]["fields"].as_array_mut().unwrap();
    let partition = fields.iter_mut().find(|field| field["field-id"] == 102).unwrap();
    partition["type"] = json!(["null", partition["type"].clone()]);
    let schema = AvroSchema::parse(&serde_json::to_vec(&schema).unwrap()).unwrap();
    let context = context("long", None);
    let projection =
        ManifestEntryProjection::with_context(&schema, ManifestVersion::V3, table(), &context).unwrap();
    let mut bytes = fixture.bytes();
    bytes.push(0);
    let mut state = ManifestEntryState::new(
        ManifestVersion::V3,
        table(),
        ManifestContent::Data,
        99,
        9,
        Some(100),
    )
    .unwrap();
    assert!(projection
        .records(
            &bytes,
            1,
            AvroDatumLimits {
                depth: 64,
                values: 10000,
                value_bytes: 1024
            },
            &mut state
        )
        .unwrap()
        .next_entry()
        .is_err());
    assert_eq!(state.next_row_id(), Some(100));
}

#[test]
fn geography_bounds_allow_dateline_crossing_and_validate_coordinate_domains() {
    for (lower, upper, valid) in [
        (vec![170.0_f64, -20.0], vec![-170.0_f64, 20.0], true),
        (vec![181.0, -20.0], vec![-170.0, 20.0], false),
        (vec![0.0, 20.0], vec![1.0, -20.0], false),
    ] {
        let lower: Vec<_> = lower.into_iter().flat_map(f64::to_le_bytes).collect();
        let upper: Vec<_> = upper.into_iter().flat_map(f64::to_le_bytes).collect();
        let mut fixture = TestManifestEntry::new(ManifestVersion::V3);
        fixture.file.extend([
            (125, "bytes-map", json!([[3, lower]])),
            (128, "bytes-map", json!([[3, upper]])),
        ]);
        assert_eq!(
            decode(&fixture, &context("geography", None), ManifestContent::Data).is_ok(),
            valid
        );
    }
}

#[test]
fn historical_dropped_field_metrics_and_position_delete_reserved_ids_are_supported() {
    let prior = context("long", None);
    let current = ManifestContext::parse(
        ManifestVersion::V3,
        0,
        0,
        br#"{"type":"struct","schema-id":0,"fields":[]}"#,
        b"[]",
    )
    .unwrap()
    .with_schema_history(&[prior])
    .unwrap();
    let mut fixture = TestManifestEntry::new(ManifestVersion::V3);
    fixture
        .file
        .push((125, "bytes-map", json!([[3, (-1_i32).to_le_bytes()]])));
    assert!(decode(&fixture, &current, ManifestContent::Data).is_ok());
    fixture.set(125, json!([[2_147_483_545_i32, 1_i64.to_le_bytes()]]));
    assert!(decode(&fixture, &current, ManifestContent::Data).is_err());
    fixture.set(134, json!(1));
    assert!(decode(&fixture, &current, ManifestContent::Deletes).is_ok());
}

#[test]
fn truncate_partition_domains_and_decimal_writer_annotations_are_validated() {
    let mut fixture = TestManifestEntry::new(ManifestVersion::V3);
    fixture
        .partition_fields
        .push(json!({"field-id":1000,"name":"p","type":["null","int"]}));
    fixture.partition_bytes = vec![2, 19];
    assert!(decode(
        &fixture,
        &context("int", Some("truncate[10]")),
        ManifestContent::Data
    )
    .is_ok());
    fixture.partition_bytes = vec![2, 17];
    assert!(decode(
        &fixture,
        &context("int", Some("truncate[10]")),
        ManifestContent::Data
    )
    .is_err());
    fixture.partition_fields[0]["type"] = json!(["null",{"type":"fixed","name":"amount","size":2,"logicalType":"decimal","precision":3,"scale":1}]);
    fixture.partition_bytes = vec![2, 0, 10];
    assert!(decode(
        &fixture,
        &context("decimal(3,1)", Some("identity")),
        ManifestContent::Data
    )
    .is_ok());
    fixture.partition_fields[0]["type"][1]["scale"] = json!(2);
    assert!(decode(
        &fixture,
        &context("decimal(3,1)", Some("identity")),
        ManifestContent::Data
    )
    .is_err());
}

#[test]
fn equality_fields_nested_in_collections_are_rejected() {
    let context=ManifestContext::parse(ManifestVersion::V3,0,0,br#"{"type":"struct","schema-id":0,"fields":[{"id":3,"name":"items","required":false,"type":{"type":"list","element-id":4,"element-required":false,"element":"long"}}]}"#,b"[]").unwrap();
    let mut fixture = TestManifestEntry::new(ManifestVersion::V3);
    fixture.set(134, json!(2));
    fixture.set(135, json!([4]));
    assert!(decode(&fixture, &context, ManifestContent::Deletes).is_err());
}
