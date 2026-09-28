use crowdb_access_iceberg::file::{
    AvroContainerError, AvroDatumLimits, AvroFieldPath, AvroProjection, AvroScalar, AvroScalarType,
    AvroSchema,
};
use serde_json::json;

fn limits() -> AvroDatumLimits {
    AvroDatumLimits {
        depth: 64,
        values: 100,
        value_bytes: 1024,
    }
}

fn schema() -> AvroSchema {
    AvroSchema::parse(
        br#"{"type":"record","name":"Entry","fields":[
        {"name":"renamed_status","field-id":0,"type":"int"},
        {"name":"renamed_file","field-id":2,"type":["null",{"type":"record","name":"File","fields":[
            {"name":"renamed_path","field-id":100,"type":"string"},
            {"name":"renamed_count","field-id":103,"type":"long"},
            {"name":"ignored","field-id":108,"type":{"type":"array","items":"null"}}
        ]}]}
    ]}"#,
    )
    .unwrap()
}

fn required(ids: &[i32]) -> AvroFieldPath<'_> {
    AvroFieldPath { ids, required: true }
}

#[test]
fn nested_projection_selects_borrowed_scalars_once_and_null_parents_clear_all_child_slots() {
    let schema = schema();
    let projection = AvroProjection::paths(
        &schema,
        &[
            required(&[2, 103]),
            required(&[0]),
            required(&[2, 100]),
            AvroFieldPath {
                ids: &[2, 142],
                required: false,
            },
        ],
    )
    .unwrap();
    assert_eq!(
        projection.field_types(),
        &[
            Some(AvroScalarType::Long),
            Some(AvroScalarType::Int),
            Some(AvroScalarType::String),
            None
        ]
    );
    let bytes = [2, 2, 2, b'a', 6, 4, 0, 4, 0];
    schema
        .validate_block(
            &bytes,
            2,
            AvroDatumLimits {
                values: 13,
                ..limits()
            },
        )
        .unwrap();
    let mut records = projection
        .records(
            &bytes,
            2,
            AvroDatumLimits {
                values: 13,
                ..limits()
            },
        )
        .unwrap();
    let values = records.next_record().unwrap().unwrap();
    assert_eq!(
        values,
        vec![
            AvroScalar::Long(3),
            AvroScalar::Int(1),
            AvroScalar::String("a"),
            AvroScalar::Null
        ]
    );
    let AvroScalar::String(path) = values[2] else {
        panic!("missing path")
    };
    assert_eq!(path.as_ptr(), bytes[3..].as_ptr());
    assert_eq!(
        records.next_record().unwrap().unwrap(),
        vec![
            AvroScalar::Null,
            AvroScalar::Int(2),
            AvroScalar::Null,
            AvroScalar::Null
        ]
    );
    assert!(records.next_record().unwrap().is_none());
}

#[test]
fn nested_cursor_enforces_the_same_work_depth_and_skipped_collection_checks_as_binary_validation() {
    let schema = schema();
    let projection = AvroProjection::paths(&schema, &[required(&[2, 100])]).unwrap();
    let bytes = [2, 2, 2, b'a', 6, 4, 0];
    for limits in [
        AvroDatumLimits {
            values: 8,
            ..limits()
        },
        AvroDatumLimits { depth: 4, ..limits() },
    ] {
        assert!(matches!(
            schema.validate_block(&bytes, 1, limits),
            Err(AvroContainerError::Bounds)
        ));
        let mut records = projection.records(&bytes, 1, limits).unwrap();
        assert!(matches!(records.next_record(), Err(AvroContainerError::Bounds)));
        assert!(matches!(records.next_record(), Err(AvroContainerError::Failed)));
    }
    for bytes in [
        vec![2, 4],
        vec![2, 2, 2, 255, 6, 0],
        vec![2, 2, 2, b'a', 6, 1, 2, 0],
        vec![2, 0, 0],
    ] {
        assert!(projection
            .records(&bytes, 1, limits())
            .unwrap()
            .next_record()
            .is_err());
    }
}

#[test]
fn nested_paths_reject_missing_required_fields_duplicates_scalar_prefixes_and_collection_descent() {
    let schema = schema();
    for paths in [
        vec![required(&[])],
        vec![required(&[2, 999])],
        vec![required(&[-1])],
        vec![required(&[2, 100]), required(&[2, 100])],
        vec![required(&[2]), required(&[2, 100])],
        vec![required(&[2, 108, 100])],
        vec![required(&[2, 100, 101])],
    ] {
        assert!(AvroProjection::paths(&schema, &paths).is_err());
    }
    assert!(AvroProjection::paths(&schema, &[required(&[2; 17])]).is_err());
    let projection = AvroProjection::paths(
        &schema,
        &[AvroFieldPath {
            ids: &[999, 100],
            required: false,
        }],
    )
    .unwrap();
    assert_eq!(
        projection
            .records(&[2, 0], 1, limits())
            .unwrap()
            .next_record()
            .unwrap()
            .unwrap(),
        vec![AvroScalar::Null]
    );
    let malformed = AvroSchema::parse(
        br#"{"type":"record","name":"R","fields":[
        {"name":"file","field-id":2,"type":{"type":"record","name":"F","fields":[
            {"name":"a","field-id":100,"type":"long"},{"name":"b","field-id":100,"type":"long"}
        ]}}
    ]}"#,
    )
    .unwrap();
    assert!(AvroProjection::paths(&malformed, &[required(&[2, 100])]).is_err());
}

#[test]
fn shared_named_record_layouts_cannot_expand_the_compiled_projection_without_a_bound() {
    let child: Vec<_> = (0..300)
        .map(|index| json!({"name":format!("child{index}"),"field-id":100+index,"type":"long"}))
        .collect();
    let mut fields =
        vec![json!({"name":"first","field-id":0,"type":{"type":"record","name":"Shared","fields":child}})];
    fields
        .extend((1..64).map(|index| json!({"name":format!("root{index}"),"field-id":index,"type":"Shared"})));
    let schema = AvroSchema::parse(
        &serde_json::to_vec(&json!({"type":"record","name":"Root","fields":fields})).unwrap(),
    )
    .unwrap();
    let ids: Vec<_> = (0..64).map(|index| [index, 100]).collect();
    let paths: Vec<_> = ids.iter().map(|ids| required(ids)).collect();
    assert!(matches!(
        AvroProjection::paths(&schema, &paths),
        Err(AvroContainerError::Bounds)
    ));
    AvroProjection::paths(&schema, &paths[..32]).unwrap();
}
