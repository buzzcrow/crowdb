#[path = "common/table_metadata.rs"]
#[allow(dead_code)]
mod fixture;

use crowdb_access_iceberg::{manifest::PrimitiveType, table::TableMetadataError};
use serde_json::json;

#[test]
fn contexts_select_historical_schema_and_spec_not_current_definitions() {
    let mut value = fixture::metadata(2);
    value["last-column-id"] = json!(2);
    value["schemas"].as_array_mut().unwrap().push(json!({
        "type":"struct","schema-id":1,"fields":[{"id":2,"name":"new","type":"string","required":false}]
    }));
    value["current-schema-id"] = json!(1);
    value["partition-specs"].as_array_mut().unwrap().push(json!({
        "spec-id":1,"fields":[{"field-id":1000,"source-id":1,"name":"old","transform":"identity"}]
    }));
    value["last-partition-id"] = json!(1000);
    let document = fixture::parse(&value).unwrap();
    let historical = document.manifest_context(0, 1, &[], 1000).unwrap();
    assert_eq!(historical.schema_id(), 0);
    assert_eq!(historical.spec_id(), 1);
    assert_eq!(historical.partitions()[0].result, Some(PrimitiveType::Long));
    assert!(document.manifest_context(1, 1, &[], 1000).is_err());
    let current = document.manifest_context(1, 0, &[0], 1000).unwrap();
    assert!(current.field(1).is_none());
    assert_eq!(
        current.retained_field(1).unwrap().primitive,
        Some(PrimitiveType::Long)
    );
    assert_eq!(current.field(2).unwrap().primitive, Some(PrimitiveType::String));
}

#[test]
fn missing_history_and_context_work_fail_closed() {
    let document = fixture::parse(&fixture::metadata(3)).unwrap();
    for (schema, spec, history) in [(1, 0, vec![]), (0, 1, vec![]), (0, 0, vec![1]), (-1, 0, vec![])] {
        assert!(document.manifest_context(schema, spec, &history, 1000).is_err());
    }
    for work in [0, 1, 1_000_001] {
        assert!(matches!(
            document.manifest_context(0, 0, &[], work),
            Err(TableMetadataError::Bounds)
        ));
    }
    assert!(matches!(
        document.manifest_context(0, 0, &[0; 17], 1000),
        Err(TableMetadataError::Bounds)
    ));
}

#[test]
fn legacy_context_uses_implicit_partition_ids_and_explicit_schema_id() {
    let mut value = fixture::metadata(1);
    value.as_object_mut().unwrap().remove("schemas");
    value.as_object_mut().unwrap().remove("current-schema-id");
    value.as_object_mut().unwrap().remove("partition-specs");
    value["schema"]["schema-id"] = json!(7);
    value["partition-spec"] = json!([{"source-id":1,"name":"id","transform":"identity"}]);
    value["last-partition-id"] = json!(1000);
    let document = fixture::parse(&value).unwrap();
    let context = document.manifest_context(7, 0, &[], 1000).unwrap();
    assert_eq!(context.partitions()[0].id, 1000);
    assert!(document.manifest_context(0, 0, &[], 1000).is_err());
}
