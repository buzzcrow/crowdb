#[path = "common/table_metadata.rs"]
#[allow(dead_code)]
mod fixture;

use crowdb_access_iceberg::{manifest::ParquetFieldMapping, table::TableMetadataError};
use serde_json::{json, Value};

fn mapping(value: Value, work: usize) -> Result<Option<ParquetFieldMapping>, TableMetadataError> {
    let mut metadata = fixture::metadata(3);
    metadata["properties"]["schema.name-mapping.default"] = Value::String(value.to_string());
    drop(value);
    fixture::parse(&metadata)
        .unwrap()
        .parquet_field_mapping(fixture::limits(), work)
}

#[test]
fn literal_paths_aliases_and_historical_ids_are_not_flattened() {
    let result = mapping(
        json!([
            {"field-id":40,"names":["a.b","old"],"fields":[{"field-id":41,"names":["nested","old_nested"]}]},
            {"names":["imported"],"fields":[{"field-id":42,"names":["child"]}]}
        ]),
        50_000,
    )
    .unwrap()
    .unwrap();
    assert_eq!(result.get(&vec!["a.b".into()]), Some(&40));
    assert_eq!(result.get(&vec!["a.b".into(), "nested".into()]), Some(&41));
    assert_eq!(result.get(&vec!["old".into(), "old_nested".into()]), Some(&41));
    assert_eq!(result.get(&vec!["imported".into(), "child".into()]), Some(&42));
    assert!(!result.contains_key(&vec!["a".into(), "b".into()]));
    assert!(!result.contains_key(&vec!["imported".into()]));
}

#[test]
fn sdk_unsafe_but_structurally_valid_mappings_are_rejected_at_selected_use() {
    for value in [
        json!([{"field-id":1,"names":["a.b"]},{"field-id":2,"names":["a"],"fields":[{"field-id":3,"names":["b"]}]}]),
        json!([{"names":["a"]},{"names":["b"]}]),
        json!([{"field-id":1,"fields":[{"field-id":2,"names":["a.b"]},{"field-id":3,"names":["a"],"fields":[{"field-id":4,"names":["b"]}]}]}]),
    ] {
        assert!(mapping(value, 50_000).is_err());
    }
}

#[test]
fn alias_cartesian_expansion_is_bounded() {
    let mut value = json!([{"field-id":10,"names":["a","b","c","d"]}]);
    for id in (1..10).rev() {
        value = json!([{"field-id":id,"names":["a","b","c","d"],"fields":value}]);
    }
    assert!(matches!(mapping(value, 50_000), Err(TableMetadataError::Bounds)));
    assert!(matches!(
        mapping(json!([{"field-id":1,"names":["a"]}]), 1),
        Err(TableMetadataError::Bounds)
    ));
    let document = fixture::parse(&fixture::metadata(3)).unwrap();
    assert!(document
        .parquet_field_mapping(fixture::limits(), 10)
        .unwrap()
        .is_none());
}
