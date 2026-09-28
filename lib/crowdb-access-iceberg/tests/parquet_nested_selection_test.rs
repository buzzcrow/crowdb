#[path = "common/selected_parquet.rs"]
mod fixture;

use crowdb_access_iceberg::file::ParquetLogicalType as Logical;
use crowdb_access_iceberg::manifest::{validate_parquet_schema, FileContentKind, ParquetFieldMapping};
use fixture::{context, entry, field, metadata, node};
use serde_json::json;

#[test]
fn lists_accept_native_wrappers_and_legacy_repeated_elements() {
    let context = context(json!([field(
        1,
        "items",
        json!({"type":"list","element-id":2,"element-required":true,"element":"long"})
    )]));
    for legacy in [false, true] {
        let mut list = node(Some(1), "items", None, 1);
        list.logical_type = Some(Logical::List);
        let mut element = node(Some(2), "item", Some(1), 0);
        element.repetition = Some(if legacy { 2 } else { 0 });
        let mut nodes = vec![list];
        if !legacy {
            let mut wrapper = node(None, "list", None, 1);
            wrapper.repetition = Some(2);
            nodes.push(wrapper);
        }
        nodes.push(element);
        let mut metadata = metadata(1, nodes);
        assert!(validate_parquet_schema(&metadata, &context, &entry(FileContentKind::Data), None).is_ok());
        metadata.schema[2].repetition = Some(1);
        assert!(validate_parquet_schema(&metadata, &context, &entry(FileContentKind::Data), None).is_err());
    }
}

#[test]
fn collection_mapping_uses_logical_names_not_wrapper_names() {
    let context = context(json!([field(
        7,
        "items",
        json!({"type":"list","element-id":8,"element-required":false,"element":"long"})
    )]));
    let mut list = node(None, "old_list", None, 1);
    list.converted_type = Some(3);
    let mut wrapper = node(None, "list", None, 1);
    wrapper.repetition = Some(2);
    let metadata = metadata(
        1,
        vec![list, wrapper, node(None, "arbitrary_element_name", Some(2), 0)],
    );
    let mapping = ParquetFieldMapping::from([
        (vec!["old_list".into()], 7),
        (vec!["old_list".into(), "element".into()], 8),
    ]);
    let selected =
        validate_parquet_schema(&metadata, &context, &entry(FileContentKind::Data), Some(&mapping)).unwrap();
    assert_eq!(selected.field_index(8), Some(3));
}

#[test]
fn map_keys_are_required_and_key_value_identity_cannot_be_swapped() {
    let context = context(json!([field(
        1,
        "map",
        json!({"type":"map","key-id":2,"key":"long","value-id":3,"value":"long","value-required":false})
    )]));
    let mut map = node(Some(1), "map", None, 1);
    map.logical_type = Some(Logical::Map);
    let mut pairs = node(None, "key_value", None, 2);
    pairs.repetition = Some(2);
    let mut key = node(Some(2), "key", Some(2), 0);
    key.repetition = Some(0);
    let mut metadata = metadata(1, vec![map, pairs, key, node(Some(3), "value", Some(2), 0)]);
    assert!(validate_parquet_schema(&metadata, &context, &entry(FileContentKind::Data), None).is_ok());
    metadata.schema[3].repetition = Some(1);
    assert!(validate_parquet_schema(&metadata, &context, &entry(FileContentKind::Data), None).is_err());
    metadata.schema[3].repetition = Some(0);
    metadata.schema[3].field_id = Some(3);
    metadata.schema[4].field_id = Some(2);
    assert!(validate_parquet_schema(&metadata, &context, &entry(FileContentKind::Data), None).is_err());
}

#[test]
fn equality_ids_must_be_present_unique_eligible_noncollection_leaves() {
    let context = context(json!([field(
        1,
        "parent",
        json!({"type":"struct","fields":[field(2,"id",json!("long")),field(3,"float",json!("float"))]})
    )]));
    let metadata = metadata(
        1,
        vec![
            node(Some(1), "parent", None, 2),
            node(Some(2), "id", Some(2), 0),
            node(Some(3), "float", Some(4), 0),
        ],
    );
    for (ids, valid) in [
        (vec![2], true),
        (vec![], false),
        (vec![2, 2], false),
        (vec![1], false),
        (vec![3], false),
        (vec![4], false),
    ] {
        let mut entry = entry(FileContentKind::EqualityDeletes);
        entry.file.equality_ids = Some(ids);
        assert_eq!(
            validate_parquet_schema(&metadata, &context, &entry, None).is_ok(),
            valid
        );
    }
}

#[test]
fn position_delete_reserved_columns_and_optional_row_projection_are_checked() {
    let context = context(json!([field(1, "payload", json!("long"))]));
    let mut path = node(Some(2_147_483_546), "file_path", Some(6), 0);
    path.logical_type = Some(Logical::String);
    path.repetition = Some(0);
    let mut pos = node(Some(2_147_483_545), "pos", Some(2), 0);
    pos.repetition = Some(0);
    let mut row = node(Some(2_147_483_544), "row", None, 1);
    row.repetition = Some(0);
    let mut metadata = metadata(3, vec![path, pos, row, node(Some(1), "old_name", Some(2), 0)]);
    let entry = entry(FileContentKind::PositionDeletes);
    assert!(validate_parquet_schema(&metadata, &context, &entry, None).is_ok());
    metadata.schema[3].repetition = Some(1);
    assert!(validate_parquet_schema(&metadata, &context, &entry, None).is_err());
    metadata.schema[3].repetition = Some(0);
    metadata.schema[2].physical_type = Some(1);
    assert!(validate_parquet_schema(&metadata, &context, &entry, None).is_err());
}
