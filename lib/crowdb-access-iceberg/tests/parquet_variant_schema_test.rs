#[path = "common/selected_parquet.rs"]
mod fixture;
use crowdb_access_iceberg::file::ParquetLogicalType as Logical;
use crowdb_access_iceberg::manifest::{validate_parquet_schema, FileContentKind};
use fixture::{context, entry, field, metadata, node};
use serde_json::json;

#[test]
fn variant_schema_accepts_unshredded_primitive_object_and_array_layouts() {
    let context = context(json!([field(1, "variant", json!("variant"))]));
    for kind in 0..4 {
        let mut variant = node(Some(1), "variant", None, if kind == 0 { 2 } else { 3 });
        variant.logical_type = Some(Logical::Variant {
            specification_version: Some(1),
        });
        let mut meta = node(None, "metadata", Some(6), 0);
        meta.repetition = Some(0);
        let mut nodes = vec![variant, meta, node(None, "value", Some(6), 0)];
        match kind {
            0 => {}
            1 => nodes.push(node(None, "typed_value", Some(2), 0)),
            2 => {
                nodes.push(node(None, "typed_value", None, 1));
                let mut property = node(None, "property", None, 1);
                property.repetition = Some(0);
                nodes.push(property);
                nodes.push(node(None, "value", Some(6), 0));
            }
            _ => {
                let mut typed = node(None, "typed_value", None, 1);
                typed.logical_type = Some(Logical::List);
                nodes.push(typed);
                let mut list = node(None, "list", None, 1);
                list.repetition = Some(2);
                nodes.push(list);
                let mut element = node(None, "element", None, 1);
                element.repetition = Some(0);
                nodes.push(element);
                nodes.push(node(None, "value", Some(6), 0));
            }
        }
        let mut metadata = metadata(1, nodes);
        assert!(validate_parquet_schema(&metadata, &context, &entry(FileContentKind::Data), None).is_ok());
        metadata.schema[2].field_id = Some(2);
        assert!(validate_parquet_schema(&metadata, &context, &entry(FileContentKind::Data), None).is_err());
        metadata.schema[2].field_id = None;
        metadata.schema[2].repetition = Some(1);
        assert!(validate_parquet_schema(&metadata, &context, &entry(FileContentKind::Data), None).is_err());
    }
}
