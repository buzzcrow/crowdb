use crowdb_access_iceberg::file::{ContentFormat, ParquetMetadata, ParquetSchemaElement, TableLocation};
use crowdb_access_iceberg::key::{CatalogId, TableId};
use crowdb_access_iceberg::manifest::{
    EntryStatus, FileContentKind, InheritedEntry, ManifestContext, ManifestEntry, ManifestFileFields,
    ManifestMetrics, ManifestScalarEntry, ManifestVersion,
};
use serde_json::{json, Value};

pub fn context(fields: impl Into<Value>) -> ManifestContext {
    ManifestContext::parse(
        ManifestVersion::V3,
        1,
        0,
        &serde_json::to_vec(&json!({"type":"struct","schema-id":1,"fields":fields.into()})).unwrap(),
        b"[]",
    )
    .unwrap()
}

pub fn field(id: i32, name: &str, kind: impl Into<Value>) -> Value {
    json!({"id":id,"name":name,"type":kind.into(),"required":false})
}

pub fn node(
    id: Option<i32>,
    name: &str,
    physical_type: Option<i32>,
    children: usize,
) -> ParquetSchemaElement {
    ParquetSchemaElement {
        name: name.into(),
        field_id: id,
        physical_type,
        repetition: Some(1),
        children,
        type_length: None,
        converted_type: None,
        scale: None,
        precision: None,
        logical_type: None,
    }
}

pub fn metadata(children: usize, nodes: Vec<ParquetSchemaElement>) -> ParquetMetadata {
    let mut root = node(None, "root", None, children);
    root.repetition = None;
    let mut schema = vec![root];
    schema.extend(nodes);
    ParquetMetadata {
        groups: vec![],
        rows: 0,
        row_groups: 0,
        schema,
    }
}

pub fn entry(content: FileContentKind) -> ManifestScalarEntry {
    ManifestScalarEntry {
        entry: ManifestEntry {
            status: EntryStatus::Added,
            content,
            snapshot_id: None,
            data_sequence: None,
            file_sequence: None,
            first_row_id: None,
            record_count: 0,
        },
        file: ManifestFileFields {
            location: TableLocation {
                catalog: CatalogId::random(),
                table: TableId::random(),
            }
            .file("data/file.parquet")
            .unwrap(),
            format: ContentFormat::Parquet,
            length: 0,
            sort_order_id: None,
            referenced_data_file: None,
            deletion_vector: None,
            equality_ids: None,
            metrics: ManifestMetrics::default(),
            partition: Some(vec![]),
        },
        inherited: InheritedEntry {
            snapshot_id: 1,
            data_sequence: 1,
            file_sequence: 1,
            first_row_id: None,
        },
    }
}
