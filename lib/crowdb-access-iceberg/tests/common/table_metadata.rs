use crowdb_access_iceberg::{
    file::TableLocation,
    key::{CatalogId, FileId, NamespaceId, TableId},
    table::{TableHead, TableLifecycle, TableMetadataDocument, TableMetadataError, TableMetadataLimits},
};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

pub fn table() -> TableLocation {
    TableLocation {
        catalog: CatalogId::from_bytes(&[1; 16]).unwrap(),
        table: TableId::from_bytes(&[2; 16]).unwrap(),
    }
}

pub fn limits() -> TableMetadataLimits {
    TableMetadataLimits {
        bytes: 1024 * 1024,
        values: 50_000,
        depth: 32,
        string_bytes: 512 * 1024,
        collection_entries: 1000,
    }
}

pub fn metadata(version: u8) -> Value {
    let schema =
        json!({"type":"struct","schema-id":0,"fields":[{"id":1,"name":"id","type":"long","required":true}]});
    let mut value = json!({
        "format-version":version,"table-uuid":"12345678-1234-1234-1234-123456789abc",
        "location":table().to_string(),"last-updated-ms":1000,"last-column-id":1,
        "schemas":[schema],"current-schema-id":0,"partition-specs":[{"spec-id":0,"fields":[]}],
        "default-spec-id":0,"last-partition-id":999,"sort-orders":[{"order-id":0,"fields":[]}],
        "default-sort-order-id":0,"properties":{},"current-snapshot-id":null,"snapshots":[],"refs":{}
    });
    if version == 1 {
        value["schema"] = schema;
        value["partition-spec"] = json!([]);
    } else {
        value["last-sequence-number"] = json!(0);
    }
    if version == 3 {
        value["next-row-id"] = json!(0);
    }
    value
}

pub fn head(bytes: &[u8], version: u8, uuid: Option<uuid::Uuid>) -> TableHead {
    TableHead {
        catalog: table().catalog,
        table: table().table,
        namespace: NamespaceId::random(),
        name: "events".into(),
        name_epoch: 1,
        lifecycle: TableLifecycle::Ready,
        generation: 1,
        metadata_file: FileId::random(),
        metadata_location: table().file("metadata/one.metadata.json").unwrap(),
        metadata_digest: Sha256::digest(bytes).into(),
        format_version: version,
        table_uuid: uuid,
        operation_fence: 1,
        pending_operation: None,
    }
}

pub fn parse(value: &Value) -> Result<TableMetadataDocument, TableMetadataError> {
    let bytes = serde_json::to_vec(value).unwrap();
    let head = head(
        &bytes,
        u8::try_from(value["format-version"].as_u64().unwrap()).unwrap(),
        value["table-uuid"]
            .as_str()
            .map(|value| uuid::Uuid::parse_str(value).unwrap()),
    );
    TableMetadataDocument::parse(bytes, &head, limits())
}

pub fn snapshot(id: i64, sequence: i64) -> Value {
    json!({"snapshot-id":id,"sequence-number":sequence,"timestamp-ms":1000,"schema-id":0,
        "manifest-list":table().file(&format!("metadata/snapshot-{id}.avro")).unwrap().to_string(),
        "summary":{"operation":"append"}})
}
