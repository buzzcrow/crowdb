use std::sync::Arc;

use crowdb_access_iceberg::{
    file::{ContentFormat, FileContent, FileIdentity, FileKind, FileRecord, FileTreeWriter, TableLocation},
    key::FileId,
};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

pub async fn graph(table: TableLocation, version: u8) -> (Value, Vec<FileRecord>, FileId) {
    let mut records = Vec::new();
    for (name, format) in [
        ("data/kept.parquet", ContentFormat::Parquet),
        ("data/deleted.parquet", ContentFormat::Parquet),
        ("data/dv.puffin", ContentFormat::Puffin),
        ("metadata/stat.puffin", ContentFormat::Puffin),
        ("metadata/part.parquet", ContentFormat::Parquet),
    ] {
        if name == "data/dv.puffin" && version != 3 {
            continue;
        }
        let owner = FileIdentity {
            table,
            file: FileId::random(),
        };
        let blocks = Arc::new(crate::blocks::TestBlocks::default());
        let mut writer = FileTreeWriter::new(blocks, owner, 128).unwrap();
        writer.push(b"opaque file payload").await.unwrap();
        let tree = writer.finish().await.unwrap();
        records.push(FileRecord {
            file: owner.file,
            location: table.file(name).unwrap(),
            kind: FileKind::Unbound,
            format,
            length: tree.length,
            digest: tree.digest,
            content: FileContent::Chunks { root: tree.root },
            hint: None,
        });
    }
    let deleted = records[1].file;
    let schema = json!({"type":"record","name":"entry","fields":[
        {"name":"status","field-id":0,"type":"int"},
        {"name":"data_file","field-id":2,"type":{"type":"record","name":"file","fields":[
            {"name":"file_path","field-id":100,"type":"string"},
            {"name":"referenced_data_file","field-id":143,"type":["null","string"]}
        ]}}
    ]});
    let mut bytes = Vec::new();
    let mut entries = vec![(0, "data/kept.parquet", None), (2, "data/deleted.parquet", None)];
    if version == 3 {
        entries.push((1, "data/dv.puffin", Some("data/kept.parquet")));
    }
    let count = i64::try_from(entries.len()).unwrap();
    for (status, path, referenced) in entries {
        long(&mut bytes, status);
        sized(&mut bytes, table.file(path).unwrap().to_string().as_bytes());
        long(&mut bytes, i64::from(referenced.is_some()));
        if let Some(path) = referenced {
            sized(&mut bytes, table.file(path).unwrap().to_string().as_bytes());
        }
    }
    records.push(inline(
        table,
        "metadata/manifest.avro",
        FileKind::Manifest,
        &ocf(&schema, count, &bytes),
    ));
    let schema = json!({"type":"record","name":"list","fields":[{"name":"manifest_path","field-id":500,"type":"string"}]});
    let mut bytes = Vec::new();
    sized(
        &mut bytes,
        table
            .file("metadata/manifest.avro")
            .unwrap()
            .to_string()
            .as_bytes(),
    );
    records.push(inline(
        table,
        "metadata/snapshot-1.avro",
        FileKind::ManifestList,
        &ocf(&schema, 1, &bytes),
    ));
    let mut document = crate::metadata::metadata(version);
    document["current-snapshot-id"] = json!(1);
    document["snapshots"] = json!([crate::metadata::snapshot(1, i64::from(version != 1))]);
    if version != 1 {
        document["last-sequence-number"] = json!(1);
    }
    document["refs"] =
        json!({"main":{"type":"branch","snapshot-id":1}, "keep":{"type":"tag","snapshot-id":1}});
    document["statistics"] = json!([{"snapshot-id":1,"statistics-path":table.file("metadata/stat.puffin").unwrap().to_string(),
        "file-size-in-bytes":19,"file-footer-size-in-bytes":0,"blob-metadata":[]}]);
    document["partition-statistics"] = json!([{"snapshot-id":1,"statistics-path":table.file("metadata/part.parquet").unwrap().to_string(),
        "file-size-in-bytes":19}]);
    (document, records, deleted)
}

fn inline(table: TableLocation, path: &str, kind: FileKind, bytes: &[u8]) -> FileRecord {
    FileRecord {
        file: FileId::random(),
        location: table.file(path).unwrap(),
        kind,
        format: ContentFormat::Avro,
        length: bytes.len() as u64,
        digest: Sha256::digest(bytes).into(),
        content: FileContent::select_inline(kind, bytes).unwrap(),
        hint: None,
    }
}

fn ocf(schema: &Value, count: i64, data: &[u8]) -> Vec<u8> {
    let mut bytes = b"Obj\x01".to_vec();
    long(&mut bytes, 1);
    sized(&mut bytes, b"avro.schema");
    sized(&mut bytes, &serde_json::to_vec(schema).unwrap());
    long(&mut bytes, 0);
    bytes.extend([42; 16]);
    long(&mut bytes, count);
    sized(&mut bytes, data);
    bytes.extend([42; 16]);
    bytes
}

fn sized(bytes: &mut Vec<u8>, input: &[u8]) {
    long(bytes, i64::try_from(input.len()).unwrap());
    bytes.extend_from_slice(input);
}

fn long(bytes: &mut Vec<u8>, value: i64) {
    let mut encoded = value.unsigned_abs() * 2 - u64::from(value < 0);
    while encoded >= 128 {
        bytes.push(u8::try_from(encoded & 127).unwrap() | 128);
        encoded >>= 7;
    }
    bytes.push(u8::try_from(encoded).unwrap());
}
