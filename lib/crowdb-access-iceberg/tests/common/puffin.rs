use crate::blocks::TestBlocks;
use crowdb_access_iceberg::file::{
    ContentFormat, FileContent, FileIdentity, FileKind, FileRecord, FileTreeWriter, TableLocation,
};
use crowdb_access_iceberg::key::{CatalogId, FileId, TableId};
use lz4_flex::frame::{FrameEncoder, FrameInfo};
use std::io::Write;
use std::sync::Arc;

pub fn referenced() -> crowdb_access_iceberg::file::FileLocation {
    TableLocation {
        catalog: CatalogId::random(),
        table: TableId::random(),
    }
    .file("data.parquet")
    .unwrap()
}

pub fn metadata(referenced: &str) -> serde_json::Value {
    serde_json::json!({"blobs":[{
        "type":"deletion-vector-v1", "fields":[], "snapshot-id":-1, "sequence-number":-1,
        "offset":4,"length":20,"properties":{"referenced-data-file":referenced,"cardinality":"2"}
    }], "properties":{"created-by":"test"}})
}

pub fn compressed(bytes: &[u8], include_size: bool) -> Vec<u8> {
    let info = FrameInfo::new()
        .content_size(include_size.then_some(bytes.len() as u64))
        .content_checksum(true)
        .block_checksums(true);
    let mut encoder = FrameEncoder::with_frame_info(info, Vec::new());
    encoder.write_all(bytes).unwrap();
    encoder.finish().unwrap()
}

pub async fn record(store: Arc<TestBlocks>, footer: &[u8], compressed: bool) -> FileRecord {
    let mut bytes = b"PFA1".to_vec();
    bytes.extend([0; 20]);
    bytes.extend(b"PFA1");
    bytes.extend(footer);
    bytes.extend(i32::try_from(footer.len()).unwrap().to_le_bytes());
    bytes.extend([u8::from(compressed), 0, 0, 0]);
    bytes.extend(b"PFA1");
    let location = referenced();
    let owner = FileIdentity {
        table: location.table(),
        file: FileId::random(),
    };
    let mut writer = FileTreeWriter::new(store, owner, 128).unwrap();
    writer.push(&bytes).await.unwrap();
    let tree = writer.finish().await.unwrap();
    FileRecord {
        file: owner.file,
        location,
        kind: FileKind::Statistics,
        format: ContentFormat::Puffin,
        length: tree.length,
        digest: tree.digest,
        content: FileContent::Chunks { root: tree.root },
        hint: None,
    }
}
