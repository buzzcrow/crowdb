use crate::blocks::TestBlocks;
use crowdb_access_iceberg::file::{
    ContentFormat, DeletionVectorReference, FileContent, FileIdentity, FileKind, FileRecord, FileTreeWriter,
    FormatHint, TableLocation,
};
use crowdb_access_iceberg::key::{CatalogId, FileId, TableId};
use std::sync::Arc;

pub fn array(key: u16, values: &[u16]) -> Vec<u8> {
    let mut bytes = 12346_u32.to_le_bytes().to_vec();
    bytes.extend(1_u32.to_le_bytes());
    bytes.extend(key.to_le_bytes());
    bytes.extend(u16::try_from(values.len() - 1).unwrap().to_le_bytes());
    bytes.extend(16_u32.to_le_bytes());
    for value in values {
        bytes.extend(value.to_le_bytes());
    }
    bytes
}

pub fn runs(key: u16, cardinality: u32, ranges: &[(u16, u16)]) -> Vec<u8> {
    let mut bytes = 12347_u32.to_le_bytes().to_vec();
    bytes.push(1);
    bytes.extend(key.to_le_bytes());
    bytes.extend(u16::try_from(cardinality - 1).unwrap().to_le_bytes());
    bytes.extend(u16::try_from(ranges.len()).unwrap().to_le_bytes());
    for (start, length_minus_one) in ranges {
        bytes.extend(start.to_le_bytes());
        bytes.extend(length_minus_one.to_le_bytes());
    }
    bytes
}

pub fn bitset(key: u16) -> Vec<u8> {
    let mut bytes = 12346_u32.to_le_bytes().to_vec();
    bytes.extend(1_u32.to_le_bytes());
    bytes.extend(key.to_le_bytes());
    bytes.extend(4096_u16.to_le_bytes());
    bytes.extend(16_u32.to_le_bytes());
    for index in 0..1024 {
        bytes.extend(
            if index < 64 {
                u64::MAX
            } else {
                u64::from(index == 64)
            }
            .to_le_bytes(),
        );
    }
    bytes
}

pub fn blob(bitmaps: &[(u32, Vec<u8>)]) -> Vec<u8> {
    let mut bytes = vec![0xd1, 0xd3, 0x39, 0x64];
    bytes.extend((bitmaps.len() as u64).to_le_bytes());
    for (key, bitmap) in bitmaps {
        bytes.extend(key.to_le_bytes());
        bytes.extend(bitmap);
    }
    let mut blob = u32::try_from(bytes.len()).unwrap().to_be_bytes().to_vec();
    blob.extend(&bytes);
    blob.extend(crc32fast::hash(&bytes).to_be_bytes());
    blob
}

pub async fn record(
    store: Arc<TestBlocks>,
    blob: &[u8],
    cardinality: u64,
) -> (FileRecord, DeletionVectorReference) {
    let table = TableLocation {
        catalog: CatalogId::random(),
        table: TableId::random(),
    };
    let (record, mut references) = record_with_references(
        store,
        table,
        "delete.puffin",
        &[("data.parquet", blob, cardinality)],
    )
    .await;
    (record, references.remove(0))
}

pub async fn record_with_references(
    store: Arc<TestBlocks>,
    table: TableLocation,
    key: &str,
    blobs: &[(&str, &[u8], u64)],
) -> (FileRecord, Vec<DeletionVectorReference>) {
    let mut bytes = b"PFA1".to_vec();
    let mut references = Vec::new();
    let mut metadata = Vec::new();
    for (target, blob, cardinality) in blobs {
        let reference = DeletionVectorReference {
            referenced: table.file(target).unwrap(),
            span: FormatHint {
                offset: bytes.len() as u64,
                length: blob.len() as u64,
            },
            cardinality: *cardinality,
        };
        metadata.push(serde_json::json!({
        "type":"deletion-vector-v1", "fields":[], "snapshot-id":-1,"sequence-number":-1,
        "offset":reference.span.offset,"length":blob.len(),"properties":{"referenced-data-file":reference.referenced.to_string(),"cardinality":cardinality.to_string()}
        }));
        references.push(reference);
        bytes.extend_from_slice(blob);
    }
    let footer = serde_json::to_vec(&serde_json::json!({"blobs":metadata})).unwrap();
    bytes.extend(b"PFA1");
    bytes.extend(&footer);
    bytes.extend(i32::try_from(footer.len()).unwrap().to_le_bytes());
    bytes.extend([0; 4]);
    bytes.extend(b"PFA1");
    let owner = FileIdentity {
        table,
        file: FileId::random(),
    };
    let mut writer = FileTreeWriter::new(store, owner, 128).unwrap();
    writer.push(&bytes).await.unwrap();
    let tree = writer.finish().await.unwrap();
    (
        FileRecord {
            file: owner.file,
            location: table.file(key).unwrap(),
            kind: FileKind::DeletionVector,
            format: ContentFormat::Puffin,
            length: tree.length,
            digest: tree.digest,
            content: FileContent::Chunks { root: tree.root },
            hint: None,
        },
        references,
    )
}
