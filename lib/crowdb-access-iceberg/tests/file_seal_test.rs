#[path = "common/file_blocks.rs"]
mod blocks;

use std::sync::Arc;

use blocks::TestBlocks;
use crowdb_access_iceberg::file::file_key;
use crowdb_access_iceberg::file::{
    ContentFormat, FileContent, FileIdentity, FileKind, FileSealer, FileTreeWriter, FormatHint, TableLocation,
};
use crowdb_access_iceberg::key::{CatalogId, FileId, TableId};
use crowdb_access_iceberg::record::StorageRecord;

fn owner() -> FileIdentity {
    FileIdentity {
        table: TableLocation {
            catalog: CatalogId::random(),
            table: TableId::random(),
        },
        file: FileId::random(),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn seal_verifies_complete_digest_and_selects_bounded_inline_metadata() {
    let blocks = Arc::new(TestBlocks::default());
    let identity = owner();
    let mut writer = FileTreeWriter::new(blocks.clone(), identity, 7).unwrap();
    writer.push(br#"{"hello":"world"}"#).await.unwrap();
    let tree = writer.finish().await.unwrap();
    let sealer = FileSealer::new(blocks.clone(), 1024).unwrap();
    let location = identity.table.file("metadata/one.json").unwrap();
    let record = sealer
        .seal(
            identity,
            location.clone(),
            tree.clone(),
            FileKind::Metadata,
            ContentFormat::Json,
        )
        .await
        .unwrap();
    assert_eq!(record.length, tree.length);
    assert_eq!(record.digest, tree.digest);
    assert!(matches!(record.content, FileContent::Inline { .. }));
    assert_eq!(record.hint, None);

    let mut wrong = tree.clone();
    wrong.digest[0] ^= 1;
    assert!(sealer
        .seal(
            identity,
            location.clone(),
            wrong,
            FileKind::Metadata,
            ContentFormat::Json
        )
        .await
        .is_err());
    assert!(sealer
        .seal(
            identity,
            location,
            tree,
            FileKind::Metadata,
            ContentFormat::Parquet
        )
        .await
        .is_err());
}

#[tokio::test]
async fn seal_checks_parquet_framing_and_stores_only_fixed_size_hint() {
    let blocks = Arc::new(TestBlocks::default());
    let identity = owner();
    let mut writer = FileTreeWriter::new(blocks.clone(), identity, 3).unwrap();
    writer.push(b"PAR1datafoot\x04\0\0\0PAR1").await.unwrap();
    let tree = writer.finish().await.unwrap();
    let sealer = FileSealer::new(blocks, 1024).unwrap();
    let record = sealer
        .seal(
            identity,
            identity.table.file("data/file.parquet").unwrap(),
            tree,
            FileKind::Data,
            ContentFormat::Parquet,
        )
        .await
        .unwrap();
    assert!(matches!(record.content, FileContent::Chunks { .. }));
    assert_eq!(record.hint, Some(FormatHint { offset: 8, length: 4 }));
}

#[tokio::test]
async fn sdk_upload_stays_unbound_until_selected_manifest_declares_use() {
    let blocks = Arc::new(TestBlocks::default());
    let identity = owner();
    let mut writer = FileTreeWriter::new(blocks.clone(), identity, 3).unwrap();
    writer.push(b"PAR1datafoot\x04\0\0\0PAR1").await.unwrap();
    let tree = writer.finish().await.unwrap();
    let record = FileSealer::new(blocks, 1024)
        .unwrap()
        .seal_uploaded(identity, identity.table.file("data/file.parquet").unwrap(), tree)
        .await
        .unwrap();
    assert_eq!(record.kind, FileKind::Unbound);
    assert!(record.bind_kind(FileKind::Data).is_ok());
    assert!(record.bind_kind(FileKind::EqualityDelete).is_ok());
    assert!(record.bind_kind(FileKind::Metadata).is_err());
    let key = file_key(record.location.table().catalog, record.file);
    let encoded = StorageRecord::File(Box::new(record.clone())).encode().unwrap();
    assert_eq!(
        StorageRecord::decode(&key, &encoded).unwrap(),
        StorageRecord::File(Box::new(record))
    );
}
