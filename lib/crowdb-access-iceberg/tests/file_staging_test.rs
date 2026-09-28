#[path = "common/file_blocks.rs"]
mod blocks;

use blocks::TestBlocks;
use crowdb_access_iceberg::file::{
    ByteRange, FileIdentity, FileReader, FileTree, FileTreeWriter, TableLocation,
};
use crowdb_access_iceberg::key::{CatalogId, FileId, TableId};
use sha2::{Digest, Sha256};
use std::sync::Arc;

fn owner() -> FileIdentity {
    FileIdentity {
        table: TableLocation {
            catalog: CatalogId::random(),
            table: TableId::random(),
        },
        file: FileId::random(),
    }
}

#[tokio::test]
async fn staged_partial_file_bytes_read_without_declaring_a_complete_file_format() {
    let store = Arc::new(TestBlocks::default());
    let owner = owner();
    let bytes = b"this is an incomplete multipart byte interval";
    let mut writer = FileTreeWriter::new(store.clone(), owner, 5).unwrap();
    writer.push(bytes).await.unwrap();
    let tree = writer.finish().await.unwrap();
    let mut reader = FileReader::from_tree(store.clone(), owner, tree.clone(), None, 7).unwrap();
    let mut actual = Vec::new();
    while let Some(frame) = reader.next().await.unwrap() {
        actual.extend(frame);
    }
    assert_eq!(actual, bytes);
    let mut range = FileReader::from_tree(
        store.clone(),
        owner,
        tree.clone(),
        Some(ByteRange { start: 4, end: 13 }),
        64,
    )
    .unwrap();
    let mut actual = Vec::new();
    while let Some(frame) = range.next().await.unwrap() {
        actual.extend(frame);
    }
    assert_eq!(actual, bytes[4..13]);
    let mut wrong = FileReader::from_tree(
        store,
        FileIdentity {
            file: FileId::random(),
            ..owner
        },
        tree,
        None,
        7,
    )
    .unwrap();
    assert!(wrong.next().await.is_err());
}

#[test]
fn staged_reader_preserves_empty_digest_and_range_validation() {
    let store = Arc::new(TestBlocks::default());
    let owner = owner();
    let valid = FileTree {
        root: None,
        length: 0,
        digest: Sha256::digest([]).into(),
    };
    assert!(FileReader::from_tree(store.clone(), owner, valid.clone(), None, 1).is_ok());
    assert!(FileReader::from_tree(store.clone(), owner, valid.clone(), None, 0).is_err());
    assert!(FileReader::from_tree(
        store.clone(),
        owner,
        valid.clone(),
        Some(ByteRange { start: 0, end: 1 }),
        1
    )
    .is_err());
    let invalid = FileTree {
        digest: [0; 32],
        ..valid
    };
    assert!(FileReader::from_tree(store, owner, invalid, None, 1).is_err());
}
