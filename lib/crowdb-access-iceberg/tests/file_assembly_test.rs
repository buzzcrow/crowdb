#[path = "common/file_blocks.rs"]
mod blocks;

use blocks::TestBlocks;
use crowdb_access_iceberg::file::{
    AssemblyPart, FileAssembly, FileIdentity, FileReader, FileTreeWriter, TableLocation,
};
use crowdb_access_iceberg::key::{CatalogId, FileId, TableId};
use sha2::{Digest, Sha256};
use std::sync::{atomic::Ordering, Arc};

fn owner() -> FileIdentity {
    FileIdentity {
        table: TableLocation {
            storage_namespace: None,
            catalog: CatalogId::random(),
            table: TableId::random(),
        },
        file: FileId::random(),
    }
}

async fn part(store: Arc<TestBlocks>, target: FileIdentity, ordinal: u16, bytes: &[u8]) -> AssemblyPart {
    let owner = FileIdentity {
        file: FileId::random(),
        ..target
    };
    let mut writer = FileTreeWriter::new(store, owner, 7).unwrap();
    writer.push(bytes).await.unwrap();
    AssemblyPart {
        ordinal,
        owner,
        tree: writer.finish().await.unwrap(),
    }
}

#[tokio::test]
async fn assembly_copies_whole_parts_in_order_with_bounded_frames_and_empty_parts() {
    let store = Arc::new(TestBlocks::default());
    let owner = owner();
    let parts = [
        part(store.clone(), owner, 0, b"first-part-payload").await,
        part(store.clone(), owner, 1, b"").await,
        part(store.clone(), owner, 2, b"last-part-payload").await,
    ];
    let mut assembly = FileAssembly::new(store.clone(), owner, 3, 1000, 5, 7).unwrap();
    for part in &parts {
        assembly.push(part).await.unwrap();
    }
    let tree = assembly.finish().await.unwrap();
    let expected = b"first-part-payloadlast-part-payload";
    assert_eq!(tree.digest, <[u8; 32]>::from(Sha256::digest(expected)));
    let mut reader = FileReader::from_tree(store, owner, tree, None, 11).unwrap();
    let mut actual = Vec::new();
    while let Some(frame) = reader.next().await.unwrap() {
        actual.extend(frame);
    }
    assert_eq!(actual, expected);
}

#[tokio::test]
async fn uncertain_write_poisons_local_copier_and_full_retry_restarts_from_selected_parts() {
    let store = Arc::new(TestBlocks::default());
    let owner = owner();
    let part = part(store.clone(), owner, 0, b"payload").await;
    let create = || FileAssembly::new(store.clone(), owner, 1, 100, 20, 7).unwrap();
    let mut assembly = create();
    store
        .fail_after
        .store(store.writes.load(Ordering::SeqCst) + 1, Ordering::SeqCst);
    assert!(assembly.push(&part).await.is_err());
    assert!(assembly.push(&part).await.is_err());
    assert!(assembly.finish().await.is_err());
    let mut retry = create();
    retry.push(&part).await.unwrap();
    assert_eq!(retry.finish().await.unwrap().digest, part.tree.digest);
    assert!(store.values.load().len() > 2);
}

#[tokio::test]
async fn assembly_rejects_reordered_foreign_incomplete_and_oversized_parts() {
    let store = Arc::new(TestBlocks::default());
    let owner = owner();
    let original = part(store.clone(), owner, 0, b"payload").await;
    assert!(FileAssembly::new(store.clone(), owner, 1, 100, 3, 7)
        .unwrap()
        .finish()
        .await
        .is_err());
    for changed in 0..4 {
        let mut part = AssemblyPart {
            ordinal: original.ordinal,
            owner: original.owner,
            tree: original.tree.clone(),
        };
        let mut limit = 100;
        match changed {
            0 => part.ordinal = 1,
            1 => part.owner.table.table = TableId::random(),
            2 => part.owner = owner,
            _ => limit = 6,
        }
        let before = store.writes.load(Ordering::SeqCst);
        let mut assembly = FileAssembly::new(store.clone(), owner, 1, limit, 3, 7).unwrap();
        assert!(assembly.push(&part).await.is_err());
        assert_eq!(store.writes.load(Ordering::SeqCst), before);
    }
    for (count, limit, frame, block) in [
        (0, 100, 3, 7),
        (10_001, 100, 3, 7),
        (1, 0, 3, 7),
        (1, 100, 0, 7),
        (1, 100, 65_537, 7),
        (1, 100, 3, 0),
        (1, 100, 3, 262_145),
    ] {
        assert!(FileAssembly::new(store.clone(), owner, count, limit, frame, block).is_err());
    }
}

#[tokio::test]
async fn whole_part_digest_failure_cannot_produce_a_finished_tree() {
    let store = Arc::new(TestBlocks::default());
    let owner = owner();
    let mut part = part(store.clone(), owner, 0, b"multi-leaf-payload").await;
    part.tree.digest[0] ^= 1;
    let mut assembly = FileAssembly::new(store, owner, 1, 100, 3, 7).unwrap();
    assert!(assembly.push(&part).await.is_err());
    assert!(assembly.finish().await.is_err());
}
