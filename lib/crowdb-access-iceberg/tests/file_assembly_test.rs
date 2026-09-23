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
async fn assembly_resumes_one_bounded_window_at_a_time_across_empty_and_nonempty_parts() {
    let store = Arc::new(TestBlocks::default());
    let owner = owner();
    let parts = [
        part(store.clone(), owner, 0, b"first-part-payload").await,
        part(store.clone(), owner, 1, b"").await,
        part(store.clone(), owner, 2, b"last-part-payload").await,
    ];
    let create = || FileAssembly::new(store.clone(), owner, [1; 32], 3, 1000, 5, 7).unwrap();
    let mut progress = create().begin();
    while progress.next_part < 3 {
        let next = create()
            .advance(&progress, &parts[usize::from(progress.next_part)])
            .await
            .unwrap();
        assert!(next.completed_bytes - progress.completed_bytes <= 5);
        progress = next;
    }
    let tree = create().finish(&progress).await.unwrap();
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
async fn assembly_replay_after_uncertain_write_copies_each_selected_byte_exactly_once() {
    let store = Arc::new(TestBlocks::default());
    let owner = owner();
    let part = part(store.clone(), owner, 0, b"payload").await;
    let assembly = FileAssembly::new(store.clone(), owner, [1; 32], 1, 100, 20, 7).unwrap();
    let initial = assembly.begin();
    store
        .fail_after
        .store(store.writes.load(Ordering::SeqCst) + 1, Ordering::SeqCst);
    assert!(assembly.advance(&initial, &part).await.is_err());
    let next = assembly.advance(&initial, &part).await.unwrap();
    assert_eq!(
        assembly.finish(&next).await.unwrap().digest,
        <[u8; 32]>::from(Sha256::digest(b"payload"))
    );
    assert!(store.values.load().len() > 2);
}

#[tokio::test]
async fn assembly_rejects_changed_parts_selections_incomplete_results_and_byte_overflow() {
    let store = Arc::new(TestBlocks::default());
    let owner = owner();
    let mut part = part(store.clone(), owner, 0, b"payload").await;
    let assembly = FileAssembly::new(store.clone(), owner, [1; 32], 1, 100, 3, 7).unwrap();
    assert!(assembly.finish(&assembly.begin()).await.is_err());
    let progress = assembly.advance(&assembly.begin(), &part).await.unwrap();
    part.owner.file = FileId::random();
    assert!(assembly.advance(&progress, &part).await.is_err());
    let other = FileAssembly::new(store.clone(), owner, [2; 32], 1, 100, 3, 7).unwrap();
    assert!(other.advance(&progress, &part).await.is_err());
    let small = FileAssembly::new(store, owner, [1; 32], 1, 6, 3, 7).unwrap();
    assert!(small.advance(&small.begin(), &part).await.is_err());
}

#[tokio::test]
async fn assembly_verifies_each_whole_part_digest_across_restarted_windows() {
    let store = Arc::new(TestBlocks::default());
    let owner = owner();
    let mut part = part(store.clone(), owner, 0, b"multi-leaf-payload").await;
    part.tree.digest[0] ^= 1;
    let assembly = FileAssembly::new(store, owner, [1; 32], 1, 100, 3, 7).unwrap();
    let mut progress = assembly.begin();
    while progress.part_offset + 3 < part.tree.length {
        progress = assembly.advance(&progress, &part).await.unwrap();
        assert_eq!(progress.part_digest.as_ref().unwrap().len(), 189);
    }
    assert!(assembly.advance(&progress, &part).await.is_err());
    assert_eq!(progress.next_part, 0);
}
