#[path = "common/file_blocks.rs"]
mod blocks;

use blocks::TestBlocks;
use crowdb_access_iceberg::file::{
    ContentFormat, FileBlockStore, FileContent, FileDigest, FileIdentity, FileKind, FileReader, FileRecord,
    FileTreeWriter, FileWriterCheckpoint, TableLocation,
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

#[tokio::test]
async fn checkpoint_restore_validates_internal_coverage_even_with_a_valid_storage_digest() {
    let store = Arc::new(TestBlocks::default());
    let owner = owner();
    let mut writer = FileTreeWriter::new(store.clone(), owner, 8).unwrap();
    writer.push(b"before").await.unwrap();
    let checkpoint = writer.checkpoint().await.unwrap();
    let original = store.read(&checkpoint.root).await.unwrap();
    let mut wrong_length = original.clone();
    let mut digest = FileDigest::new(owner);
    digest.update(b"changed").unwrap();
    wrong_length[7..196].copy_from_slice(&digest.checkpoint());
    let mut extra = original.clone();
    extra.push(0);
    let mut wrong_height = original;
    wrong_height[253] = 2;
    for bytes in [wrong_length, extra, wrong_height] {
        let root = store.put(owner, 0, &bytes).await.unwrap();
        assert!(
            FileTreeWriter::restore(store.clone(), owner, 8, &FileWriterCheckpoint { root })
                .await
                .is_err()
        );
    }
}

#[tokio::test]
async fn writer_checkpoints_resume_partial_leaves_and_directory_frontiers_on_another_instance() {
    let store = Arc::new(TestBlocks::default());
    let owner = owner();
    let mut writer = FileTreeWriter::new(store.clone(), owner, 7).unwrap();
    let mut bytes = Vec::new();
    for size in [0, 1, 55, 64, 257 * 7, 3000] {
        let input = vec![42; size];
        bytes.extend_from_slice(&input);
        writer.push(&input).await.unwrap();
        let checkpoint = writer.checkpoint().await.unwrap();
        assert_eq!(checkpoint.root.height, 0);
        assert!(checkpoint.root.logical_length <= 256 * 1024);
        drop(writer);
        writer = FileTreeWriter::restore(store.clone(), owner, 11, &checkpoint)
            .await
            .unwrap();
    }
    let tree = writer.finish().await.unwrap();
    assert_eq!(tree.length, bytes.len() as u64);
    assert_eq!(tree.digest, <[u8; 32]>::from(Sha256::digest(&bytes)));
    let record = FileRecord {
        file: owner.file,
        location: owner.table.file("data.parquet").unwrap(),
        kind: FileKind::Data,
        format: ContentFormat::Parquet,
        length: tree.length,
        digest: tree.digest,
        content: FileContent::Chunks { root: tree.root },
        hint: None,
    };
    let mut reader = FileReader::new(store, record, None, 13).unwrap();
    let mut actual = Vec::new();
    while let Some(frame) = reader.next().await.unwrap() {
        actual.extend(frame);
    }
    assert_eq!(actual, bytes);
}

#[tokio::test]
async fn checkpoint_write_failure_preserves_prior_progress_and_wrong_owner_or_corruption_fails() {
    let store = Arc::new(TestBlocks::default());
    let owner = owner();
    let mut writer = FileTreeWriter::new(store.clone(), owner, 8).unwrap();
    writer.push(b"before").await.unwrap();
    let checkpoint = writer.checkpoint().await.unwrap();
    writer.push(b"after").await.unwrap();
    store
        .fail_after
        .store(store.writes.load(Ordering::SeqCst) + 1, Ordering::SeqCst);
    assert!(writer.checkpoint().await.is_err());
    assert!(writer.push(b"ignored").await.is_err());
    let restored = FileTreeWriter::restore(store.clone(), owner, 8, &checkpoint)
        .await
        .unwrap();
    assert_eq!(
        restored.finish().await.unwrap().digest,
        <[u8; 32]>::from(Sha256::digest(b"before"))
    );
    let wrong = FileIdentity {
        file: FileId::random(),
        ..owner
    };
    assert!(FileTreeWriter::restore(store.clone(), wrong, 8, &checkpoint)
        .await
        .is_err());
    store.corrupt_reads.store(true, Ordering::SeqCst);
    assert!(FileTreeWriter::restore(store, owner, 8, &checkpoint)
        .await
        .is_err());
}
