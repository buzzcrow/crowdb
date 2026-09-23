#[path = "common/file_blocks.rs"]
mod blocks;

use std::sync::{atomic::Ordering, Arc};

use blocks::TestBlocks;
use crowdb_access_iceberg::file::{
    ByteRange, ContentFormat, FileContent, FileIdentity, FileKind, FileReader, FileRecord, FileTree,
    FileTreeWriter, TableLocation, MAX_FILE_BLOCK_BYTES,
};
use crowdb_access_iceberg::key::{CatalogId, FileId, TableId};

fn owner() -> FileIdentity {
    FileIdentity {
        table: TableLocation {
            catalog: CatalogId::random(),
            table: TableId::random(),
        },
        file: FileId::random(),
    }
}

fn record(owner: FileIdentity, tree: FileTree) -> FileRecord {
    FileRecord {
        file: owner.file,
        location: owner.table.file("data/file.parquet").unwrap(),
        kind: FileKind::Data,
        format: ContentFormat::Parquet,
        length: tree.length,
        digest: tree.digest,
        content: FileContent::Chunks { root: tree.root },
        hint: None,
    }
}

async fn collect(mut reader: FileReader) -> Vec<u8> {
    let mut result = Vec::new();
    while let Some(bytes) = reader.next().await.unwrap() {
        assert!(!bytes.is_empty());
        assert!(bytes.len() <= 3);
        result.extend_from_slice(&bytes);
    }
    result
}

#[tokio::test]
async fn streaming_tree_round_trips_empty_single_leaf_and_multiple_directory_levels() {
    for length in [0, 1, 7, 8, 9, 2048, 2056] {
        let store = Arc::new(TestBlocks::default());
        let owner = owner();
        let input: Vec<_> = (0..length)
            .map(|index| u8::try_from(index % 251).unwrap())
            .collect();
        let mut writer = FileTreeWriter::new(store.clone(), owner, 8).unwrap();
        for bytes in input.chunks(3) {
            writer.push(bytes).await.unwrap();
            assert!(writer.retained_bytes() <= 256 * 1024);
        }
        let tree = writer.finish().await.unwrap();
        if length > 2048 {
            assert_eq!(tree.root.as_ref().unwrap().height, 2);
        }
        let record = record(owner, tree);
        let reader = FileReader::new(store.clone(), record.clone(), None, 3).unwrap();
        assert_eq!(collect(reader).await, input);
        if length > 1 {
            let range = ByteRange {
                start: 1,
                end: length as u64 - 1,
            };
            let reader = FileReader::new(store.clone(), record, Some(range), 3).unwrap();
            assert_eq!(collect(reader).await, input[1..length - 1]);
        }
        assert!(store.max_input.load(Ordering::SeqCst) <= MAX_FILE_BLOCK_BYTES);
    }
}

#[tokio::test]
async fn backpressure_holds_one_leaf_and_performs_no_speculative_reads() {
    let store = Arc::new(TestBlocks::default());
    let owner = owner();
    let mut writer = FileTreeWriter::new(store.clone(), owner, 8).unwrap();
    writer.push(b"abcdefghijklmnopq").await.unwrap();
    let mut reader = FileReader::new(
        store.clone(),
        record(owner, writer.finish().await.unwrap()),
        None,
        3,
    )
    .unwrap();
    assert_eq!(store.reads.load(Ordering::SeqCst), 0);
    assert_eq!(reader.next().await.unwrap().unwrap(), b"abc");
    let reads = store.reads.load(Ordering::SeqCst);
    assert_eq!(reads, 2);
    assert!(reader.retained_payload_bytes() <= 8);
    assert_eq!(reader.next().await.unwrap().unwrap(), b"def");
    assert_eq!(store.reads.load(Ordering::SeqCst), reads);
    assert_eq!(reader.next().await.unwrap().unwrap(), b"gh");
    assert_eq!(reader.next().await.unwrap().unwrap(), b"ijk");
    assert_eq!(store.reads.load(Ordering::SeqCst), reads + 2);
}

#[tokio::test]
async fn storage_failure_retains_blocks_and_poisoned_writer_cannot_publish_partial_data() {
    for fail_after in [1, 257] {
        let store = Arc::new(TestBlocks::default());
        store.fail_after.store(fail_after, Ordering::SeqCst);
        let mut writer = FileTreeWriter::new(store.clone(), owner(), 1).unwrap();
        assert!(writer.push(&vec![42; 256]).await.is_err());
        assert!(writer.push(b"retry").await.is_err());
        assert!(writer.finish().await.is_err());
        assert_eq!(store.values.load().len(), fail_after);
    }
}

#[tokio::test]
async fn directory_corruption_foreign_owner_and_full_file_digest_mismatch_fail_closed() {
    let store = Arc::new(TestBlocks::default());
    let owner = owner();
    let mut writer = FileTreeWriter::new(store.clone(), owner, 4).unwrap();
    writer.push(b"abcdefgh").await.unwrap();
    let valid = record(owner, writer.finish().await.unwrap());
    store.corrupt_reads.store(true, Ordering::SeqCst);
    let mut reader = FileReader::new(store.clone(), valid.clone(), None, 3).unwrap();
    assert!(reader.next().await.is_err());
    assert!(reader.next().await.is_err());
    store.corrupt_reads.store(false, Ordering::SeqCst);
    let mut wrong_owner = valid.clone();
    wrong_owner.file = FileId::random();
    assert!(FileReader::new(store.clone(), wrong_owner, None, 3)
        .unwrap()
        .next()
        .await
        .is_err());
    let mut wrong_digest = valid;
    wrong_digest.digest[0] ^= 1;
    let mut reader = FileReader::new(store.clone(), wrong_digest, None, 3).unwrap();
    assert!(reader.next().await.unwrap().is_some());
    assert!(reader.next().await.unwrap().is_some());
    assert!(reader.next().await.unwrap().is_some());
    assert!(reader.next().await.is_err());
}
