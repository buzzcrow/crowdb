#[path = "common/file_blocks.rs"]
mod blocks;

use std::sync::{atomic::Ordering, Arc};

use crowdb_access_iceberg::{
    file::{
        ByteRange, ChunkDirectory, ChunkEntry, ContentFormat, FileBlockStore, FileContent, FileIdentity,
        FileKind, FileReader, FileRecord, TableLocation, MAX_FILE_BLOCK_BYTES,
    },
    gc::{CandidatePhase, GcCandidate, ReclaimStep, TreeReclaimCursor},
    key::{CatalogId, FileId, OperationId, TableId},
    record::StorageRecord,
};

async fn logical_tib(store: &blocks::TestBlocks) -> FileRecord {
    let owner = FileIdentity {
        table: TableLocation {
            catalog: CatalogId::random(),
            table: TableId::random(),
        },
        file: FileId::random(),
    };
    // Repeated immutable references model the address space without writing a TiB.
    // This exercises production traversal and bounds, not physical capacity.
    let mut root = store
        .put(owner, 0, &vec![37; MAX_FILE_BLOCK_BYTES])
        .await
        .unwrap();
    let mut length = MAX_FILE_BLOCK_BYTES as u64;
    for (height, fanout) in [(1, 256), (2, 256), (3, 64)] {
        let directory = ChunkDirectory {
            owner,
            height,
            entries: vec![ChunkEntry { length, root }; fanout],
        };
        length = directory.length().unwrap();
        root = store
            .put(owner, height, &directory.encode().unwrap())
            .await
            .unwrap();
    }
    assert_eq!(length, 1_u64 << 40);
    FileRecord {
        file: owner.file,
        location: owner.table.file("data/scale.parquet").unwrap(),
        kind: FileKind::Data,
        format: ContentFormat::Parquet,
        length,
        digest: [19; 32],
        content: FileContent::Chunks { root: Some(root) },
        hint: None,
    }
}

#[tokio::test]
async fn tib_address_space_range_reads_keep_fixed_windows_and_small_authority() {
    let store = Arc::new(blocks::TestBlocks::default());
    let record = logical_tib(&store).await;
    let encoded = StorageRecord::File(Box::new(record.clone())).encode().unwrap();
    assert!(encoded.len() < 1024);
    for boundary in [
        MAX_FILE_BLOCK_BYTES as u64,
        1_u64 << 32,
        1_u64 << 39,
        record.length - 7,
    ] {
        let reads = store.reads.load(Ordering::SeqCst);
        let mut reader = FileReader::new(
            store.clone(),
            record.clone(),
            Some(ByteRange {
                start: boundary - 7,
                end: boundary + 7,
            }),
            5,
        )
        .unwrap();
        assert_eq!(store.reads.load(Ordering::SeqCst), reads);
        let mut bytes = 0;
        while let Some(frame) = reader.next().await.unwrap() {
            assert!(frame.len() <= 5);
            assert!(frame.iter().all(|byte| *byte == 37));
            bytes += frame.len();
            assert!(reader.retained_payload_bytes() <= MAX_FILE_BLOCK_BYTES);
            assert!(reader.retained_directory_bytes() <= 32 * 1024);
        }
        assert_eq!(bytes, 14);
        assert!(store.reads.load(Ordering::SeqCst) - reads <= 8);
    }
    assert!(store.max_input.load(Ordering::SeqCst) <= MAX_FILE_BLOCK_BYTES);
}

#[tokio::test]
async fn tib_reclamation_progress_serializes_a_bounded_resumable_cursor() {
    let store = blocks::TestBlocks::default();
    let record = logical_tib(&store).await;
    let mut candidate = GcCandidate {
        assembly: None,
        next_root: 0,
        completed_round: 0,
        task: OperationId::random(),
        generation: 1,
        first_seen_ms: 100,
        not_before_ms: 1000,
        revision: 1,
        phase: CandidatePhase::Deleting,
        cursor: TreeReclaimCursor::new(&record).unwrap(),
        file: record,
        part: None,
    };
    for _ in 0..600 {
        let reads = store.reads.load(Ordering::SeqCst);
        candidate.cursor = match candidate.cursor.next(&store).await.unwrap() {
            ReclaimStep::Descended(next) => next,
            ReclaimStep::Delete(next) => next.acknowledge(next.pending.as_ref().unwrap()).unwrap(),
            ReclaimStep::Complete => panic!("one bounded batch cannot traverse a TiB"),
        };
        assert!(store.reads.load(Ordering::SeqCst) - reads <= 1);
        assert!(candidate.cursor.frames.len() <= 4);
        candidate.revision += 1;
        let encoded = StorageRecord::GcCandidate(Box::new(candidate.clone()))
            .encode()
            .unwrap();
        assert!(encoded.len() < 4096);
        let StorageRecord::GcCandidate(recovered) =
            StorageRecord::decode(&candidate.key(), &encoded).unwrap()
        else {
            panic!("wrong record");
        };
        candidate = *recovered;
    }
}
