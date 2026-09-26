use std::sync::Arc;

use crowdb_access_iceberg::{
    file::{
        ContentFormat, FileBlockStore, FileContent, FileIdentity, FileKind, FileRecord, FileTreeWriter,
        TableLocation,
    },
    gc::{CandidatePhase, GcCandidate, ReclaimStep, TreeReclaimCursor},
    key::{CatalogId, FileId, OperationId, TableId},
    record::StorageRecord,
};

#[path = "common/file_blocks.rs"]
mod blocks;

#[tokio::test]
async fn directory_deletion_resumes_without_rereading_deleted_children() {
    let blocks = Arc::new(blocks::TestBlocks::default());
    let owner = FileIdentity {
        table: TableLocation {
            catalog: CatalogId::random(),
            table: TableId::random(),
        },
        file: FileId::random(),
    };
    let mut writer = FileTreeWriter::new(blocks.clone(), owner, 64).unwrap();
    writer.push(&vec![7; 1024]).await.unwrap();
    let tree = writer.finish().await.unwrap();
    let record = FileRecord {
        file: owner.file,
        location: owner.table.file("data/file.parquet").unwrap(),
        kind: FileKind::Data,
        format: ContentFormat::Parquet,
        length: tree.length,
        digest: tree.digest,
        content: FileContent::Chunks { root: tree.root },
        hint: None,
    };
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
    let root = candidate.cursor.frames[0].root.chunk.low;
    let mut deleted = Vec::new();
    loop {
        match candidate.cursor.next(blocks.as_ref()).await.unwrap() {
            ReclaimStep::Descended(next) => candidate.cursor = next,
            ReclaimStep::Delete(next) => {
                candidate.cursor = next;
                let bytes = StorageRecord::GcCandidate(Box::new(candidate.clone()))
                    .encode()
                    .unwrap();
                let StorageRecord::GcCandidate(recovered) =
                    StorageRecord::decode(&candidate.key(), &bytes).unwrap()
                else {
                    panic!()
                };
                candidate = *recovered;
                let pending = candidate.cursor.pending.clone().unwrap();
                assert_eq!(
                    candidate.cursor.next(blocks.as_ref()).await.unwrap(),
                    ReclaimStep::Delete(candidate.cursor.clone())
                );
                blocks.values.rcu(|values| {
                    let mut next = (**values).clone();
                    next.remove(&pending.chunk.low);
                    next
                });
                assert_eq!(
                    candidate.cursor.next(blocks.as_ref()).await.unwrap(),
                    ReclaimStep::Delete(candidate.cursor.clone())
                );
                deleted.push(pending.chunk.low);
                candidate.cursor = candidate.cursor.acknowledge(&pending).unwrap();
            }
            ReclaimStep::Complete => break,
        }
        candidate.revision += 1;
    }
    assert_eq!(deleted.last(), Some(&root));
    assert!(blocks.values.load().is_empty());
    assert!(deleted.len() > 1);
}

#[tokio::test]
async fn corrupt_directory_never_authorizes_a_child_deletion() {
    let blocks = blocks::TestBlocks::default();
    let owner = FileIdentity {
        table: TableLocation {
            catalog: CatalogId::random(),
            table: TableId::random(),
        },
        file: FileId::random(),
    };
    let root = blocks.put(owner, 1, b"not a directory").await.unwrap();
    let cursor = TreeReclaimCursor {
        owner,
        frames: vec![crowdb_access_iceberg::gc::ReclaimFrame {
            root,
            length: 10,
            next_child: 0,
        }],
        pending: None,
    };
    assert!(cursor.next(&blocks).await.is_err());
    assert!(cursor.pending.is_none());
    assert_eq!(blocks.values.load().len(), 1);
}
