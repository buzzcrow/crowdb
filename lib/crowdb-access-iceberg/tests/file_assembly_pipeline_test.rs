#[path = "common/file_blocks.rs"]
mod blocks;

use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc,
};
use std::time::Duration;

use async_trait::async_trait;
use crowdb_access_iceberg::{
    file::{
        AssemblyPart, ChunkRoot, FileAssembly, FileBlockStore, FileIdentity, FileIoError, FileReader,
        FileTreeWriter, TableLocation,
    },
    key::{CatalogId, FileId, TableId},
};

#[derive(Default)]
struct TestPipelineBlocks {
    inner: blocks::TestBlocks,
    enabled: AtomicBool,
    reads: AtomicUsize,
    writes: AtomicUsize,
    fail_read: AtomicBool,
    fail_write: AtomicBool,
    read_entered: tokio::sync::Notify,
    write_entered: tokio::sync::Notify,
}

#[async_trait]
impl FileBlockStore for TestPipelineBlocks {
    async fn put(&self, owner: FileIdentity, height: u8, bytes: &[u8]) -> Result<ChunkRoot, FileIoError> {
        if self.enabled.load(Ordering::SeqCst) && self.writes.fetch_add(1, Ordering::SeqCst) == 0 {
            self.write_entered.notify_one();
            self.read_entered.notified().await;
            if self.fail_write.load(Ordering::SeqCst) {
                return Err(FileIoError::Bounds);
            }
        }
        self.inner.put(owner, height, bytes).await
    }

    async fn read(&self, root: &ChunkRoot) -> Result<Vec<u8>, FileIoError> {
        if self.enabled.load(Ordering::SeqCst) && self.reads.fetch_add(1, Ordering::SeqCst) == 2 {
            self.write_entered.notified().await;
            self.read_entered.notify_one();
            if self.fail_read.load(Ordering::SeqCst) {
                return Err(FileIoError::Bounds);
            }
        }
        self.inner.read(root).await
    }
}

async fn setup() -> (Arc<TestPipelineBlocks>, FileAssembly, AssemblyPart, FileIdentity) {
    let store = Arc::new(TestPipelineBlocks::default());
    let owner = FileIdentity {
        table: TableLocation {
            catalog: CatalogId::random(),
            table: TableId::random(),
        },
        file: FileId::random(),
    };
    let source = FileIdentity {
        file: FileId::random(),
        ..owner
    };
    let mut writer = FileTreeWriter::new(store.clone(), source, 8).unwrap();
    writer.push(b"abcdefghijklmnop").await.unwrap();
    let part = AssemblyPart {
        ordinal: 0,
        owner: source,
        tree: writer.finish().await.unwrap(),
    };
    let assembly = FileAssembly::new(store.clone(), owner, [1; 32], 1, 16, 16, 8).unwrap();
    store.enabled.store(true, Ordering::SeqCst);
    (store, assembly, part, owner)
}

#[tokio::test]
async fn assembly_overlaps_one_next_read_with_the_current_write_without_reordering_bytes() {
    let (store, assembly, part, owner) = setup().await;
    let progress = tokio::time::timeout(Duration::from_secs(1), assembly.advance(&assembly.begin(), &part))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(progress.completed_bytes, 16);
    assert_eq!(store.reads.load(Ordering::SeqCst), 3);
    store.enabled.store(false, Ordering::SeqCst);
    let tree = assembly.finish(&progress).await.unwrap();
    let mut reader = FileReader::from_tree(store, owner, tree, None, 8).unwrap();
    assert_eq!(reader.next().await.unwrap().unwrap(), b"abcdefgh");
    assert_eq!(reader.next().await.unwrap().unwrap(), b"ijklmnop");
    assert!(reader.next().await.unwrap().is_none());
}

#[tokio::test]
async fn failed_overlapped_read_or_write_never_returns_a_checkpoint_or_detaches_work() {
    for fail_read in [false, true] {
        let (store, assembly, part, _) = setup().await;
        store.fail_read.store(fail_read, Ordering::SeqCst);
        store.fail_write.store(!fail_read, Ordering::SeqCst);
        let initial = assembly.begin();
        assert!(
            tokio::time::timeout(Duration::from_secs(1), assembly.advance(&initial, &part))
                .await
                .unwrap()
                .is_err()
        );
        assert_eq!(initial.completed_bytes, 0);
        assert!(initial.writer.is_none());
        let writes = store.inner.writes.load(Ordering::SeqCst);
        tokio::task::yield_now().await;
        assert_eq!(store.inner.writes.load(Ordering::SeqCst), writes);
        store.enabled.store(false, Ordering::SeqCst);
        let progress = assembly.advance(&initial, &part).await.unwrap();
        assert_eq!(progress.completed_bytes, 16);
        assert_eq!(assembly.finish(&progress).await.unwrap().digest, part.tree.digest);
    }
}
