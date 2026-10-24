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
            storage_namespace: None,
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
    let assembly = FileAssembly::new(store.clone(), owner, 1, 16, 16, 8).unwrap();
    store.enabled.store(true, Ordering::SeqCst);
    (store, assembly, part, owner)
}

#[tokio::test]
async fn assembly_overlaps_one_next_read_with_the_current_write_without_reordering_bytes() {
    let (store, mut assembly, part, owner) = setup().await;
    tokio::time::timeout(Duration::from_secs(1), assembly.push(&part))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(store.reads.load(Ordering::SeqCst), 3);
    store.enabled.store(false, Ordering::SeqCst);
    let tree = assembly.finish().await.unwrap();
    let mut reader = FileReader::from_tree(store, owner, tree, None, 8).unwrap();
    assert_eq!(reader.next().await.unwrap().unwrap(), b"abcdefgh");
    assert_eq!(reader.next().await.unwrap().unwrap(), b"ijklmnop");
    assert!(reader.next().await.unwrap().is_none());
}

#[tokio::test]
async fn failed_overlapped_read_or_write_poisons_the_copier_without_detaching_work() {
    for fail_read in [false, true] {
        let (store, mut assembly, part, owner) = setup().await;
        store.fail_read.store(fail_read, Ordering::SeqCst);
        store.fail_write.store(!fail_read, Ordering::SeqCst);
        assert!(tokio::time::timeout(Duration::from_secs(1), assembly.push(&part))
            .await
            .unwrap()
            .is_err());
        assert!(assembly.push(&part).await.is_err());
        assert!(assembly.finish().await.is_err());
        let writes = store.inner.writes.load(Ordering::SeqCst);
        tokio::task::yield_now().await;
        assert_eq!(store.inner.writes.load(Ordering::SeqCst), writes);
        store.enabled.store(false, Ordering::SeqCst);
        let mut assembly = FileAssembly::new(store.clone(), owner, 1, 16, 16, 8).unwrap();
        assembly.push(&part).await.unwrap();
        assert_eq!(assembly.finish().await.unwrap().digest, part.tree.digest);
    }
}

#[tokio::test]
async fn cancelled_copy_drops_its_read_and_requires_a_complete_new_attempt() {
    let (store, mut assembly, part, owner) = setup().await;
    store.enabled.store(false, Ordering::SeqCst);
    store.inner.pause_reads.store(true, Ordering::SeqCst);
    {
        let copy = assembly.push(&part);
        tokio::pin!(copy);
        tokio::select! {
            result = &mut copy => panic!("copy finished before its blocked read: {result:?}"),
            () = store.inner.read_entered.notified() => {}
        }
    }
    assert!(assembly.finish().await.is_err());
    let writes = store.inner.writes.load(Ordering::SeqCst);
    store.inner.pause_reads.store(false, Ordering::SeqCst);
    store.inner.read_release.notify_one();
    tokio::task::yield_now().await;
    assert_eq!(store.inner.writes.load(Ordering::SeqCst), writes);
    let mut retry = FileAssembly::new(store, owner, 1, 16, 16, 8).unwrap();
    retry.push(&part).await.unwrap();
    assert_eq!(retry.finish().await.unwrap().digest, part.tree.digest);
}
