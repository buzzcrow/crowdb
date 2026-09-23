use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use async_trait::async_trait;
use crowdb_access_iceberg::file::{
    ChunkRoot, ContentFormat, FileBlockStore, FileContent, FileIdentity, FileIoError, FileKind, FileRecord,
    TableLocation,
};
use crowdb_access_iceberg::key::{CatalogId, FileId, TableId};
use crowdb_protocol::common::ChunkId;
use sha2::{Digest, Sha256};

#[derive(Default)]
pub struct TestFileBlocks {
    pub bytes: Vec<u8>,
    pub reads: AtomicUsize,
    pub fail: AtomicBool,
    pub pause: AtomicBool,
    pub entered: tokio::sync::Notify,
    pub release: tokio::sync::Notify,
}

impl TestFileBlocks {
    pub fn record(&self) -> FileRecord {
        let digest = Sha256::digest(&self.bytes).into();
        FileRecord {
            file: FileId::random(),
            location: TableLocation {
                catalog: CatalogId::random(),
                table: TableId::random(),
            }
            .file("data.parquet")
            .unwrap(),
            kind: FileKind::Data,
            format: ContentFormat::Parquet,
            length: self.bytes.len() as u64,
            digest,
            hint: None,
            content: FileContent::Chunks {
                root: (!self.bytes.is_empty()).then_some(ChunkRoot {
                    chunk: ChunkId { high: 1, low: 1 },
                    offset: 0,
                    physical_length: self.bytes.len() as u64 + 64,
                    logical_offset: 0,
                    logical_length: self.bytes.len() as u64,
                    height: 0,
                    digest,
                }),
            },
        }
    }
}

#[async_trait]
impl FileBlockStore for TestFileBlocks {
    async fn put(&self, _owner: FileIdentity, _height: u8, _bytes: &[u8]) -> Result<ChunkRoot, FileIoError> {
        Err(FileIoError::Bounds)
    }
    async fn read(&self, _root: &ChunkRoot) -> Result<Vec<u8>, FileIoError> {
        self.reads.fetch_add(1, Ordering::SeqCst);
        if self.pause.load(Ordering::SeqCst) {
            self.entered.notify_one();
            self.release.notified().await;
        }
        if self.fail.load(Ordering::SeqCst) {
            return Err(FileIoError::Bounds);
        }
        Ok(self.bytes.clone())
    }
}
