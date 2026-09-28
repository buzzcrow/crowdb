use std::collections::{BTreeMap, VecDeque};
use std::pin::Pin;
use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc,
};
use std::task::{Context, Poll};

use arc_swap::ArcSwap;
use async_trait::async_trait;
use crowdb_access_iceberg::file::{ChunkRoot, FileBlockStore, FileIdentity, FileIoError, TableLocation};
use crowdb_access_iceberg::key::{CatalogId, FileId, TableId};
use crowdb_protocol::common::ChunkId;
use hyper::body::{Body, Bytes, Frame};
use sha2::{Digest, Sha256};

pub fn owner() -> FileIdentity {
    FileIdentity {
        table: TableLocation {
            catalog: CatalogId::random(),
            table: TableId::random(),
        },
        file: FileId::random(),
    }
}

pub struct TestUploadBody {
    pub frames: VecDeque<Result<Frame<Bytes>, std::io::Error>>,
    pub polls: Arc<AtomicUsize>,
}

impl TestUploadBody {
    pub fn new(bytes: &[u8], frame_bytes: usize) -> Self {
        Self {
            frames: bytes
                .chunks(frame_bytes)
                .map(|bytes| Ok(Frame::data(Bytes::copy_from_slice(bytes))))
                .collect(),
            polls: Arc::new(AtomicUsize::new(0)),
        }
    }
}

impl Body for TestUploadBody {
    type Data = Bytes;
    type Error = std::io::Error;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        _context: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Self::Error>>> {
        self.polls.fetch_add(1, Ordering::SeqCst);
        Poll::Ready(self.frames.pop_front())
    }
}

#[derive(Default)]
pub struct TestUploadBlocks {
    pub values: ArcSwap<BTreeMap<u64, Arc<Vec<u8>>>>,
    pub writes: AtomicUsize,
    pub max_input: AtomicUsize,
    pub fail: AtomicBool,
    pub pause: AtomicBool,
    pub entered: tokio::sync::Notify,
    pub release: tokio::sync::Notify,
}

#[async_trait]
impl FileBlockStore for TestUploadBlocks {
    async fn put(&self, _owner: FileIdentity, height: u8, bytes: &[u8]) -> Result<ChunkRoot, FileIoError> {
        self.max_input.fetch_max(bytes.len(), Ordering::SeqCst);
        let index = self.writes.fetch_add(1, Ordering::SeqCst) as u64 + 1;
        self.values.rcu(|values| {
            let mut next = (**values).clone();
            next.insert(index, Arc::new(bytes.to_vec()));
            next
        });
        if self.pause.load(Ordering::SeqCst) {
            self.entered.notify_one();
            self.release.notified().await;
        }
        if self.fail.load(Ordering::SeqCst) {
            return Err(FileIoError::Bounds);
        }
        Ok(ChunkRoot {
            chunk: ChunkId { high: 1, low: index },
            offset: 0,
            physical_length: bytes.len() as u64 + 64,
            logical_offset: 0,
            logical_length: bytes.len() as u64,
            height,
            digest: Sha256::digest(bytes).into(),
        })
    }

    async fn read(&self, root: &ChunkRoot) -> Result<Vec<u8>, FileIoError> {
        self.values
            .load()
            .get(&root.chunk.low)
            .map(|bytes| bytes.as_ref().clone())
            .ok_or(FileIoError::Bounds)
    }
}
