use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc,
};

use serde::de::IgnoredAny;
use serde::Deserialize;

use super::{ContentFormat, FileBlockStore, FileIoError, FileKind, FileReader, FileRecord};

mod reader;
mod scan;

#[derive(Debug, thiserror::Error)]
pub enum JsonSealError {
    #[error(transparent)]
    Storage(#[from] FileIoError),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error("metadata JSON sealing bounds exceeded")]
    Bounds,
    #[error("metadata JSON sealing capacity exhausted")]
    Busy,
    #[error("metadata JSON sealing requires the Tokio runtime")]
    Runtime,
    #[error("metadata JSON sealing worker failed: {0}")]
    Worker(String),
}

pub struct JsonSealer {
    store: Arc<dyn FileBlockStore>,
    active: Arc<AtomicUsize>,
    max_active: usize,
    max_file_bytes: u64,
    max_depth: usize,
}

impl JsonSealer {
    /// # Errors
    /// Rejects unbounded concurrency, empty file limits and excessive nesting limits.
    pub fn new(
        store: Arc<dyn FileBlockStore>,
        max_active: usize,
        max_file_bytes: u64,
        max_depth: usize,
    ) -> Result<Self, JsonSealError> {
        if max_active == 0 || max_active > 64 || max_file_bytes == 0 || max_depth == 0 || max_depth > 128 {
            return Err(JsonSealError::Bounds);
        }
        Ok(Self {
            store,
            active: Arc::new(AtomicUsize::new(0)),
            max_active,
            max_file_bytes,
            max_depth,
        })
    }

    #[must_use]
    pub fn active(&self) -> usize {
        self.active.load(Ordering::Acquire)
    }

    /// Validates a complete UTF-8 JSON object without materializing its metadata graph.
    /// Iceberg schema and table semantics are validated separately at commit admission.
    /// # Errors
    /// Rejects malformed input, oversized files, excessive nesting and busy admission.
    pub async fn validate(&self, record: FileRecord) -> Result<FileRecord, JsonSealError> {
        if record.kind != FileKind::Metadata
            || record.format != ContentFormat::Json
            || record.length > self.max_file_bytes
        {
            return Err(JsonSealError::Bounds);
        }
        let handle = tokio::runtime::Handle::try_current().map_err(|_| JsonSealError::Runtime)?;
        self.active
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |active| {
                (active < self.max_active).then_some(active + 1)
            })
            .map_err(|_| JsonSealError::Busy)?;
        let permit = Permit(self.active.clone());
        let reader = FileReader::new(self.store.clone(), record.clone(), None, 16 * 1024)?;
        let cancelled = Arc::new(AtomicBool::new(false));
        let cancellation = Cancellation(cancelled.clone());
        let max_depth = self.max_depth;
        let result = tokio::task::spawn_blocking(move || {
            let _permit = permit;
            let mut input = reader::JsonReader::new(reader, handle, cancelled, max_depth);
            let mut parser = serde_json::Deserializer::from_reader(&mut input);
            IgnoredAny::deserialize(&mut parser)?;
            parser.end()?;
            input.finish().map_err(serde_json::Error::io)?;
            Ok::<_, JsonSealError>(record)
        })
        .await
        .map_err(|error| JsonSealError::Worker(error.to_string()))?;
        drop(cancellation);
        result
    }
}

struct Permit(Arc<AtomicUsize>);
impl Drop for Permit {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::AcqRel);
    }
}

struct Cancellation(Arc<AtomicBool>);
impl Drop for Cancellation {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Release);
    }
}
