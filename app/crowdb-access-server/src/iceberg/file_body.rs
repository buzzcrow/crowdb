use std::future::Future;
use std::pin::Pin;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use std::task::{Context, Poll};

use crowdb_access_iceberg::file::{ByteRange, FileBlockStore, FileIoError, FileReader, FileRecord};
use hyper::body::{Body, Bytes, Frame, SizeHint};

#[derive(Debug, thiserror::Error)]
pub enum FileBodyError {
    #[error(transparent)]
    Storage(#[from] FileIoError),
    #[error("file response capacity exhausted")]
    Busy,
}

pub struct FileResponseBudget {
    active: Arc<AtomicUsize>,
    limit: usize,
}

impl FileResponseBudget {
    /// # Errors
    /// Rejects empty or unbounded response concurrency limits.
    pub fn new(limit: usize) -> Result<Self, FileIoError> {
        if limit == 0 || limit > 64 {
            return Err(FileIoError::Bounds);
        }
        Ok(Self {
            active: Arc::new(AtomicUsize::new(0)),
            limit,
        })
    }

    #[must_use]
    pub fn active(&self) -> usize {
        self.active.load(Ordering::Acquire)
    }

    /// Admits one pull response without fetching any file blocks.
    /// # Errors
    /// Rejects exhausted response capacity or invalid file records/ranges.
    pub fn body(
        &self,
        store: Arc<dyn FileBlockStore>,
        record: FileRecord,
        range: Option<ByteRange>,
    ) -> Result<FileReadBody, FileBodyError> {
        self.active
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |active| {
                (active < self.limit).then_some(active + 1)
            })
            .map_err(|_| FileBodyError::Busy)?;
        let permit = Permit(self.active.clone());
        let length = range.map_or(record.length, |range| range.end.saturating_sub(range.start));
        let reader = FileReader::new(store, record, range, 16 * 1024)?;
        Ok(FileReadBody {
            reader: (length > 0).then_some(reader),
            pending: None,
            remaining: length,
            permit: (length > 0).then_some(permit),
        })
    }
}

struct Permit(Arc<AtomicUsize>);
impl Drop for Permit {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::AcqRel);
    }
}

type ReadFuture = Pin<Box<dyn Future<Output = (FileReader, Result<Option<Vec<u8>>, FileIoError>)> + Send>>;

pub struct FileReadBody {
    reader: Option<FileReader>,
    pending: Option<ReadFuture>,
    remaining: u64,
    permit: Option<Permit>,
}

impl FileReadBody {
    fn finish(&mut self) {
        self.reader = None;
        self.pending = None;
        self.remaining = 0;
        self.permit = None;
    }
}

impl Body for FileReadBody {
    type Data = Bytes;
    type Error = FileIoError;

    fn poll_frame(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, FileIoError>>> {
        let body = self.get_mut();
        if body.remaining == 0 {
            return Poll::Ready(None);
        }
        if body.pending.is_none() {
            let Some(mut reader) = body.reader.take() else {
                body.finish();
                return Poll::Ready(Some(Err(FileIoError::Finished)));
            };
            body.pending = Some(Box::pin(async move {
                let result = reader.next().await;
                (reader, result)
            }));
        }
        let (reader, result) = match body
            .pending
            .as_mut()
            .expect("pending read is initialized")
            .as_mut()
            .poll(context)
        {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(value) => value,
        };
        body.pending = None;
        match result {
            Ok(Some(bytes)) if !bytes.is_empty() && bytes.len() as u64 <= body.remaining => {
                body.remaining -= bytes.len() as u64;
                if body.remaining == 0 {
                    body.finish();
                } else {
                    body.reader = Some(reader);
                }
                Poll::Ready(Some(Ok(Frame::data(Bytes::from(bytes)))))
            }
            result => {
                body.finish();
                Poll::Ready(Some(Err(result.err().unwrap_or(FileIoError::Bounds))))
            }
        }
    }

    fn is_end_stream(&self) -> bool {
        self.remaining == 0
    }

    fn size_hint(&self) -> SizeHint {
        SizeHint::with_exact(self.remaining)
    }
}
