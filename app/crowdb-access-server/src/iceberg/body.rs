use std::pin::Pin;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use std::task::{Context, Poll};

use hyper::body::{Body, Bytes, Frame, SizeHint};

use super::file_body::FileReadBody;

pub(super) struct SpoolPermit(Arc<AtomicUsize>);

impl SpoolPermit {
    pub(super) fn acquire(active: &Arc<AtomicUsize>) -> Option<Self> {
        active
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |count| {
                (count < 4).then_some(count + 1)
            })
            .ok()?;
        Some(Self(active.clone()))
    }
}

impl Drop for SpoolPermit {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::AcqRel);
    }
}

pub(super) struct IcebergBody {
    bytes: Bytes,
    _permit: Option<SpoolPermit>,
    file: Option<FileReadBody>,
}

impl IcebergBody {
    pub(super) fn new(bytes: Vec<u8>) -> Self {
        Self {
            bytes: Bytes::from(bytes),
            _permit: None,
            file: None,
        }
    }
    pub(super) fn with_permit(bytes: Vec<u8>, permit: SpoolPermit) -> Self {
        Self {
            bytes: Bytes::from(bytes),
            _permit: Some(permit),
            file: None,
        }
    }

    pub(super) fn file(body: FileReadBody) -> Self {
        Self {
            bytes: Bytes::new(),
            _permit: None,
            file: Some(body),
        }
    }
}

impl Body for IcebergBody {
    type Data = Bytes;
    type Error = Box<dyn std::error::Error + Send + Sync>;

    fn poll_frame(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Self::Error>>> {
        let body = self.get_mut();
        if let Some(file) = &mut body.file {
            return Pin::new(file)
                .poll_frame(context)
                .map(|frame| frame.map(|result| result.map_err(Into::into)));
        }
        if body.bytes.is_empty() {
            return Poll::Ready(None);
        }
        let length = body.bytes.len().min(16 * 1024);
        Poll::Ready(Some(Ok(Frame::data(body.bytes.split_to(length)))))
    }

    fn is_end_stream(&self) -> bool {
        self.bytes.is_empty() && self.file.as_ref().map_or(true, Body::is_end_stream)
    }
    fn size_hint(&self) -> SizeHint {
        self.file
            .as_ref()
            .map_or_else(|| SizeHint::with_exact(self.bytes.len() as u64), Body::size_hint)
    }
}
