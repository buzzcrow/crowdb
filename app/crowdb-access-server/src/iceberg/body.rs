use std::pin::Pin;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use std::task::{Context, Poll};

use hyper::body::{Body, Bytes, Frame, SizeHint};

use super::file_body::FileReadBody;
use super::file_complete::FileCompleteBody;
use super::metrics::RequestObservation;

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
    permit: Option<SpoolPermit>,
    file: Option<FileReadBody>,
    complete: Option<FileCompleteBody>,
    observation: Option<Arc<RequestObservation>>,
}

impl IcebergBody {
    pub(super) fn with_observation(mut self, observation: Arc<RequestObservation>) -> Self {
        self.observation = Some(observation);
        self
    }

    pub(super) fn with_spool_permit(mut self, permit: SpoolPermit) -> Self {
        self.permit = Some(permit);
        self
    }

    pub(super) fn new(bytes: Vec<u8>) -> Self {
        Self {
            bytes: Bytes::from(bytes),
            permit: None,
            file: None,
            complete: None,
            observation: None,
        }
    }
    pub(super) fn with_permit(bytes: Vec<u8>, permit: SpoolPermit) -> Self {
        Self {
            bytes: Bytes::from(bytes),
            permit: Some(permit),
            file: None,
            complete: None,
            observation: None,
        }
    }

    pub(super) fn file(body: FileReadBody) -> Self {
        Self {
            bytes: Bytes::new(),
            permit: None,
            file: Some(body),
            complete: None,
            observation: None,
        }
    }

    pub(super) fn complete(body: FileCompleteBody) -> Self {
        Self {
            bytes: Bytes::new(),
            permit: None,
            file: None,
            complete: Some(body),
            observation: None,
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
        let result = if let Some(complete) = &mut body.complete {
            Pin::new(complete)
                .poll_frame(context)
                .map(|frame| frame.map(|result| result.map_err(Into::into)))
        } else if let Some(file) = &mut body.file {
            Pin::new(file)
                .poll_frame(context)
                .map(|frame| frame.map(|result| result.map_err(Into::into)))
        } else if body.bytes.is_empty() {
            Poll::Ready(None)
        } else {
            let length = body.bytes.len().min(16 * 1024);
            Poll::Ready(Some(Ok(Frame::data(body.bytes.split_to(length)))))
        };
        if let Poll::Ready(Some(Ok(frame))) = &result {
            if let Some(bytes) = frame.data_ref() {
                if let Some(observation) = &body.observation {
                    observation.response_bytes(bytes.len());
                }
            }
        }
        result
    }

    fn is_end_stream(&self) -> bool {
        self.bytes.is_empty()
            && self.file.as_ref().map_or(true, Body::is_end_stream)
            && self.complete.as_ref().map_or(true, Body::is_end_stream)
    }
    fn size_hint(&self) -> SizeHint {
        if let Some(complete) = &self.complete {
            return complete.size_hint();
        }
        self.file
            .as_ref()
            .map_or_else(|| SizeHint::with_exact(self.bytes.len() as u64), Body::size_hint)
    }
}
