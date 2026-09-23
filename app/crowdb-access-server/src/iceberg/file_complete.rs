use std::convert::Infallible;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use crowdb_access_iceberg::key::OperationId;
use hyper::body::{Body, Bytes, Frame, SizeHint};
use tokio::time::{Instant, Sleep};

use super::file_response::{FileResponseError, FileS3ErrorCode, MultipartResponses};

const XML_PREFIX: &[u8] = b"<?xml version=\"1.0\" encoding=\"UTF-8\"?>";
type Completion = Pin<Box<dyn Future<Output = Result<Vec<u8>, FileS3ErrorCode>> + Send>>;

pub struct FileCompleteBody {
    completion: Option<Completion>,
    heartbeat: Pin<Box<Sleep>>,
    deadline: Pin<Box<Sleep>>,
    interval: Duration,
    resource: String,
    started: bool,
}

impl FileCompleteBody {
    /// # Errors
    /// Rejects unbounded resource names and invalid heartbeat or work limits.
    pub fn new(
        completion: impl Future<Output = Result<Vec<u8>, FileS3ErrorCode>> + Send + 'static,
        resource: &str,
        interval: Duration,
        timeout: Duration,
    ) -> Result<Self, FileResponseError> {
        if resource.len() > 2048
            || interval.is_zero()
            || interval > Duration::from_secs(30)
            || timeout <= interval
            || timeout > Duration::from_secs(300)
        {
            return Err(FileResponseError::Invalid);
        }
        Ok(Self {
            completion: Some(Box::pin(completion)),
            heartbeat: Box::pin(tokio::time::sleep(interval)),
            deadline: Box::pin(tokio::time::sleep(timeout)),
            interval,
            resource: resource.to_owned(),
            started: false,
        })
    }

    fn finish(&mut self, result: Result<Vec<u8>, FileS3ErrorCode>) -> Bytes {
        self.completion = None;
        let bytes = result.unwrap_or_else(|code| {
            MultipartResponses::error(code, &self.resource, &OperationId::random().to_string())
                .expect("validated resource and fixed-size request ID")
                .into_body()
        });
        let bytes = Bytes::from(bytes);
        if bytes.starts_with(XML_PREFIX) {
            bytes.slice(XML_PREFIX.len()..)
        } else {
            bytes
        }
    }
}

impl Body for FileCompleteBody {
    type Data = Bytes;
    type Error = Infallible;

    fn poll_frame(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Self::Error>>> {
        let body = self.get_mut();
        if body.completion.is_none() {
            return Poll::Ready(None);
        }
        if !body.started {
            body.started = true;
            return Poll::Ready(Some(Ok(Frame::data(Bytes::from_static(XML_PREFIX)))));
        }
        let result = if body.deadline.as_mut().poll(context).is_ready() {
            Poll::Ready(Err(FileS3ErrorCode::SlowDown))
        } else {
            body.completion.as_mut().unwrap().as_mut().poll(context)
        };
        if let Poll::Ready(result) = result {
            return Poll::Ready(Some(Ok(Frame::data(body.finish(result)))));
        }
        if body.heartbeat.as_mut().poll(context).is_ready() {
            body.heartbeat.as_mut().reset(Instant::now() + body.interval);
            return Poll::Ready(Some(Ok(Frame::data(Bytes::from_static(b"\n")))));
        }
        Poll::Pending
    }

    fn is_end_stream(&self) -> bool {
        self.completion.is_none()
    }

    fn size_hint(&self) -> SizeHint {
        if self.is_end_stream() {
            SizeHint::with_exact(0)
        } else {
            SizeHint::default()
        }
    }
}
