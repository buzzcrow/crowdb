use std::pin::Pin;
use std::task::{Context, Poll};

use crowdb_access_s3::auth::StreamingPayloadVerifier;
use hyper::body::{Body, Bytes, Frame};
use hyper::HeaderMap;

mod checksum;
mod chunks;

#[derive(Clone, Copy, Debug, thiserror::Error)]
pub enum FileEncodingError {
    #[error("invalid upload framing")]
    Framing,
    #[error("upload length exceeds its declared bounds")]
    Length,
    #[error("upload chunk signature is invalid")]
    Signature,
    #[error("upload checksum is invalid")]
    Checksum,
    #[error("upload transport failed")]
    Transport,
}

pub struct FileUploadBody<Input> {
    input: Input,
    buffered: Bytes,
    chunks: Option<chunks::Chunks>,
    checksum: Option<checksum::Checksum>,
    length: Option<u64>,
    wire_length: Option<u64>,
    wire_bytes: u64,
    max_wire_bytes: u64,
    done: bool,
    failure: Option<FileEncodingError>,
}

impl<Input> FileUploadBody<Input> {
    /// The streaming verifier must come from authenticating these exact headers.
    /// Returned data is staging input; only successful EOF authorizes publication.
    /// # Errors
    /// Rejects ambiguous framing, unsupported checksums and excessive encoded lengths.
    pub fn new(
        input: Input,
        headers: &HeaderMap,
        verifier: Option<StreamingPayloadVerifier>,
        max_wire_bytes: u64,
    ) -> Result<Self, FileEncodingError> {
        let wire_length = length_header(headers, "content-length")?;
        if wire_length.is_some_and(|length| length > max_wire_bytes) {
            return Err(FileEncodingError::Length);
        }
        let (length, chunks, checksum) = if let Some(verifier) = verifier {
            if header(headers, "content-encoding")? != Some("aws-chunked") {
                return Err(FileEncodingError::Framing);
            }
            let length =
                length_header(headers, "x-amz-decoded-content-length")?.ok_or(FileEncodingError::Framing)?;
            if length > max_wire_bytes {
                return Err(FileEncodingError::Length);
            }
            if !verifier.has_trailer() && headers.contains_key("x-amz-trailer") {
                return Err(FileEncodingError::Framing);
            }
            let checksum = checksum::Checksum::from_headers(headers, verifier.has_trailer())?;
            (
                Some(length),
                Some(chunks::Chunks::new(verifier, checksum, length)),
                None,
            )
        } else {
            if headers.contains_key("x-amz-decoded-content-length")
                || headers.contains_key("x-amz-trailer")
                || header(headers, "content-encoding")?
                    .is_some_and(|value| value.split(',').any(|encoding| encoding.trim() == "aws-chunked"))
            {
                return Err(FileEncodingError::Framing);
            }
            (
                wire_length,
                None,
                checksum::Checksum::from_headers(headers, false)?,
            )
        };
        Ok(Self {
            input,
            buffered: Bytes::new(),
            chunks,
            checksum,
            length,
            wire_length,
            wire_bytes: 0,
            max_wire_bytes,
            done: false,
            failure: None,
        })
    }

    #[must_use]
    pub const fn decoded_length(&self) -> Option<u64> {
        self.length
    }

    pub(super) const fn failure(&self) -> Option<FileEncodingError> {
        self.failure
    }

    fn finish(&self) -> Result<(), FileEncodingError> {
        if self.wire_length.is_some_and(|length| length != self.wire_bytes) {
            return Err(FileEncodingError::Length);
        }
        if let Some(chunks) = &self.chunks {
            chunks.finish()?;
        }
        if let Some(checksum) = &self.checksum {
            checksum.verify()?;
        }
        Ok(())
    }
}

impl<Input: Body<Data = Bytes> + Unpin> FileUploadBody<Input> {
    fn poll_data(&mut self, context: &mut Context<'_>) -> Poll<Result<Option<Bytes>, FileEncodingError>> {
        for _ in 0..64 {
            if !self.buffered.is_empty() {
                if let Some(chunks) = &mut self.chunks {
                    if let Some(bytes) = chunks.next(&mut self.buffered)? {
                        return Poll::Ready(Ok(Some(bytes)));
                    }
                } else {
                    let bytes = self.buffered.split_to(self.buffered.len().min(64 * 1024));
                    if let Some(checksum) = &mut self.checksum {
                        checksum.update(&bytes);
                    }
                    return Poll::Ready(Ok(Some(bytes)));
                }
            }
            match std::task::ready!(Pin::new(&mut self.input).poll_frame(context)) {
                Some(Ok(frame)) => {
                    self.buffered = frame.into_data().map_err(|_| FileEncodingError::Framing)?;
                    super::metrics::record_request_bytes(self.buffered.len());
                    self.wire_bytes = self
                        .wire_bytes
                        .checked_add(self.buffered.len() as u64)
                        .ok_or(FileEncodingError::Length)?;
                    if self.wire_bytes > self.max_wire_bytes {
                        return Poll::Ready(Err(FileEncodingError::Length));
                    }
                }
                Some(Err(_)) => return Poll::Ready(Err(FileEncodingError::Transport)),
                None => {
                    self.finish()?;
                    return Poll::Ready(Ok(None));
                }
            }
        }
        context.waker().wake_by_ref();
        Poll::Pending
    }
}

impl<Input: Body<Data = Bytes> + Unpin> Body for FileUploadBody<Input> {
    type Data = Bytes;
    type Error = FileEncodingError;

    fn poll_frame(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Self::Error>>> {
        let body = self.get_mut();
        if body.done {
            return Poll::Ready(None);
        }
        match std::task::ready!(body.poll_data(context)) {
            Ok(Some(bytes)) => Poll::Ready(Some(Ok(Frame::data(bytes)))),
            Ok(None) => {
                body.done = true;
                Poll::Ready(None)
            }
            Err(error) => {
                body.done = true;
                body.failure = Some(error);
                body.buffered = Bytes::new();
                Poll::Ready(Some(Err(error)))
            }
        }
    }

    fn is_end_stream(&self) -> bool {
        self.done
    }
}

fn header<'a>(headers: &'a HeaderMap, name: &str) -> Result<Option<&'a str>, FileEncodingError> {
    if headers.get_all(name).iter().count() > 1 {
        return Err(FileEncodingError::Framing);
    }
    headers
        .get(name)
        .map(|value| value.to_str().map_err(|_| FileEncodingError::Framing))
        .transpose()
}

fn length_header(headers: &HeaderMap, name: &str) -> Result<Option<u64>, FileEncodingError> {
    header(headers, name)?
        .map(|value| {
            if value.is_empty() || !value.bytes().all(|byte| byte.is_ascii_digit()) {
                return Err(FileEncodingError::Framing);
            }
            value.parse().map_err(|_| FileEncodingError::Length)
        })
        .transpose()
}
