use std::fmt::Write;
use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::task::Poll;
use std::time::Instant;

use crowdb_access_iceberg::catalog::CatalogContext;
use crowdb_access_iceberg::file::{
    FileBlockStore, FileIdentity, FileLocation, FileRecord, FileRepository, MultipartPart,
    MultipartRepository, MultipartSession, MultipartStreamPart,
};
use crowdb_access_s3::native_buffer::NativeBodyReceiver;
use crowdb_chunk_client::{FramedWriteBuffer, LargeWritePolicy};
use crowdb_protocol::frame::{FrameMagic, MAX_FRAME_PAYLOAD_BYTES};
use http_body_util::BodyExt;
use hyper::body::{Bytes, Incoming};
use tokio::sync::mpsc;

use super::metrics::{UploadFlowMetrics, UploadObservation};
use super::{
    admission_error, catalog_error, multipart, FileS3ErrorCode, FileTransferAdmission, FileUploadBody,
    FileUploadBudget,
};
use crate::upload_flow::digest_pipe::{DigestPipe, Digests};
use crate::upload_flow::{drive_transfer, write_buffers, OfferStatus, UploadBuffer, WriteFlow};

// Sixteen complete frames occupy one 1-MiB physical mirror strip. A full
// logical MiB would spill frame overhead into the next strip and serialize
// otherwise independent writes in the non-native receive path.
const TARGET_BUFFER_BYTES: usize = 16 * MAX_FRAME_PAYLOAD_BYTES;

struct WriteObject<'a> {
    body: FileUploadBody<Incoming>,
    object: object::ObjectWriter,
    frame_magic: FrameMagic,
    held_buffers: usize,
    admission: &'a FileTransferAdmission,
    receiver: Option<&'a NativeBodyReceiver>,
    owner: FileIdentity,
    location: FileLocation,
    declared_length: Option<u64>,
    expected_sha256: Option<[u8; 32]>,
    observation: UploadObservation,
    publication: Publication<'a>,
    metrics: &'a UploadFlowMetrics,
}

pub(super) enum Publication<'a> {
    Direct {
        repository: &'a FileRepository,
        context: CatalogContext,
    },
    Part {
        repository: &'a MultipartRepository,
        session: &'a MultipartSession,
        number: u16,
        now_ms: u64,
    },
}

pub(super) enum UploadedObject {
    Direct(FileRecord),
    Part(MultipartPart),
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn upload(
    blocks: &dyn FileBlockStore,
    budget: &FileUploadBudget,
    admission: &FileTransferAdmission,
    mut body: FileUploadBody<Incoming>,
    owner: FileIdentity,
    location: FileLocation,
    declared_length: Option<u64>,
    expected_sha256: Option<[u8; 32]>,
    native_receiver: Option<&NativeBodyReceiver>,
    small_threshold_exclusive: usize,
    large_write: &LargeWritePolicy,
    metrics: &Arc<UploadFlowMetrics>,
    publication: Publication<'_>,
) -> Result<UploadedObject, FileS3ErrorCode> {
    let _permit = budget.acquire().map_err(|_| FileS3ErrorCode::SlowDown)?;
    let small = declared_length
        .and_then(|length| usize::try_from(length).ok())
        .filter(|length| *length < small_threshold_exclusive);
    let native_small = small.is_some_and(|length| length > 0);
    let receiver =
        native_receiver.filter(|_| (small.is_none() || native_small) && body.native_handoff_eligible());
    let writer = blocks
        .prepare_upload_writer(&location.to_string(), small, declared_length, large_write)
        .await
        .map_err(|_| FileS3ErrorCode::SlowDown)?
        .ok_or(FileS3ErrorCode::SlowDown)?;
    if let Some(receiver) = receiver {
        receiver
            .enable_owner_handoff_with_magic(if small.is_some() {
                FrameMagic::RepoSmallV1
            } else {
                FrameMagic::RepoLargeV1
            })
            .map_err(|_| FileS3ErrorCode::InternalError)?;
    }
    body.defer_md5();
    let object = WriteObject {
        body,
        object: object::ObjectWriter::new(writer, small.is_some(), expected_sha256.is_some())
            .map_err(|()| FileS3ErrorCode::InternalError)?,
        frame_magic: if small.is_some() {
            FrameMagic::RepoSmallV1
        } else {
            FrameMagic::RepoLargeV1
        },
        held_buffers: large_write.client.large_held_buffers,
        admission,
        receiver,
        owner,
        location,
        declared_length,
        expected_sha256,
        observation: metrics.start(),
        publication,
        metrics,
    };
    object.run().await
}

mod object;
mod small_receive;

impl WriteObject<'_> {
    async fn run(mut self) -> Result<UploadedObject, FileS3ErrorCode> {
        let transfer = self.transfer().await;
        let (length, written) = match transfer {
            Ok(result) => result,
            Err(error) => {
                let _ = self.object.digest_mut().finish().await;
                let _ = self.object.writer_mut().on_error().await;
                self.observation.complete(false);
                return Err(error);
            }
        };
        self.observation.writer_feeds(written.feeds, written.feed_time);
        self.observation
            .writer_capacity_waits(written.capacity_waits, written.capacity_wait_time);
        let digest_started = Instant::now();
        let digest = self
            .object
            .digest_mut()
            .finish()
            .await
            .map_err(|()| FileS3ErrorCode::InternalError);
        self.observation.digest_finish(digest_started.elapsed());
        let etag = digest.and_then(|digest| self.validate_digest(&digest));
        let etag = match etag {
            Ok(etag) => etag,
            Err(error) => {
                let _ = self.object.writer_mut().on_error().await;
                self.observation.complete(false);
                return Err(error);
            }
        };
        let started = Instant::now();
        let locations = self
            .object
            .writer_mut()
            .on_finish()
            .await
            .map_err(|_| FileS3ErrorCode::SlowDown);
        self.observation.writer_finish(started.elapsed());
        if let Some(timing) = self.object.writer_mut().write_timing() {
            self.observation.chunk_write_timing(timing);
        }
        let locations = match locations {
            Ok(locations) => locations,
            Err(error) => {
                let _ = self.object.writer_mut().on_error().await;
                self.observation.complete(false);
                return Err(error);
            }
        };
        let record = FileRecord::from_uploaded_locations(
            self.owner.file,
            self.location.clone(),
            &locations,
            length,
            etag,
        )
        .map_err(|_| FileS3ErrorCode::InternalError);
        let record = match record {
            Ok(record) => record,
            Err(error) => {
                self.observation.complete(false);
                return Err(error);
            }
        };
        let published = self.publish(record).await;
        self.observation.complete(published.is_ok());
        published
    }

    async fn transfer(&mut self) -> Result<(u64, crate::upload_flow::WriteStats), FileS3ErrorCode> {
        if self.frame_magic == FrameMagic::RepoSmallV1 {
            if let Some(receiver) = self.receiver {
                let (writer, digest) = self.object.parts_mut();
                return small_receive::receive(
                    &mut self.body,
                    writer,
                    digest,
                    self.admission,
                    receiver,
                    self.declared_length,
                    &mut self.observation,
                )
                .await;
            }
        }
        // One upload task polls both sides before yielding.
        let (sender, receiver) = mpsc::channel(self.held_buffers);
        let progress = AtomicU64::new(0);
        let queued_peak = AtomicU64::new(0);
        let flow = WriteFlow::new(sender, &progress, &queued_peak);
        let target_buffer = usize::try_from(self.declared_length.unwrap_or(TARGET_BUFFER_BYTES as u64))
            .unwrap_or(TARGET_BUFFER_BYTES)
            .clamp(1, TARGET_BUFFER_BYTES);
        let (writer, digest) = self.object.parts_mut();
        let receive = receive_body(
            &mut self.body,
            digest,
            self.admission,
            self.receiver,
            self.declared_length,
            target_buffer,
            self.frame_magic,
            flow,
            &mut self.observation,
        );
        let write = write_buffers(writer, receiver, &progress, |_| FileS3ErrorCode::SlowDown);
        let result = drive_transfer(receive, write, &progress).await;
        self.observation
            .queued_buffers_peak(queued_peak.load(Ordering::Relaxed));
        result
    }

    fn validate_digest(&mut self, digest: &Digests) -> Result<String, FileS3ErrorCode> {
        self.observation.digest_process(digest.process_time);
        self.body
            .verify_deferred_md5(digest.md5)
            .map_err(multipart::encoding_error)?;
        if self
            .expected_sha256
            .is_some_and(|expected| digest.sha256 != Some(expected))
        {
            return Err(FileS3ErrorCode::InvalidRequest);
        }
        let mut etag = String::with_capacity(32);
        for byte in digest.md5 {
            write!(&mut etag, "{byte:02x}").expect("string write cannot fail");
        }
        Ok(etag)
    }

    async fn publish(&mut self, record: FileRecord) -> Result<UploadedObject, FileS3ErrorCode> {
        let started = Instant::now();
        let published = match &self.publication {
            Publication::Direct { repository, context } => repository
                .publish(*context, &record)
                .await
                .map(UploadedObject::Direct)
                .map_err(catalog_error),
            Publication::Part {
                repository,
                session,
                number,
                now_ms,
            } => {
                let part = MultipartPart {
                    upload: session.upload,
                    number: *number,
                    revision: 1,
                    modified_ms: *now_ms,
                    owner: self.owner,
                    tree: None,
                    stream: Some(MultipartStreamPart {
                        length: record.length,
                        content: record.content,
                    }),
                };
                repository
                    .put_stream_part(session, &part, *now_ms)
                    .await
                    .map_err(catalog_error)
                    .and_then(|part| part.map(UploadedObject::Part).ok_or(FileS3ErrorCode::SlowDown))
            }
        };
        self.metrics.publication(started.elapsed());
        published
    }
}

#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
async fn receive_body(
    body: &mut FileUploadBody<Incoming>,
    digest: &mut DigestPipe,
    admission: &FileTransferAdmission,
    native_receiver: Option<&NativeBodyReceiver>,
    declared_length: Option<u64>,
    target_buffer: usize,
    frame_magic: FrameMagic,
    flow: WriteFlow<'_>,
    observation: &mut UploadObservation,
) -> Result<u64, FileS3ErrorCode> {
    let mut length = 0u64;
    let mut pending = Vec::with_capacity(if native_receiver.is_some() {
        0
    } else {
        target_buffer
    });
    let mut digest_pending = Vec::with_capacity(16);
    loop {
        let started = Instant::now();
        let mut frame = std::pin::pin!(body.frame());
        let mut waited = None;
        let next = std::future::poll_fn(|cx| match frame.as_mut().poll(cx) {
            Poll::Pending => {
                waited.get_or_insert_with(Instant::now);
                Poll::Pending
            }
            Poll::Ready(value) => Poll::Ready(value),
        })
        .await;
        if let Some(waited) = waited {
            observation.body_wait(waited.elapsed());
        }
        observation.body_poll(started.elapsed(), next.is_some());
        let Some(frame) = next else { break };
        let mut bytes = frame
            .map_err(multipart::encoding_error)?
            .into_data()
            .map_err(|_| FileS3ErrorCode::InvalidRequest)?;
        if bytes.is_empty() {
            continue;
        }
        observation.payload(bytes.len());
        length = length
            .checked_add(bytes.len() as u64)
            .ok_or(FileS3ErrorCode::EntityTooLarge)?;
        admission.check_bytes(length, length).map_err(admission_error)?;
        if declared_length.is_some_and(|declared| length > declared) {
            return Err(FileS3ErrorCode::InvalidRequest);
        }
        if let Some(receiver) = native_receiver {
            digest_pending.push(bytes);
            if let Some(mut owner) = receiver.take_ready_owner() {
                let started = Instant::now();
                owner
                    .prepare_frames(frame_magic, super::now_ms()?)
                    .map_err(|_| FileS3ErrorCode::InternalError)?;
                observation.frame_prepare(owner.frame_count(), started.elapsed());
                handoff(
                    digest,
                    &flow,
                    UploadBuffer::Framed(Box::new(owner)),
                    std::mem::take(&mut digest_pending),
                    observation,
                )
                .await?;
            }
        } else {
            while !bytes.is_empty() {
                let count = (target_buffer - pending.len()).min(bytes.len());
                let piece = bytes.split_to(count);
                pending.extend_from_slice(&piece);
                digest_pending.push(piece);
                if pending.len() == target_buffer {
                    handoff(
                        digest,
                        &flow,
                        UploadBuffer::Data(Bytes::from(std::mem::replace(
                            &mut pending,
                            Vec::with_capacity(target_buffer),
                        ))),
                        std::mem::take(&mut digest_pending),
                        observation,
                    )
                    .await?;
                }
            }
        }
    }
    if let Some(receiver) = native_receiver {
        if let Some(mut owner) = receiver
            .finish_owner_when_ready()
            .await
            .map_err(|_| FileS3ErrorCode::InvalidRequest)?
        {
            let started = Instant::now();
            owner
                .prepare_frames(frame_magic, super::now_ms()?)
                .map_err(|_| FileS3ErrorCode::InternalError)?;
            observation.frame_prepare(owner.frame_count(), started.elapsed());
            handoff(
                digest,
                &flow,
                UploadBuffer::Framed(Box::new(owner)),
                std::mem::take(&mut digest_pending),
                observation,
            )
            .await?;
        }
    }
    if !pending.is_empty() {
        handoff(
            digest,
            &flow,
            UploadBuffer::Data(Bytes::from(pending)),
            std::mem::take(&mut digest_pending),
            observation,
        )
        .await?;
    }
    if declared_length.is_some_and(|declared| declared != length) {
        return Err(FileS3ErrorCode::InvalidRequest);
    }
    Ok(length)
}

async fn handoff(
    digest: &mut DigestPipe,
    flow: &WriteFlow<'_>,
    buffer: UploadBuffer,
    payload: Vec<Bytes>,
    observation: &mut UploadObservation,
) -> Result<(), FileS3ErrorCode> {
    let offer_status = flow.offer(buffer).await.map_err(|()| FileS3ErrorCode::SlowDown)?;
    let started = Instant::now();
    digest
        .enqueue(payload)
        .map_err(|()| FileS3ErrorCode::InternalError)?;
    observation.digest_enqueue(started.elapsed());
    match offer_status {
        OfferStatus::Continue => Ok(()),
        OfferStatus::Pause => {
            let started = Instant::now();
            flow.wait_ready().await.map_err(|()| FileS3ErrorCode::SlowDown)?;
            observation.write_flow_pause(started.elapsed());
            Ok(())
        }
    }
}
