use std::fmt::Write;
use std::future::{poll_fn, Future};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::task::Poll;
use std::time::{Duration, Instant};

use crowdb_access_iceberg::catalog::CatalogContext;
use crowdb_access_iceberg::file::{
    FileBlockStore, FileIdentity, FileLocation, FileRecord, FileRepository, MultipartPart,
    MultipartRepository, MultipartSession, MultipartStreamPart,
};
use crowdb_access_iceberg::storage::IcebergFileWriter;
use crowdb_access_s3::native_buffer::NativeBodyReceiver;
use crowdb_chunk_client::{FramedWriteBuffer, LargeWritePolicy};
use crowdb_protocol::frame::FrameMagic;
use http_body_util::BodyExt;
use hyper::body::{Bytes, Incoming};
use tokio::sync::mpsc;

use super::digest_pipe::DigestPipe;
use super::metrics::{UploadFlowMetrics, UploadObservation};
use super::{
    admission_error, catalog_error, multipart, FileS3ErrorCode, FileTransferAdmission, FileUploadBody,
    FileUploadBudget,
};

const TARGET_BUFFER_BYTES: usize = 1024 * 1024;

enum UploadBuffer {
    Framed(Box<dyn FramedWriteBuffer>),
    Data(Bytes),
}

enum OfferStatus {
    Continue,
    Pause,
}

struct WriteFlow<'a> {
    sender: mpsc::Sender<UploadBuffer>,
    progress: &'a AtomicU64,
}

impl WriteFlow<'_> {
    async fn offer(&self, buffer: UploadBuffer) -> Result<OfferStatus, FileS3ErrorCode> {
        self.sender
            .send(buffer)
            .await
            .map_err(|_| FileS3ErrorCode::SlowDown)?;
        self.progress.fetch_add(1, Ordering::Relaxed);
        Ok(if self.sender.capacity() == 0 {
            OfferStatus::Pause
        } else {
            OfferStatus::Continue
        })
    }

    async fn wait_ready(&self) -> Result<(), FileS3ErrorCode> {
        let permit = self
            .sender
            .reserve()
            .await
            .map_err(|_| FileS3ErrorCode::SlowDown)?;
        drop(permit);
        Ok(())
    }
}

struct WrittenObject {
    locations: Vec<crowdb_protocol::chunkdb::rpc::Location>,
    feeds: u64,
    feed_time: Duration,
    capacity_waits: u64,
    capacity_wait_time: Duration,
    finish_time: Duration,
}

struct WriteObject<'a> {
    body: FileUploadBody<Incoming>,
    writer: IcebergFileWriter,
    digest: DigestPipe,
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
    let receiver = native_receiver.filter(|_| small.is_none() && body.native_handoff_eligible());
    let writer = blocks
        .prepare_upload_writer(&location.to_string(), small, declared_length, large_write)
        .await
        .map_err(|_| FileS3ErrorCode::SlowDown)?
        .ok_or(FileS3ErrorCode::SlowDown)?;
    if let Some(receiver) = receiver {
        receiver.enable_owner_handoff();
    }
    body.defer_md5();
    WriteObject {
        body,
        writer,
        digest: DigestPipe::start(expected_sha256.is_some()),
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
    }
    .run()
    .await
}

impl WriteObject<'_> {
    async fn run(mut self) -> Result<UploadedObject, FileS3ErrorCode> {
        // The current upload task drives both sides before it yields. One
        // queued owner plus one owner in the writer bounds receive-ahead.
        let (sender, receiver) = mpsc::channel(self.held_buffers);
        let progress = AtomicU64::new(0);
        let flow = WriteFlow {
            sender,
            progress: &progress,
        };
        let target_buffer = usize::try_from(self.declared_length.unwrap_or(TARGET_BUFFER_BYTES as u64))
            .unwrap_or(TARGET_BUFFER_BYTES)
            .clamp(1, TARGET_BUFFER_BYTES);
        let receive = receive_body(
            &mut self.body,
            &mut self.digest,
            self.admission,
            self.receiver,
            self.declared_length,
            target_buffer,
            flow,
            &mut self.observation,
        );
        let write = write_buffers(&mut self.writer, receiver, &progress);
        let transfer = drive_transfer(receive, write, &progress).await;
        let (length, written) = match transfer {
            Ok(result) => result,
            Err(error) => {
                let _ = self.digest.finish().await;
                let _ = self.writer.on_error().await;
                self.observation.complete(false);
                return Err(error);
            }
        };
        self.observation.writer_feeds(written.feeds, written.feed_time);
        if let Some(timing) = self.writer.write_timing() {
            self.observation.chunk_write_timing(timing);
        }
        self.observation
            .writer_capacity_waits(written.capacity_waits, written.capacity_wait_time);
        self.observation.writer_finish(written.finish_time);
        let digest_started = Instant::now();
        let digest = self
            .digest
            .finish()
            .await
            .map_err(|()| FileS3ErrorCode::InternalError);
        self.observation.digest_finish(digest_started.elapsed());
        let result = (|| {
            let digest = digest?;
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
            FileRecord::from_uploaded_locations(
                self.owner.file,
                self.location.clone(),
                &written.locations,
                length,
                etag,
            )
            .map_err(|_| FileS3ErrorCode::InternalError)
        })();
        if result.is_err() {
            let _ = self.writer.on_error().await;
        }
        let record = match result {
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

async fn drive_transfer<R, W>(
    receive: R,
    write: W,
    progress: &AtomicU64,
) -> Result<(u64, WrittenObject), FileS3ErrorCode>
where
    R: Future<Output = Result<u64, FileS3ErrorCode>>,
    W: Future<Output = Result<WrittenObject, FileS3ErrorCode>>,
{
    let mut receive = Some(Box::pin(receive));
    let mut write = Some(Box::pin(write));
    let mut length = None;
    let mut written = None;
    poll_fn(|cx| {
        for _ in 0..32 {
            let before = progress.load(Ordering::Relaxed);
            if let Some(Poll::Ready(result)) = receive.as_mut().map(|future| future.as_mut().poll(cx)) {
                match result {
                    Ok(value) => length = Some(value),
                    Err(error) => return Poll::Ready(Err(error)),
                }
                // Dropping the producer closes the write queue at EOF.
                receive = None;
            }
            if let Some(Poll::Ready(result)) = write.as_mut().map(|future| future.as_mut().poll(cx)) {
                match result {
                    Ok(value) => written = Some(value),
                    Err(error) => return Poll::Ready(Err(error)),
                }
                write = None;
            }
            if let Some(length) = length {
                if let Some(written) = written.take() {
                    return Poll::Ready(Ok((length, written)));
                }
            }
            if progress.load(Ordering::Relaxed) == before {
                return Poll::Pending;
            }
        }
        cx.waker().wake_by_ref();
        Poll::Pending
    })
    .await
}

#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
async fn receive_body(
    body: &mut FileUploadBody<Incoming>,
    digest: &mut DigestPipe,
    admission: &FileTransferAdmission,
    native_receiver: Option<&NativeBodyReceiver>,
    declared_length: Option<u64>,
    target_buffer: usize,
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
        let next = body.frame().await;
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
                    .prepare_frames(FrameMagic::RepoLargeV1, super::now_ms()?)
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
                .prepare_frames(FrameMagic::RepoLargeV1, super::now_ms()?)
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
    digest: &DigestPipe,
    flow: &WriteFlow<'_>,
    buffer: UploadBuffer,
    payload: Vec<Bytes>,
    observation: &mut UploadObservation,
) -> Result<(), FileS3ErrorCode> {
    let offer_status = flow.offer(buffer).await?;
    let started = Instant::now();
    digest
        .enqueue(payload)
        .map_err(|()| FileS3ErrorCode::InternalError)?;
    observation.digest_enqueue(started.elapsed());
    match offer_status {
        OfferStatus::Continue => Ok(()),
        OfferStatus::Pause => {
            let started = Instant::now();
            flow.wait_ready().await?;
            observation.write_flow_pause(started.elapsed());
            Ok(())
        }
    }
}

async fn write_buffers(
    writer: &mut IcebergFileWriter,
    mut receiver: mpsc::Receiver<UploadBuffer>,
    progress: &AtomicU64,
) -> Result<WrittenObject, FileS3ErrorCode> {
    let mut feed_time = Duration::ZERO;
    let mut feeds = 0;
    let mut capacity_waits = 0;
    let mut capacity_wait_time = Duration::ZERO;
    loop {
        while !writer.require_data() && !writer.input_complete() {
            let started = Instant::now();
            writer.wait_for_capacity().await;
            capacity_waits += 1;
            capacity_wait_time += started.elapsed();
        }
        let Some(buffer) = receiver.recv().await else {
            break;
        };
        progress.fetch_add(1, Ordering::Relaxed);
        let started = Instant::now();
        match buffer {
            UploadBuffer::Framed(owner) => writer.on_framed_data(owner).await,
            UploadBuffer::Data(bytes) => writer.on_data(bytes).await,
        }
        .map_err(|_| FileS3ErrorCode::SlowDown)?;
        feed_time += started.elapsed();
        feeds += 1;
    }
    let started = Instant::now();
    let result = writer.on_finish().await.map_err(|_| FileS3ErrorCode::SlowDown)?;
    Ok(WrittenObject {
        locations: result,
        feeds,
        feed_time,
        capacity_waits,
        capacity_wait_time,
        finish_time: started.elapsed(),
    })
}
