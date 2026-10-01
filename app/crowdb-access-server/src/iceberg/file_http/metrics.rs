use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, serde::Serialize)]
pub struct UploadFlowSnapshot {
    pub attempts: u64,
    pub completed: u64,
    pub failed: u64,
    pub cancelled: u64,
    pub logical_bytes: u64,
    pub body_frames: u64,
    pub body_poll_ns: u64,
    pub frames_prepared: u64,
    pub frame_prepare_ns: u64,
    pub digest_enqueues: u64,
    pub digest_enqueue_ns: u64,
    pub digest_process_ns: u64,
    pub write_flow_pauses: u64,
    pub write_flow_pause_ns: u64,
    pub writer_feeds: u64,
    pub writer_feed_ns: u64,
    pub strip_prepare_waits: u64,
    pub strip_prepare_wait_ns: u64,
    pub strip_write_successes: u64,
    pub strip_write_success_ns: u64,
    pub strip_write_success_max_ns: u64,
    pub writer_capacity_waits: u64,
    pub writer_capacity_wait_ns: u64,
    pub writer_finish_ns: u64,
    pub digest_finish_ns: u64,
    pub publication_attempts: u64,
    pub publication_ns: u64,
    pub multipart_completions: u64,
    pub multipart_complete_ns: u64,
    pub transfer_ns: u64,
}

#[derive(Default)]
pub(super) struct UploadFlowMetrics {
    attempts: AtomicU64,
    completed: AtomicU64,
    failed: AtomicU64,
    cancelled: AtomicU64,
    logical_bytes: AtomicU64,
    body_frames: AtomicU64,
    body_poll_ns: AtomicU64,
    frames_prepared: AtomicU64,
    frame_prepare_ns: AtomicU64,
    digest_enqueues: AtomicU64,
    digest_enqueue_ns: AtomicU64,
    digest_process_ns: AtomicU64,
    write_flow_pauses: AtomicU64,
    write_flow_pause_ns: AtomicU64,
    writer_feeds: AtomicU64,
    writer_feed_ns: AtomicU64,
    strip_prepare_waits: AtomicU64,
    strip_prepare_wait_ns: AtomicU64,
    strip_write_successes: AtomicU64,
    strip_write_success_ns: AtomicU64,
    strip_write_success_max_ns: AtomicU64,
    writer_capacity_waits: AtomicU64,
    writer_capacity_wait_ns: AtomicU64,
    writer_finish_ns: AtomicU64,
    digest_finish_ns: AtomicU64,
    publication_attempts: AtomicU64,
    publication_ns: AtomicU64,
    multipart_completions: AtomicU64,
    multipart_complete_ns: AtomicU64,
    transfer_ns: AtomicU64,
}

impl UploadFlowMetrics {
    pub(super) fn start(self: &Arc<Self>) -> UploadObservation {
        UploadObservation {
            metrics: Arc::clone(self),
            started: Instant::now(),
            outcome: None,
            sample: UploadFlowSnapshot::default(),
        }
    }

    pub(super) fn publication(&self, elapsed: Duration) {
        self.publication_attempts.fetch_add(1, Ordering::Relaxed);
        self.publication_ns.fetch_add(nanos(elapsed), Ordering::Relaxed);
    }

    pub(super) fn multipart_complete(&self, elapsed: Duration) {
        self.multipart_completions.fetch_add(1, Ordering::Relaxed);
        self.multipart_complete_ns
            .fetch_add(nanos(elapsed), Ordering::Relaxed);
    }

    pub(super) fn snapshot(&self) -> UploadFlowSnapshot {
        UploadFlowSnapshot {
            attempts: self.attempts.load(Ordering::Relaxed),
            completed: self.completed.load(Ordering::Relaxed),
            failed: self.failed.load(Ordering::Relaxed),
            cancelled: self.cancelled.load(Ordering::Relaxed),
            logical_bytes: self.logical_bytes.load(Ordering::Relaxed),
            body_frames: self.body_frames.load(Ordering::Relaxed),
            body_poll_ns: self.body_poll_ns.load(Ordering::Relaxed),
            frames_prepared: self.frames_prepared.load(Ordering::Relaxed),
            frame_prepare_ns: self.frame_prepare_ns.load(Ordering::Relaxed),
            digest_enqueues: self.digest_enqueues.load(Ordering::Relaxed),
            digest_enqueue_ns: self.digest_enqueue_ns.load(Ordering::Relaxed),
            digest_process_ns: self.digest_process_ns.load(Ordering::Relaxed),
            write_flow_pauses: self.write_flow_pauses.load(Ordering::Relaxed),
            write_flow_pause_ns: self.write_flow_pause_ns.load(Ordering::Relaxed),
            writer_feeds: self.writer_feeds.load(Ordering::Relaxed),
            writer_feed_ns: self.writer_feed_ns.load(Ordering::Relaxed),
            strip_prepare_waits: self.strip_prepare_waits.load(Ordering::Relaxed),
            strip_prepare_wait_ns: self.strip_prepare_wait_ns.load(Ordering::Relaxed),
            strip_write_successes: self.strip_write_successes.load(Ordering::Relaxed),
            strip_write_success_ns: self.strip_write_success_ns.load(Ordering::Relaxed),
            strip_write_success_max_ns: self.strip_write_success_max_ns.load(Ordering::Relaxed),
            writer_capacity_waits: self.writer_capacity_waits.load(Ordering::Relaxed),
            writer_capacity_wait_ns: self.writer_capacity_wait_ns.load(Ordering::Relaxed),
            writer_finish_ns: self.writer_finish_ns.load(Ordering::Relaxed),
            digest_finish_ns: self.digest_finish_ns.load(Ordering::Relaxed),
            publication_attempts: self.publication_attempts.load(Ordering::Relaxed),
            publication_ns: self.publication_ns.load(Ordering::Relaxed),
            multipart_completions: self.multipart_completions.load(Ordering::Relaxed),
            multipart_complete_ns: self.multipart_complete_ns.load(Ordering::Relaxed),
            transfer_ns: self.transfer_ns.load(Ordering::Relaxed),
        }
    }
}

pub(super) struct UploadObservation {
    metrics: Arc<UploadFlowMetrics>,
    started: Instant,
    outcome: Option<bool>,
    sample: UploadFlowSnapshot,
}

impl UploadObservation {
    pub(super) fn body_poll(&mut self, elapsed: Duration, has_frame: bool) {
        self.sample.body_poll_ns += nanos(elapsed);
        self.sample.body_frames += u64::from(has_frame);
    }

    pub(super) fn payload(&mut self, bytes: usize) {
        self.sample.logical_bytes += bytes as u64;
    }

    pub(super) fn frame_prepare(&mut self, frames: usize, elapsed: Duration) {
        self.sample.frames_prepared += u64::try_from(frames).unwrap_or(u64::MAX);
        self.sample.frame_prepare_ns += nanos(elapsed);
    }

    pub(super) fn digest_enqueue(&mut self, elapsed: Duration) {
        self.sample.digest_enqueues += 1;
        self.sample.digest_enqueue_ns += nanos(elapsed);
    }

    pub(super) fn digest_process(&mut self, elapsed: Duration) {
        self.sample.digest_process_ns += nanos(elapsed);
    }

    pub(super) fn write_flow_pause(&mut self, elapsed: Duration) {
        self.sample.write_flow_pauses += 1;
        self.sample.write_flow_pause_ns += nanos(elapsed);
    }

    pub(super) fn writer_feeds(&mut self, count: u64, elapsed: Duration) {
        self.sample.writer_feeds += count;
        self.sample.writer_feed_ns += nanos(elapsed);
    }

    pub(super) fn chunk_write_timing(&mut self, timing: crowdb_chunk_client::ChunkWriteTiming) {
        self.sample.strip_prepare_waits += timing.strip_prepare_waits;
        self.sample.strip_prepare_wait_ns += nanos(timing.strip_prepare_wait_time);
        self.sample.strip_write_successes += timing.strip_write_successes;
        self.sample.strip_write_success_ns += nanos(timing.strip_write_success_time);
        self.sample.strip_write_success_max_ns = self
            .sample
            .strip_write_success_max_ns
            .max(nanos(timing.strip_write_success_max));
    }

    pub(super) fn writer_capacity_waits(&mut self, count: u64, elapsed: Duration) {
        self.sample.writer_capacity_waits += count;
        self.sample.writer_capacity_wait_ns += nanos(elapsed);
    }

    pub(super) fn writer_finish(&mut self, elapsed: Duration) {
        self.sample.writer_finish_ns += nanos(elapsed);
    }

    pub(super) fn digest_finish(&mut self, elapsed: Duration) {
        self.sample.digest_finish_ns += nanos(elapsed);
    }

    pub(super) fn complete(&mut self, success: bool) {
        self.outcome = Some(success);
    }
}

impl Drop for UploadObservation {
    fn drop(&mut self) {
        self.metrics.attempts.fetch_add(1, Ordering::Relaxed);
        match self.outcome {
            Some(true) => &self.metrics.completed,
            Some(false) => &self.metrics.failed,
            None => &self.metrics.cancelled,
        }
        .fetch_add(1, Ordering::Relaxed);
        self.metrics
            .logical_bytes
            .fetch_add(self.sample.logical_bytes, Ordering::Relaxed);
        self.metrics
            .body_frames
            .fetch_add(self.sample.body_frames, Ordering::Relaxed);
        self.metrics
            .body_poll_ns
            .fetch_add(self.sample.body_poll_ns, Ordering::Relaxed);
        self.metrics
            .frames_prepared
            .fetch_add(self.sample.frames_prepared, Ordering::Relaxed);
        self.metrics
            .frame_prepare_ns
            .fetch_add(self.sample.frame_prepare_ns, Ordering::Relaxed);
        self.metrics
            .digest_enqueues
            .fetch_add(self.sample.digest_enqueues, Ordering::Relaxed);
        self.metrics
            .digest_enqueue_ns
            .fetch_add(self.sample.digest_enqueue_ns, Ordering::Relaxed);
        self.metrics
            .digest_process_ns
            .fetch_add(self.sample.digest_process_ns, Ordering::Relaxed);
        self.metrics
            .write_flow_pauses
            .fetch_add(self.sample.write_flow_pauses, Ordering::Relaxed);
        self.metrics
            .write_flow_pause_ns
            .fetch_add(self.sample.write_flow_pause_ns, Ordering::Relaxed);
        self.metrics
            .writer_feeds
            .fetch_add(self.sample.writer_feeds, Ordering::Relaxed);
        self.metrics
            .writer_feed_ns
            .fetch_add(self.sample.writer_feed_ns, Ordering::Relaxed);
        self.metrics
            .strip_prepare_waits
            .fetch_add(self.sample.strip_prepare_waits, Ordering::Relaxed);
        self.metrics
            .strip_prepare_wait_ns
            .fetch_add(self.sample.strip_prepare_wait_ns, Ordering::Relaxed);
        self.metrics
            .strip_write_successes
            .fetch_add(self.sample.strip_write_successes, Ordering::Relaxed);
        self.metrics
            .strip_write_success_ns
            .fetch_add(self.sample.strip_write_success_ns, Ordering::Relaxed);
        self.metrics
            .strip_write_success_max_ns
            .fetch_max(self.sample.strip_write_success_max_ns, Ordering::Relaxed);
        self.metrics
            .writer_capacity_waits
            .fetch_add(self.sample.writer_capacity_waits, Ordering::Relaxed);
        self.metrics
            .writer_capacity_wait_ns
            .fetch_add(self.sample.writer_capacity_wait_ns, Ordering::Relaxed);
        self.metrics
            .writer_finish_ns
            .fetch_add(self.sample.writer_finish_ns, Ordering::Relaxed);
        self.metrics
            .digest_finish_ns
            .fetch_add(self.sample.digest_finish_ns, Ordering::Relaxed);
        self.metrics
            .transfer_ns
            .fetch_add(nanos(self.started.elapsed()), Ordering::Relaxed);
    }
}

fn nanos(duration: Duration) -> u64 {
    duration.as_nanos().try_into().unwrap_or(u64::MAX)
}
