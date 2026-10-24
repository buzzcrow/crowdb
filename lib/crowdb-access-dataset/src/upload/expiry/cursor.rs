use std::collections::VecDeque;

use super::super::DatasetUploads;
use super::super::UploadError;
use std::time::Duration;

const MAX_PENDING: usize = 32;
pub(super) const PART_READS: usize = 32;
pub(super) const RANGE_NOTIFICATIONS: usize = 64;

/// Volatile scheduling hints only. Dropping this cursor restarts discovery;
/// upload authorities and exact part ranges remain the cleanup obligations.
#[derive(Debug, Default)]
pub struct UploadExpiryCursor {
    pub(super) after: Option<Vec<u8>>,
    pub(super) pending: VecDeque<ExpiredUpload>,
}

#[derive(Debug)]
pub(super) struct ExpiredUpload {
    pub location: Vec<u8>,
    pub parts: Option<u32>,
    pub after: Option<Vec<u8>>,
    pub clear: bool,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct UploadExpiryStep {
    pub scanned: usize,
    pub part_reads: usize,
    pub range_notifications: usize,
    pub time_limited: bool,
}

impl UploadExpiryCursor {
    /// Locally scheduled uploads, not the complete storage-wide cleanup backlog.
    #[must_use]
    pub fn pending_uploads(&self) -> usize {
        self.pending.len()
    }

    #[must_use]
    pub fn has_more(&self) -> bool {
        self.after.is_some() || !self.pending.is_empty()
    }

    pub(super) fn enqueue(&mut self, location: Vec<u8>) {
        if !self.pending.iter().any(|item| item.location == location) {
            self.pending.push_back(ExpiredUpload {
                location,
                parts: None,
                after: None,
                clear: true,
            });
        }
    }

    pub(super) fn full(&self) -> bool {
        self.pending.len() >= MAX_PENDING
    }

    fn requeue(&mut self, work: ExpiredUpload) {
        // Always leave a discovery slot. Only a work item already served in this
        // window may be evicted; accepted part retirements have removed their
        // metadata, so even an evicted large upload makes progress after rescan.
        if self.pending.len() < MAX_PENDING - 1 {
            self.pending.push_back(work);
        }
    }
}

impl DatasetUploads {
    /// Discovers at most twenty keys and advances one round-robin upload by at
    /// most 32 part reads / 64 range notification attempts. Discovery and cleanup
    /// each have an independent two-second deadline.
    /// No scan/progress state is persisted. Deferred ranges remain in their
    /// resource records; a full cache may discard a served scheduling hint.
    /// # Errors
    /// Returns corrupt metadata and storage errors. Resource records retain every
    /// range whose retirement has not been accepted.
    pub async fn expire_step(
        &self,
        now: u64,
        cursor: &mut UploadExpiryCursor,
    ) -> Result<UploadExpiryStep, UploadError> {
        // A bad discovery row must not starve work already queued behind it.
        let discovered = tokio::time::timeout(Duration::from_secs(2), self.discover_expired(now, cursor))
            .await
            .unwrap_or(Err(UploadError::Pending));
        let mut report = UploadExpiryStep {
            scanned: *discovered.as_ref().unwrap_or(&0),
            ..UploadExpiryStep::default()
        };
        if let Some(mut work) = cursor.pending.pop_front() {
            let mut ranges = RANGE_NOTIFICATIONS;
            let result = tokio::time::timeout(
                Duration::from_secs(2),
                self.expire_window(&mut work, &mut report, &mut ranges),
            )
            .await;
            report.range_notifications = RANGE_NOTIFICATIONS - ranges;
            match result {
                Ok(Ok(true)) => (),
                Ok(Ok(false)) => cursor.requeue(work),
                Ok(Err(error)) => {
                    cursor.requeue(work);
                    return Err(error);
                }
                Err(_) => {
                    cursor.requeue(work);
                    report.time_limited = true;
                }
            }
        }
        discovered?;
        Ok(report)
    }
}
