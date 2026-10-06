use std::sync::Arc;

use crate::{
    AuthorityError, DatasetAuthority, DatasetError, DatasetIdentity, DatasetReadRequest, DatasetReadResponse,
    DatasetScanRequest, DatasetScanResponse, DeliveryWindow, ReadCursor, ReadLease, ReadSurface,
    SampleShuffle,
};

/// Shared bounded read adapter used by HTTP and native callers.
///
/// The adapter owns transport admission and lifecycle state; snapshot and
/// projection semantics remain in [`DatasetAuthority`]. Keeping this boundary
/// in the Dataset crate prevents the two public surfaces from drifting.
pub struct DatasetReadService {
    authority: Arc<DatasetAuthority>,
    identity: DatasetIdentity,
    window: Arc<DeliveryWindow>,
    lease: Arc<ReadLease>,
}

#[derive(Debug, thiserror::Error)]
pub enum ReadTransportError {
    #[error(transparent)]
    Invalid(#[from] DatasetError),
    #[error("dataset read admission window is full")]
    WindowFull,
    #[error(transparent)]
    Authority(#[from] AuthorityError),
}

impl DatasetReadService {
    /// Creates a service with a bounded number of batches in flight.
    ///
    /// # Errors
    /// Returns `InvalidManifest` for a zero window.
    pub fn new(
        authority: Arc<DatasetAuthority>,
        identity: DatasetIdentity,
        max_in_flight: usize,
        now_seconds: u64,
    ) -> Result<Self, DatasetError> {
        Self::with_ttl(
            authority,
            identity,
            max_in_flight,
            now_seconds,
            crate::DEFAULT_READ_LEASE_SECONDS,
        )
    }

    /// Creates a service with an explicit inactivity TTL for deployments with
    /// a configured lifecycle policy.
    ///
    /// # Errors
    /// Returns `InvalidManifest` for a zero window or TTL.
    pub fn with_ttl(
        authority: Arc<DatasetAuthority>,
        identity: DatasetIdentity,
        max_in_flight: usize,
        now_seconds: u64,
        ttl_seconds: u64,
    ) -> Result<Self, DatasetError> {
        let window = DeliveryWindow::new(max_in_flight).ok_or(DatasetError::InvalidManifest)?;
        if ttl_seconds == 0 {
            return Err(DatasetError::InvalidManifest);
        }
        Ok(Self {
            authority,
            identity,
            window: Arc::new(window),
            lease: Arc::new(ReadLease::with_ttl(now_seconds, ttl_seconds)),
        })
    }

    /// Reads one validated wire request. The returned response preserves the
    /// request sample order and projection order semantics of the authority.
    ///
    /// # Errors
    /// Returns validation, admission, cancellation, or authority failures.
    pub async fn read(
        &self,
        request: DatasetReadRequest,
        now_seconds: u64,
    ) -> Result<DatasetReadResponse, ReadTransportError> {
        self.read_surface(ReadSurface::Http, request, now_seconds).await
    }

    /// Reads the same request for either public surface.
    ///
    /// # Errors
    /// Returns validation, admission, cancellation, or authority failures.
    pub async fn read_surface(
        &self,
        surface: ReadSurface,
        request: DatasetReadRequest,
        now_seconds: u64,
    ) -> Result<DatasetReadResponse, ReadTransportError> {
        request.validate()?;
        if self.lease.expired(now_seconds) {
            return Err(DatasetError::ReadCancelled.into());
        }
        if !self.window.try_acquire() {
            return Err(ReadTransportError::WindowFull);
        }
        self.authority
            .acquire_snapshot_read(
                &self.identity,
                request.snapshot,
                now_seconds,
                crate::DEFAULT_READ_LEASE_SECONDS,
            )
            .await?;
        let permit = WindowPermit(Arc::clone(&self.window));
        self.lease.touch(now_seconds);
        let fields: Vec<&str> = request.fields.iter().map(String::as_str).collect();
        let samples = self
            .authority
            .read_batch_surface(
                surface,
                &self.identity,
                request.snapshot,
                &request.sample_ids,
                &fields,
            )
            .await;
        self.authority
            .release_snapshot_read(&self.identity, request.snapshot)
            .await?;
        drop(permit);
        self.lease.touch(now_seconds);
        Ok(DatasetReadResponse::from_batch(request.snapshot, samples?))
    }

    /// Plans and delivers exactly one bounded scan window, optionally shuffled.
    ///
    /// # Errors
    /// Returns validation, admission, cursor, planning, or authority failures.
    pub async fn scan(
        &self,
        request: DatasetScanRequest,
        now_seconds: u64,
    ) -> Result<DatasetScanResponse, ReadTransportError> {
        self.scan_surface(ReadSurface::Http, request, now_seconds).await
    }

    /// Executes a bounded scan for a native or HTTP caller.
    ///
    /// # Errors
    /// Returns validation, admission, cursor, planning, or authority failures.
    pub async fn scan_surface(
        &self,
        surface: ReadSurface,
        request: DatasetScanRequest,
        now_seconds: u64,
    ) -> Result<DatasetScanResponse, ReadTransportError> {
        request.plan.validate()?;
        if self.lease.expired(now_seconds) {
            return Err(DatasetError::ReadCancelled.into());
        }
        if !self.window.try_acquire() {
            return Err(ReadTransportError::WindowFull);
        }
        self.authority
            .acquire_snapshot_read(
                &self.identity,
                request.plan.snapshot,
                now_seconds,
                crate::DEFAULT_READ_LEASE_SECONDS,
            )
            .await?;
        let permit = WindowPermit(Arc::clone(&self.window));
        let windows = match request.shuffle.as_ref() {
            Some(spec) => {
                let manifest = self
                    .authority
                    .get_manifest(&self.identity, request.plan.snapshot)
                    .await?;
                SampleShuffle::execute_windows(&request.plan, &manifest, spec)?
            }
            None => {
                self.authority
                    .plan_batches(
                        &self.identity,
                        &request.plan,
                        crate::ReadLimits {
                            max_samples: 1_000_000,
                            max_metadata_bytes: 256 * 1024 * 1024,
                            max_batches: 100_000,
                            prefetch: 1,
                            in_flight: 1,
                        },
                    )
                    .await?
            }
        };
        let stored = self.authority.load_cursor(&self.identity, &request.plan).await?;
        let cursor = request
            .cursor
            .or(stored)
            .unwrap_or_else(|| ReadCursor::start(&request.plan));
        cursor.validate_for(&request.plan)?;
        let group = usize::try_from(cursor.group).map_err(|_| DatasetError::CursorMismatch)?;
        if group > windows.len() || (group == windows.len() && cursor.offset != 0) {
            return Err(DatasetError::CursorMismatch.into());
        }
        let ids = if group == windows.len() {
            Vec::new()
        } else {
            let offset = usize::try_from(cursor.offset).map_err(|_| DatasetError::CursorMismatch)?;
            if offset > windows[group].len() {
                return Err(DatasetError::CursorMismatch.into());
            }
            windows[group][offset..].to_vec()
        };
        let fields: Vec<&str> = request.plan.projection.iter().map(String::as_str).collect();
        let samples = if ids.is_empty() {
            Vec::new()
        } else {
            self.authority
                .read_batch_surface(surface, &self.identity, request.plan.snapshot, &ids, &fields)
                .await?
        };
        self.authority
            .release_snapshot_read(&self.identity, request.plan.snapshot)
            .await?;
        let next_group = group
            .checked_add(usize::from(!ids.is_empty()))
            .and_then(|value| u32::try_from(value).ok())
            .ok_or(DatasetError::CursorMismatch)?;
        let next = cursor.confirm_batch(&request.plan, next_group, 0)?;
        let expected = self.authority.load_cursor(&self.identity, &request.plan).await?;
        self.authority
            .persist_cursor(&self.identity, &request.plan, &next, expected.as_ref())
            .await?;
        drop(permit);
        self.lease.touch(now_seconds);
        Ok(DatasetScanResponse {
            snapshot: request.plan.snapshot,
            samples,
            cursor: next,
            end: group >= windows.len().saturating_sub(1),
        })
    }

    /// Cancels queued and future reads and releases the active lease.
    pub fn cancel(&self) {
        self.window.cancel();
        self.lease.release();
    }

    #[must_use]
    pub fn in_flight(&self) -> usize {
        self.window.in_flight()
    }

    #[must_use]
    pub fn is_cancelled(&self) -> bool {
        self.window.is_cancelled()
    }

    #[must_use]
    pub fn lease_expired(&self, now_seconds: u64) -> bool {
        self.lease.expired(now_seconds)
    }

    #[must_use]
    pub fn authority(&self) -> Arc<DatasetAuthority> {
        Arc::clone(&self.authority)
    }

    #[must_use]
    pub fn identity(&self) -> &DatasetIdentity {
        &self.identity
    }
}

struct WindowPermit(Arc<DeliveryWindow>);

impl Drop for WindowPermit {
    fn drop(&mut self) {
        self.0.release();
    }
}
