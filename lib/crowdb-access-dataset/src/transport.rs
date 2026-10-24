use std::sync::Arc;

use crate::{
    AuthorityError, DatasetAuthority, DatasetError, DatasetIdentity, DatasetReadRequest, DatasetReadResponse,
    DatasetScanRequest, DatasetScanResponse, DeliveryWindow, ReadCursor, ReadSurface, SampleShuffle,
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
    /// Creates a service with a bounded number of independent requests in flight.
    /// # Errors
    /// Rejects a zero request window.
    pub fn new(
        authority: Arc<DatasetAuthority>,
        identity: DatasetIdentity,
        max_in_flight: usize,
    ) -> Result<Self, DatasetError> {
        let window = DeliveryWindow::new(max_in_flight).ok_or(DatasetError::InvalidManifest)?;
        Ok(Self {
            authority,
            identity,
            window: Arc::new(window),
        })
    }

    /// Reads one validated wire request. The returned response preserves the
    /// request sample order and projection order semantics of the authority.
    ///
    /// # Errors
    /// Returns validation, admission, cancellation, or authority failures.
    pub async fn read(&self, request: DatasetReadRequest) -> Result<DatasetReadResponse, ReadTransportError> {
        self.read_surface(ReadSurface::Http, request).await
    }

    /// Reads the same request for either public surface.
    ///
    /// # Errors
    /// Returns validation, admission, cancellation, or authority failures.
    pub async fn read_surface(
        &self,
        surface: ReadSurface,
        request: DatasetReadRequest,
    ) -> Result<DatasetReadResponse, ReadTransportError> {
        request.validate()?;
        if !self.window.try_acquire() {
            return Err(ReadTransportError::WindowFull);
        }
        let permit = WindowPermit(Arc::clone(&self.window));
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
        drop(permit);
        Ok(DatasetReadResponse::from_batch(request.snapshot, samples?))
    }

    /// Plans and delivers exactly one bounded scan window, optionally shuffled.
    ///
    /// # Errors
    /// Returns validation, admission, cursor, planning, or authority failures.
    pub async fn scan(&self, request: DatasetScanRequest) -> Result<DatasetScanResponse, ReadTransportError> {
        self.scan_surface(ReadSurface::Http, request).await
    }

    /// Executes a bounded scan for a native or HTTP caller.
    ///
    /// # Errors
    /// Returns validation, admission, cursor, planning, or authority failures.
    pub async fn scan_surface(
        &self,
        surface: ReadSurface,
        request: DatasetScanRequest,
    ) -> Result<DatasetScanResponse, ReadTransportError> {
        request.plan.validate()?;
        if !self.window.try_acquire() {
            return Err(ReadTransportError::WindowFull);
        }
        let permit = WindowPermit(Arc::clone(&self.window));
        let cursor = request.cursor.unwrap_or_else(|| ReadCursor::start(&request.plan));
        cursor.validate_for(&request.plan)?;
        let planned = match request.shuffle.as_ref() {
            Some(spec) => {
                let manifest = self
                    .authority
                    .get_manifest(&self.identity, request.plan.snapshot)
                    .await?;
                SampleShuffle::execute(&request.plan, &manifest, spec)?
            }
            None => {
                self.authority
                    .plan_ids(
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
        let windows = planned.len().div_ceil(request.plan.batch_size);
        let group = usize::try_from(cursor.group).map_err(|_| DatasetError::CursorMismatch)?;
        let range = crate::planner::batch_range(
            planned.len(),
            request.plan.batch_size,
            cursor.group,
            cursor.offset,
        )?;
        let ids: Vec<_> = planned.into_iter().skip(range.start).take(range.len()).collect();
        let fields: Vec<&str> = request.plan.projection.iter().map(String::as_str).collect();
        let samples = if ids.is_empty() {
            self.authority
                .get_snapshot(&self.identity, request.plan.snapshot)
                .await?;
            Vec::new()
        } else {
            self.authority
                .read_batch_surface(surface, &self.identity, request.plan.snapshot, &ids, &fields)
                .await?
        };
        let next_group = group
            .checked_add(usize::from(!ids.is_empty()))
            .and_then(|value| u32::try_from(value).ok())
            .ok_or(DatasetError::CursorMismatch)?;
        let next = cursor.confirm_batch(&request.plan, next_group, 0)?;
        drop(permit);
        Ok(DatasetScanResponse {
            snapshot: request.plan.snapshot,
            samples,
            cursor: next,
            end: group >= windows.saturating_sub(1),
        })
    }

    /// Cancels queued and future requests on this local service.
    pub fn cancel(&self) {
        self.window.cancel();
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
