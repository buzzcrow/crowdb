// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::error::ApiError;
use hyper::body::Bytes;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

/// Each admitted JSON request reserves one bounded request and response window.
/// Response credit follows the byte owner through HTTP delivery and disconnect.
pub(crate) struct ApiAdmission {
    active: AtomicUsize,
    limit: usize,
}
impl ApiAdmission {
    pub(crate) fn new(limit: usize) -> Self {
        Self {
            active: AtomicUsize::new(0),
            limit,
        }
    }
    pub(super) fn acquire(self: &Arc<Self>) -> Result<ApiPermit, ApiError> {
        self.active
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |active| {
                active.checked_add(1).filter(|next| *next <= self.limit)
            })
            .map_err(|_| ApiError::capacity())?;
        Ok(ApiPermit(Arc::clone(self)))
    }
}
pub(super) struct ApiPermit(Arc<ApiAdmission>);
impl Drop for ApiPermit {
    fn drop(&mut self) {
        self.0.active.fetch_sub(1, Ordering::AcqRel);
    }
}
pub(super) struct ResponseOwner {
    pub bytes: Bytes,
    pub permit: ApiPermit,
}
impl AsRef<[u8]> for ResponseOwner {
    fn as_ref(&self) -> &[u8] {
        let _ = &self.permit;
        &self.bytes
    }
}
