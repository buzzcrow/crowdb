// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
use std::future::Future;
use std::pin::Pin;

use super::AsyncCrowdbtree;
use crate::error::CtError;
use crate::reactor::{drive_completion, try_poll_ct_future_pinned, FutureGuard, PinnedValue};
use crate::sys;
use bytes::Bytes;

type OwnedValue = Option<(u64, Bytes)>;

/// Both immediate and pending reads retain the immutable native allocation.
pub enum OwnedGetOutcome {
    Ready(Result<OwnedValue, CtError>),
    Pending(Pin<Box<dyn Future<Output = Result<OwnedValue, CtError>> + Send>>),
}

fn convert(value: Option<(u64, PinnedValue)>) -> Result<OwnedValue, CtError> {
    value
        .map(|(slot, owner)| owner.try_into_bytes().map(|bytes| (slot, bytes)))
        .transpose()
}

impl AsyncCrowdbtree {
    /// Read without copying a pending native result into unaccounted storage.
    /// The native future and the eventual Bytes control retain one paid owner.
    pub fn try_get_owned(&self, key: &[u8]) -> OwnedGetOutcome {
        let future = unsafe { sys::ct_get_async(self.inner.as_ptr(), key.as_ptr(), key.len()) };
        let mut guard = FutureGuard(future);
        if let Some(result) = try_poll_ct_future_pinned(&mut guard) {
            return OwnedGetOutcome::Ready(result.and_then(convert));
        }
        let tree = self.inner.clone();
        OwnedGetOutcome::Pending(Box::pin(async move {
            let value = drive_completion(guard, &tree, try_poll_ct_future_pinned).await?;
            convert(value)
        }))
    }
}
