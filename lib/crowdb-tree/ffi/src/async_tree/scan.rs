// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{AsyncCrowdbtree, ScanOutcome};
use crate::error::CtError;
use crate::reactor::{drive_ct_future, try_poll_ct_future, FutureKind, RawOutcome};
use crate::scan::{decode_scan_bytes, owner, ScanEntry, ScanOptions};

fn decode(out: RawOutcome) -> Result<(Vec<ScanEntry>, bool), CtError> {
    let packed = out.packed.ok_or(CtError::Internal)?;
    let count = usize::try_from(out.slot).map_err(|_| CtError::Corruption)?;
    Ok((decode_scan_bytes(packed, count)?, out.found))
}

impl AsyncCrowdbtree {
    /// Scans with explicit cursor, result limit and byte-budget semantics.
    /// Internal work windows do not reduce the requested result limit. The
    /// native allocation stays charged until the last returned `Bytes` owner drops.
    pub async fn scan_with_options(&self, options: ScanOptions) -> Result<(Vec<ScanEntry>, bool), CtError> {
        let guard = owner::start(self.inner.as_ptr(), &options)?;
        decode(drive_ct_future(guard, &self.inner, FutureKind::ScanOwned).await?)
    }

    /// Starts the same explicit-bound scan and polls its completion once.
    pub fn try_scan_with_options(&self, options: ScanOptions) -> ScanOutcome {
        let mut guard = match owner::start(self.inner.as_ptr(), &options) {
            Ok(guard) => guard,
            Err(error) => return ScanOutcome::Ready(Err(error)),
        };
        if let Some(result) = try_poll_ct_future(&mut guard, FutureKind::ScanOwned) {
            return ScanOutcome::Ready(result.and_then(decode));
        }
        let tree = self.inner.clone();
        ScanOutcome::Pending(Box::pin(async move {
            decode(drive_ct_future(guard, &tree, FutureKind::ScanOwned).await?)
        }))
    }
}
