// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Segment ordering barriers after caller-proven exclusion of old producers.

use super::{Bytes, DiskioClient, DiskioResult, OperationOptions, SegmentTarget, WriteMode, WritePayload};

impl DiskioClient {
    /// Wait for prior segment writes, then synchronize them through one reply.
    /// The caller must independently exclude future writes before treating this
    /// as authority to observe a final prefix or reuse physical storage.
    ///
    /// # Errors
    /// Returns input, topology, timeout, queue, disk, synchronization or ambiguous
    /// outcome errors. A failed or unproven response never establishes a drain.
    pub async fn drain_segment(&self, target: SegmentTarget, options: OperationOptions) -> DiskioResult<()> {
        self.write_payload(
            target,
            0,
            WritePayload::Contiguous(Bytes::new()),
            WriteMode::Drain,
            options,
        )
        .await
    }
}
