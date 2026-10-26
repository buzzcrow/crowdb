// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{DiskioClient, DiskioError, DiskioResult, Ordering};

impl DiskioClient {
    /// Closes owned request admission, joins RPC workers and stops timeout reaping.
    /// Caller-owned payloads retain their native recyclers after worker shutdown.
    pub fn shutdown(&self) {
        if self.closed.swap(true, Ordering::AcqRel) {
            return;
        }
        self.closing.notify_waiters();
        self.server.stop();
        self.normal.clear();
        self.priority.clear();
        self.wire.stop_reaper();
    }

    pub(super) fn check_open(&self) -> DiskioResult<()> {
        if self.closed.load(Ordering::Acquire) {
            Err(DiskioError::TransportUnavailable(
                "DiskIO client is closed".into(),
            ))
        } else {
            Ok(())
        }
    }
}
