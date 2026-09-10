// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! One declared small object retained under a whole-object reservation.

use std::sync::Arc;
use std::time::Instant;

use bytes::Bytes;
use crowdb_protocol::chunkdb::rpc::Location as ProtoLocation;
use tokio::sync::oneshot;

use crate::io::{ChunkIoWriter, FeedStatus};
use crate::{IoError, Result};

use super::small_pool::{ByteReservation, PendingObject, SmallPoolRuntime};

/// A single-use object handle backed by the client's shared small-write pool.
pub struct SmallObjectWriter {
    runtime: Option<Arc<SmallPoolRuntime>>,
    reservation: Option<ByteReservation>,
    declared_size: usize,
    retained_size: usize,
    fragments: Vec<Bytes>,
    finished: bool,
}

impl SmallObjectWriter {
    pub(crate) fn new(
        runtime: Arc<SmallPoolRuntime>,
        declared_size: usize,
        reservation: ByteReservation,
    ) -> Self {
        Self {
            runtime: Some(runtime),
            reservation: Some(reservation),
            declared_size,
            retained_size: 0,
            fragments: Vec::new(),
            finished: false,
        }
    }

    pub(crate) fn empty() -> Self {
        Self {
            runtime: None,
            reservation: None,
            declared_size: 0,
            retained_size: 0,
            fragments: Vec::new(),
            finished: false,
        }
    }

    fn ensure_open(&self) -> Result<()> {
        if self.finished {
            Err(IoError::Finished)
        } else {
            Ok(())
        }
    }

    fn fail_size(&mut self, actual: usize) -> IoError {
        self.finished = true;
        self.fragments.clear();
        self.reservation.take();
        IoError::ObjectSizeMismatch {
            declared: self.declared_size,
            actual,
        }
    }
}

#[async_trait::async_trait]
impl ChunkIoWriter for SmallObjectWriter {
    async fn on_data(&mut self, buffer: Bytes) -> Result<FeedStatus> {
        self.ensure_open()?;
        let actual = self.retained_size.saturating_add(buffer.len());
        if actual > self.declared_size {
            return Err(self.fail_size(actual));
        }
        self.retained_size = actual;
        self.fragments.push(buffer);
        Ok(if self.retained_size == self.declared_size {
            FeedStatus::Pause
        } else {
            FeedStatus::Continue
        })
    }

    async fn on_finish(&mut self) -> Result<Vec<ProtoLocation>> {
        self.ensure_open()?;
        if self.retained_size != self.declared_size {
            return Err(self.fail_size(self.retained_size));
        }
        self.finished = true;
        if self.declared_size == 0 {
            return Ok(Vec::new());
        }
        let runtime = self
            .runtime
            .take()
            .ok_or_else(|| IoError::Internal("small writer missing shared pool".into()))?;
        let reservation = self
            .reservation
            .take()
            .ok_or_else(|| IoError::Internal("small writer missing reservation".into()))?;
        let (completion, result) = oneshot::channel();
        let object = PendingObject {
            fragments: std::mem::take(&mut self.fragments),
            len: self.declared_size,
            enqueued_at: Instant::now(),
            completion,
            _reservation: reservation,
        };
        runtime.submit(object).await?;
        result
            .await
            .map_err(|_| IoError::WriteFailed("small-write completion was lost".into()))?
    }

    async fn on_error(&mut self) -> Result<Vec<ProtoLocation>> {
        self.ensure_open()?;
        self.finished = true;
        self.fragments.clear();
        self.reservation.take();
        Ok(Vec::new())
    }

    fn require_data(&self) -> bool {
        !self.finished && self.retained_size < self.declared_size
    }
}
