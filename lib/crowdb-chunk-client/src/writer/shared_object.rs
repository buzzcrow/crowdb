// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! One object retained for the shared-chunk aggregation path.

use std::sync::Arc;
use std::time::Instant;

use bytes::Bytes;
use crowdb_protocol::chunkdb::rpc::Location as ProtoLocation;
use tokio::sync::oneshot;

use crate::io::{ChunkIoWriter, FeedStatus};
use crate::{IoError, Result};

use super::small_pool::{PendingObject, PipelineRoute, RouteCharge, SmallPoolRuntime};

#[async_trait::async_trait]
pub trait SmallWriteIntent: Send + Sync {
    async fn before_write(&self, location: &ProtoLocation) -> Result<()>;
}

/// A single-use object handle backed by the client's shared small-write pool.
pub struct SharedObjectWriter {
    runtime: Option<Arc<SmallPoolRuntime>>,
    charge: Option<RouteCharge>,
    declared_size: usize,
    route_hash: u64,
    route: Option<Arc<PipelineRoute>>,
    retained_size: usize,
    fragments: Vec<Bytes>,
    finished: bool,
    durable_completion: bool,
    intent: Option<Arc<dyn SmallWriteIntent>>,
}

impl SharedObjectWriter {
    pub(crate) fn new(
        runtime: Arc<SmallPoolRuntime>,
        declared_size: usize,
        route: Arc<PipelineRoute>,
        route_hash: u64,
    ) -> Self {
        let charge = RouteCharge::new(Arc::clone(&route), Arc::clone(&runtime.metrics));
        Self {
            runtime: Some(runtime),
            charge: Some(charge),
            declared_size,
            route_hash,
            route: Some(route),
            retained_size: 0,
            fragments: Vec::new(),
            finished: false,
            durable_completion: false,
            intent: None,
        }
    }

    pub(crate) fn empty() -> Self {
        Self {
            runtime: None,
            charge: None,
            declared_size: 0,
            route_hash: 0,
            route: None,
            retained_size: 0,
            fragments: Vec::new(),
            finished: false,
            durable_completion: false,
            intent: None,
        }
    }

    fn ensure_open(&self) -> Result<()> {
        if self.finished {
            Err(IoError::Finished)
        } else {
            Ok(())
        }
    }

    /// Completes only after the readable chunk cursor covers this object's bytes.
    /// # Errors
    /// Returns admission, physical write, metadata confirmation or size failures.
    pub async fn finish_durable(&mut self) -> Result<Vec<ProtoLocation>> {
        self.durable_completion = true;
        self.on_finish().await
    }

    /// Persists exact object ownership before any physical write for the batch.
    /// # Errors
    /// A failed intent aborts the batch without issuing its disk writes.
    pub async fn finish_durable_with_intent(
        &mut self,
        intent: Arc<dyn SmallWriteIntent>,
    ) -> Result<Vec<ProtoLocation>> {
        self.ensure_open()?;
        self.intent = Some(intent);
        self.finish_durable().await
    }

    fn fail_size(&mut self, actual: usize) -> IoError {
        self.finished = true;
        self.fragments.clear();
        self.charge.take();
        IoError::ObjectSizeMismatch {
            declared: self.declared_size,
            actual,
        }
    }
}

#[async_trait::async_trait]
impl ChunkIoWriter for SharedObjectWriter {
    async fn on_data(&mut self, buffer: Bytes) -> Result<FeedStatus> {
        self.ensure_open()?;
        let actual = self.retained_size.saturating_add(buffer.len());
        if actual > self.declared_size {
            return Err(self.fail_size(actual));
        }
        self.retained_size = actual;
        self.charge
            .as_mut()
            .ok_or_else(|| IoError::Internal("shared writer missing route charge".into()))?
            .add(buffer.len());
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
        let charge = self
            .charge
            .take()
            .ok_or_else(|| IoError::Internal("small writer missing route charge".into()))?;
        let (completion, result) = oneshot::channel();
        let object = PendingObject {
            intent: self.intent.take(),
            durable_completion: self.durable_completion,
            route_hash: self.route_hash,
            route: self
                .route
                .take()
                .ok_or_else(|| IoError::Internal("shared writer missing route".into()))?,
            fragments: std::mem::take(&mut self.fragments),
            len: self.declared_size,
            enqueued_at: Instant::now(),
            completion,
            charge,
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
        self.charge.take();
        Ok(Vec::new())
    }

    fn require_data(&self) -> bool {
        !self.finished
            && self.retained_size < self.declared_size
            && self.route.as_ref().map_or(true, |route| route.has_capacity())
    }

    fn input_complete(&self) -> bool {
        !self.finished && self.retained_size == self.declared_size
    }

    async fn wait_for_capacity(&mut self) {
        let Some(route) = &self.route else {
            return;
        };
        let notified = route.capacity_changed.notified();
        if route.has_capacity() {
            return;
        }
        tokio::select! {
            () = notified => {},
            () = tokio::time::sleep(std::time::Duration::from_millis(5)) => {},
        }
    }
}
