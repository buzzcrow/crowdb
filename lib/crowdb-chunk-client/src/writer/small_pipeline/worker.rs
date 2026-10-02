// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{
    fail_one, frame_bytes, Arc, Duration, IoError, Ordering, OwnedChunk, PendingObject, PipelineWorker,
    Result,
};

impl PipelineWorker {
    pub(super) async fn run(mut self) -> Result<()> {
        let mut liveness = tokio::time::interval(self.runtime.policy.writer_lease / 2);
        liveness.tick().await;
        loop {
            if self.retire.load(Ordering::Acquire) {
                self.receiver.close();
            }
            let (first, dequeued) = if let Some(object) = self.carry.take() {
                (Some(object), false)
            } else if self.retire.load(Ordering::Acquire) {
                (self.receiver.recv().await, true)
            } else {
                let object = tokio::select! {
                    object = self.receiver.recv() => object,
                    () = self.wake.notified() => {
                        self.receiver.close();
                        self.receiver.recv().await
                    },
                    _ = liveness.tick() => {
                        if let Err(error) = self.renew_idle_chunks().await {
                            self.receiver.close();
                            self.fail_remaining(&error.to_string()).await;
                            let _ = self.finish_chunks().await;
                            return Err(error);
                        }
                        continue;
                    },
                };
                (object, true)
            };
            let Some(first) = first else {
                break;
            };
            if dequeued {
                self.note_dequeue(&first);
            }
            let fit = self.ensure_object_fits(frame_bytes(first.len)?).await;
            if let Err(error) = fit {
                self.receiver.close();
                fail_one(first, &error.to_string(), &self.runtime.metrics);
                self.fail_remaining(&error.to_string()).await;
                let _ = self.finish_chunks().await;
                return Err(error);
            }
            let batch = self.collect_batch(first);
            self.route.busy.store(true, Ordering::Release);
            let result = self.write_batch_with_watchdog(batch).await;
            self.route.busy.store(false, Ordering::Release);
            self.route
                .last_active_ms
                .store(self.runtime.now_ms(), Ordering::Relaxed);
            if let Err(error) = result {
                if matches!(error, IoError::SourceRead(_)) {
                    let replacement = self.take_replacement().await?;
                    self.chunk.finish().await?;
                    self.chunk = replacement;
                    continue;
                }
                self.receiver.close();
                self.fail_remaining(&error.to_string()).await;
                let _ = self.finish_chunks().await;
                return Err(error);
            }
            self.prepare_replacement();
        }
        self.finish_chunks().await
    }

    async fn write_batch_with_watchdog(&mut self, batch: Vec<PendingObject>) -> Result<()> {
        let object_count = batch.len();
        let logical_bytes: usize = batch.iter().map(|object| object.len).sum();
        let watchdog = self.runtime.policy.batch_watchdog;
        let metrics = Arc::clone(&self.runtime.metrics);
        let write = self.chunk.write_batch(batch, &metrics, &self.runtime);
        tokio::pin!(write);
        let mut elapsed = Duration::ZERO;
        loop {
            tokio::select! {
                result = &mut write => return result,
                () = tokio::time::sleep(watchdog) => {
                    elapsed = elapsed.saturating_add(watchdog);
                    metrics
                        .batch_watchdog_expirations
                        .fetch_add(1, Ordering::Relaxed);
                    tracing::warn!(
                        object_count,
                        logical_bytes,
                        watchdog_ms = watchdog.as_millis(),
                        elapsed_ms = elapsed.as_millis(),
                        "small-write batch remains in flight after watchdog interval"
                    );
                }
            }
        }
    }

    fn note_dequeue(&self, object: &PendingObject) {
        self.route.dequeued(object.len, self.runtime.now_ms());
        let delay = u64::try_from(object.enqueued_at.elapsed().as_nanos()).unwrap_or(u64::MAX);
        self.runtime
            .metrics
            .queue_delay_ns
            .fetch_add(delay, Ordering::Relaxed);
        self.runtime
            .metrics
            .max_queue_delay_ns
            .fetch_max(delay, Ordering::Relaxed);
    }

    async fn ensure_object_fits(&mut self, object_len: usize) -> Result<()> {
        if self.chunk.remaining_in_chunk() < object_len as u64 {
            let replacement = self.take_replacement().await?;
            self.chunk.finish().await?;
            self.chunk = replacement;
        }
        self.chunk.ensure_strip().await?;
        Ok(())
    }

    fn prepare_replacement(&mut self) {
        let runway = u64::from(self.runtime.policy.small_strip_prefetch_count).saturating_mul(
            self.chunk
                .current_strip()
                .map_or(1024 * 1024, |strip| u64::from(strip.capacity) * 1024),
        );
        if self.replacement.is_none()
            && self.ready_replacement.is_none()
            && self.chunk.remaining_in_chunk() <= runway.max(self.runtime.policy.object_limit as u64)
        {
            let runtime = Arc::clone(&self.runtime);
            let conversion = Arc::clone(&self.route.conversion_active);
            self.replacement = Some(tokio::spawn(async move {
                OwnedChunk::allocate(&runtime, conversion).await
            }));
        }
    }

    async fn take_replacement(&mut self) -> Result<OwnedChunk> {
        if let Some(chunk) = self.ready_replacement.take() {
            return Ok(chunk);
        }
        if let Some(pending) = self.replacement.take() {
            pending
                .await
                .map_err(|error| IoError::AllocationFailed(error.to_string()))?
        } else {
            OwnedChunk::allocate(&self.runtime, Arc::clone(&self.route.conversion_active)).await
        }
    }

    async fn finish_chunks(&mut self) -> Result<()> {
        let current_result = self.chunk.finish().await;
        let replacement_result = if self.replacement.is_some() || self.ready_replacement.is_some() {
            match self.take_replacement().await {
                Ok(mut chunk) => chunk.finish().await,
                Err(error) => Err(error),
            }
        } else {
            Ok(())
        };
        current_result.and(replacement_result)
    }

    async fn renew_idle_chunks(&mut self) -> Result<()> {
        self.chunk.renew_liveness().await?;
        if self
            .replacement
            .as_ref()
            .is_some_and(tokio::task::JoinHandle::is_finished)
        {
            self.ready_replacement = Some(self.take_replacement().await?);
        }
        if let Some(chunk) = &mut self.ready_replacement {
            chunk.renew_liveness().await?;
        }
        Ok(())
    }

    fn collect_batch(&mut self, first: PendingObject) -> Vec<PendingObject> {
        let mut bytes = frame_bytes(first.len).unwrap_or(usize::MAX);
        let mut batch = vec![first];
        while batch.len() < self.runtime.policy.max_batch_objects
            && bytes < self.runtime.policy.max_batch_bytes
        {
            let Ok(next) = self.receiver.try_recv() else {
                break;
            };
            self.note_dequeue(&next);
            let candidate_bytes = bytes.saturating_add(frame_bytes(next.len).unwrap_or(usize::MAX));
            let available = self.chunk.remaining_in_chunk();
            if candidate_bytes > self.runtime.policy.max_batch_bytes || candidate_bytes as u64 > available {
                self.carry = Some(next);
                break;
            }
            bytes = candidate_bytes;
            batch.push(next);
        }
        batch
    }

    async fn fail_remaining(&mut self, message: &str) {
        if let Some(object) = self.carry.take() {
            fail_one(object, message, &self.runtime.metrics);
        }
        while let Some(object) = self.receiver.recv().await {
            self.note_dequeue(&object);
            fail_one(object, message, &self.runtime.metrics);
        }
    }
}
