// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Metrics forwarding preserves the underlying writer's completion contract.

use super::{async_trait, Arc, Bytes, ChunkClientMetrics, DiskWriter, Result, Segment};

pub(super) struct MetricsDiskWriter {
    pub(super) inner: Arc<dyn DiskWriter>,
    pub(super) metrics: Arc<ChunkClientMetrics>,
}

#[async_trait]
impl DiskWriter for MetricsDiskWriter {
    async fn write(&self, seg: &Segment, unit_bytes: u64, data: Bytes) -> Result<()> {
        let bytes = u64::try_from(data.len()).unwrap_or(u64::MAX);
        let mut operation = self.metrics.diskio_write.start();
        let result = self.inner.write(seg, unit_bytes, data).await;
        if result.is_ok() {
            self.metrics.diskio_write_bytes.observe(bytes);
            operation.mark_success();
        }
        result
    }

    async fn write_views(&self, seg: &Segment, unit_bytes: u64, data: Vec<Bytes>) -> Result<()> {
        let bytes = data.iter().fold(0_u64, |total, view| {
            total.saturating_add(u64::try_from(view.len()).unwrap_or(u64::MAX))
        });
        let mut operation = self.metrics.diskio_write.start();
        let result = self.inner.write_views(seg, unit_bytes, data).await;
        if result.is_ok() {
            self.metrics.diskio_write_bytes.observe(bytes);
            operation.mark_success();
        }
        result
    }

    async fn fsync(&self, seg: &Segment) -> Result<()> {
        self.inner.fsync(seg).await
    }

    async fn drain_segment(
        &self,
        seg: &Segment,
        unit_bytes: u64,
        deadline: std::time::Instant,
    ) -> Result<()> {
        self.inner.drain_segment(seg, unit_bytes, deadline).await
    }

    async fn write_views_at_byte_offset(
        &self,
        seg: &Segment,
        unit_bytes: u64,
        byte_offset: u64,
        data: Vec<Bytes>,
    ) -> Result<()> {
        let bytes = data.iter().map(|view| view.len() as u64).sum();
        let mut operation = self.metrics.diskio_write.start();
        let result = self
            .inner
            .write_views_at_byte_offset(seg, unit_bytes, byte_offset, data)
            .await;
        if result.is_ok() {
            self.metrics.diskio_write_bytes.observe(bytes);
            operation.mark_success();
        }
        result
    }

    async fn write_synchronized_at_byte_offset(
        &self,
        seg: &Segment,
        unit_bytes: u64,
        byte_offset: u64,
        data: Bytes,
    ) -> Result<()> {
        let bytes = u64::try_from(data.len()).unwrap_or(u64::MAX);
        let mut operation = self.metrics.diskio_write.start();
        let result = self
            .inner
            .write_synchronized_at_byte_offset(seg, unit_bytes, byte_offset, data)
            .await;
        if result.is_ok() {
            self.metrics.diskio_write_bytes.observe(bytes);
            operation.mark_success();
        }
        result
    }

    async fn write_synchronized_views_at_byte_offset(
        &self,
        seg: &Segment,
        unit_bytes: u64,
        byte_offset: u64,
        data: Vec<Bytes>,
    ) -> Result<()> {
        let bytes = data.iter().map(|view| view.len() as u64).sum();
        let mut operation = self.metrics.diskio_write.start();
        let result = self
            .inner
            .write_synchronized_views_at_byte_offset(seg, unit_bytes, byte_offset, data)
            .await;
        if result.is_ok() {
            self.metrics.diskio_write_bytes.observe(bytes);
            operation.mark_success();
        }
        result
    }

    async fn initialize_synchronized_at_byte_offset(
        &self,
        seg: &Segment,
        unit_bytes: u64,
        byte_offset: u64,
        data: Bytes,
    ) -> Result<()> {
        let bytes = data.len() as u64;
        let mut operation = self.metrics.diskio_write.start();
        let result = self
            .inner
            .initialize_synchronized_at_byte_offset(seg, unit_bytes, byte_offset, data)
            .await;
        if result.is_ok() {
            self.metrics.diskio_write_bytes.observe(bytes);
            operation.mark_success();
        }
        result
    }

    async fn write_priority_at_byte_offset(
        &self,
        seg: &Segment,
        unit_bytes: u64,
        byte_offset: u64,
        data: Bytes,
    ) -> Result<()> {
        let bytes = u64::try_from(data.len()).unwrap_or(u64::MAX);
        let mut operation = self.metrics.diskio_write.start();
        let result = self
            .inner
            .write_priority_at_byte_offset(seg, unit_bytes, byte_offset, data)
            .await;
        if result.is_ok() {
            self.metrics.diskio_write_bytes.observe(bytes);
            operation.mark_success();
        }
        result
    }

    async fn fsync_priority(&self, seg: &Segment) -> Result<()> {
        self.inner.fsync_priority(seg).await
    }

    async fn read(&self, seg: &Segment, unit_bytes: u64, segment_offset: u64, length: u32) -> Result<Bytes> {
        let mut operation = self.metrics.diskio_read.start();
        let result = self.inner.read(seg, unit_bytes, segment_offset, length).await;
        if let Ok(data) = &result {
            self.metrics
                .diskio_read_bytes
                .observe(u64::try_from(data.len()).unwrap_or(u64::MAX));
            operation.mark_success();
        }
        result
    }

    async fn write_at_byte_offset(
        &self,
        seg: &Segment,
        unit_bytes: u64,
        byte_offset: u64,
        data: Bytes,
    ) -> Result<()> {
        let bytes = u64::try_from(data.len()).unwrap_or(u64::MAX);
        let mut operation = self.metrics.diskio_write.start();
        let result = self
            .inner
            .write_at_byte_offset(seg, unit_bytes, byte_offset, data)
            .await;
        if result.is_ok() {
            self.metrics.diskio_write_bytes.observe(bytes);
            operation.mark_success();
        }
        result
    }
}
