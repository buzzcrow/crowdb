mod repair;

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use crowdb_protocol::chunkdb::rpc::{Chunk, Strip};
use crowdb_protocol::common::ChunkId;
use crowdb_protocol::diskdb::rpc::Segment;
use tokio::task::{JoinHandle, JoinSet};

use crate::metrics::SmallWriteMetrics;
use crate::negative_list::FailedDiskList;
use crate::{ChunkAllocator, DiskWriter, IoError, Result};

pub struct MirrorStripFlow {
    allocator: Arc<dyn ChunkAllocator>,
    disk_writer: Arc<dyn DiskWriter>,
    failed_disks: Arc<FailedDiskList>,
    attempts: usize,
    writer_epoch: u64,
    sync: bool,
    resolve_ambiguity: bool,
    metrics: Option<Arc<SmallWriteMetrics>>,
}

impl MirrorStripFlow {
    pub fn new(
        allocator: Arc<dyn ChunkAllocator>,
        disk_writer: Arc<dyn DiskWriter>,
        writer_epoch: u64,
        attempts: usize,
        sync: bool,
    ) -> Result<Self> {
        Self::with_shared_failures(
            allocator,
            disk_writer,
            Arc::new(FailedDiskList::new(Duration::from_secs(60))),
            writer_epoch,
            attempts,
            sync,
        )
    }

    pub fn with_shared_failures(
        allocator: Arc<dyn ChunkAllocator>,
        disk_writer: Arc<dyn DiskWriter>,
        failed_disks: Arc<FailedDiskList>,
        writer_epoch: u64,
        attempts: usize,
        sync: bool,
    ) -> Result<Self> {
        if writer_epoch == 0 || attempts == 0 {
            return Err(IoError::Internal("invalid mirror-strip writer policy".into()));
        }
        Ok(Self {
            allocator,
            disk_writer,
            failed_disks,
            attempts,
            writer_epoch,
            sync,
            resolve_ambiguity: false,
            metrics: None,
        })
    }

    #[must_use]
    pub fn resolve_ambiguity(mut self) -> Self {
        self.resolve_ambiguity = true;
        self
    }

    pub(crate) fn with_small_write_metrics(
        mut self,
        failed_disks: Arc<FailedDiskList>,
        metrics: Arc<SmallWriteMetrics>,
    ) -> Self {
        self.failed_disks = failed_disks;
        self.metrics = Some(metrics);
        self
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn write(
        &self,
        chunk: &mut Chunk,
        committed_cursor: u64,
        strip_sequence: u32,
        block_offset: u64,
        data: Bytes,
        full_image: Bytes,
        pending_advance: &mut Option<JoinHandle<Result<Chunk>>>,
    ) -> Result<()> {
        self.write_views(
            chunk,
            committed_cursor,
            strip_sequence,
            block_offset,
            vec![data],
            &[full_image],
            pending_advance,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn write_views(
        &self,
        chunk: &mut Chunk,
        committed_cursor: u64,
        strip_sequence: u32,
        block_offset: u64,
        data: Vec<Bytes>,
        full_image: &[Bytes],
        pending_advance: &mut Option<JoinHandle<Result<Chunk>>>,
    ) -> Result<()> {
        let data_len: usize = data.iter().map(Bytes::len).sum();
        let expected_image_len = usize::try_from(block_offset)
            .ok()
            .and_then(|offset| offset.checked_add(data_len))
            .ok_or_else(|| IoError::WriteFailed("mirror image length overflows".into()))?;
        if data.is_empty() || data.iter().any(Bytes::is_empty) {
            return Err(IoError::Internal("mirror image has invalid view geometry".into()));
        }
        let strip = chunk
            .strips
            .iter()
            .find(|strip| strip.strip_sequence == strip_sequence)
            .ok_or_else(|| IoError::MetadataConflict("mirror strip disappeared".into()))?;
        let Some(Strip::MirrorStrip(mirror)) = &strip.strip else {
            return Err(IoError::MetadataConflict("strip is not mirrored".into()));
        };
        let unit_bytes = u64::from(strip.unit_kb) * 1024;
        if mirror.segments.is_empty()
            || unit_bytes == 0
            || expected_image_len
                > usize::try_from(strip.capacity)
                    .unwrap_or(usize::MAX)
                    .saturating_mul(1024)
        {
            return Err(IoError::MetadataConflict(
                "mirror strip has invalid write geometry".into(),
            ));
        }
        let failed = self
            .write_segments(
                chunk.id,
                strip_sequence,
                &mirror.segments,
                unit_bytes,
                block_offset,
                data,
            )
            .await?;
        if !failed.is_empty() {
            if let Some(pending) = pending_advance.take() {
                *chunk = pending.await.map_err(|error| {
                    IoError::WriteFailed(format!("background advance task panicked: {error}"))
                })??;
            }
        }
        if !failed.is_empty() && full_image.iter().map(Bytes::len).sum::<usize>() != expected_image_len {
            return Err(IoError::Internal(
                "mirror repair prefix has invalid geometry".into(),
            ));
        }
        let repair_image = if failed.is_empty() {
            Bytes::new()
        } else if full_image.len() == 1 {
            full_image[0].clone()
        } else {
            let mut image = bytes::BytesMut::with_capacity(expected_image_len);
            for view in full_image {
                image.extend_from_slice(view);
            }
            image.freeze()
        };
        for segment in failed {
            self.repair(
                chunk,
                committed_cursor,
                strip_sequence,
                segment,
                repair_image.clone(),
                unit_bytes,
            )
            .await?;
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    async fn write_segments(
        &self,
        chunk_id: Option<ChunkId>,
        strip_sequence: u32,
        segments: &[Segment],
        unit_bytes: u64,
        block_offset: u64,
        data: Vec<Bytes>,
    ) -> Result<Vec<Segment>> {
        let mut writes = JoinSet::new();
        for segment in segments {
            let segment = *segment;
            let disk_writer = Arc::clone(&self.disk_writer);
            let data = data.clone();
            writes.spawn(async move {
                let result = disk_writer
                    .write_views_at_byte_offset(&segment, unit_bytes, block_offset, data)
                    .await;
                (segment, result)
            });
        }
        let mut failed = Vec::new();
        while let Some(result) = writes.join_next().await {
            let (segment, result) = result
                .map_err(|error| IoError::WriteFailed(format!("mirror writer task failed: {error}")))?;
            if let Err(error) = result {
                tracing::warn!(?chunk_id, strip_sequence, ?segment, %error, "mirror strip disk write failed");
                failed.push(segment);
            }
        }
        if self.sync {
            let mut syncs = JoinSet::new();
            for segment in segments {
                if !failed.contains(segment) {
                    let segment = *segment;
                    let disk_writer = Arc::clone(&self.disk_writer);
                    syncs.spawn(async move { (segment, disk_writer.fsync(&segment).await) });
                }
            }
            while let Some(result) = syncs.join_next().await {
                let (segment, result) = result
                    .map_err(|error| IoError::WriteFailed(format!("mirror fsync task failed: {error}")))?;
                if let Err(error) = result {
                    tracing::warn!(?chunk_id, strip_sequence, ?segment, %error, "mirror strip fsync failed");
                    failed.push(segment);
                }
            }
        }
        Ok(failed)
    }
}
