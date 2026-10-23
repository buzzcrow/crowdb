// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Bind physical submissions to the original local owner context.

use super::ProductionStreamChunkStore;
use crate::{Result, StreamError, StreamName, StreamWriteContext};
use async_trait::async_trait;
use bytes::Bytes;
use crowdb_chunk_client::{DiskWriter, IoError};
use crowdb_protocol::diskdb::rpc::Segment;
use std::sync::Arc;

struct OwnedDiskWriter {
    inner: Arc<dyn DiskWriter>,
    context: StreamWriteContext,
}

impl ProductionStreamChunkStore {
    pub(super) fn owner_context(&self, name: StreamName, epoch: u64) -> Result<Option<StreamWriteContext>> {
        self.owners
            .get(&name)
            .map(|entry| {
                if entry.value().0 != epoch {
                    return Err(StreamError::StaleWriter);
                }
                entry.value().1.capture()
            })
            .transpose()
    }

    pub(super) fn owned_disk_writer(&self, context: Option<StreamWriteContext>) -> Arc<dyn DiskWriter> {
        match context {
            Some(context) => Arc::new(OwnedDiskWriter {
                inner: self.disk_writer.clone(),
                context,
            }),
            None => self.disk_writer.clone(),
        }
    }
}

#[async_trait]
impl DiskWriter for OwnedDiskWriter {
    async fn write_at_byte_offset(
        &self,
        segment: &Segment,
        unit_bytes: u64,
        offset: u64,
        data: Bytes,
    ) -> crowdb_chunk_client::Result<()> {
        self.write_synchronized_at_byte_offset(segment, unit_bytes, offset, data)
            .await
    }
    async fn write(
        &self,
        _segment: &Segment,
        _unit_bytes: u64,
        _data: Bytes,
    ) -> crowdb_chunk_client::Result<()> {
        Err(IoError::WriteFailed(
            "owned stream IO requires synchronization".into(),
        ))
    }

    async fn write_synchronized_views_at_byte_offset(
        &self,
        segment: &Segment,
        unit_bytes: u64,
        offset: u64,
        data: Vec<Bytes>,
    ) -> crowdb_chunk_client::Result<()> {
        self.context.check_io().map_err(|error| authority_error(&error))?;
        self.inner
            .write_synchronized_views_until(segment, unit_bytes, offset, data, self.context.deadline())
            .await?;
        self.context.check_ack().map_err(|error| authority_error(&error))
    }

    async fn write_synchronized_at_byte_offset(
        &self,
        segment: &Segment,
        unit_bytes: u64,
        offset: u64,
        data: Bytes,
    ) -> crowdb_chunk_client::Result<()> {
        self.write_synchronized_views_at_byte_offset(segment, unit_bytes, offset, vec![data])
            .await
    }

    async fn initialize_synchronized_at_byte_offset(
        &self,
        segment: &Segment,
        unit_bytes: u64,
        offset: u64,
        data: Bytes,
    ) -> crowdb_chunk_client::Result<()> {
        self.context.check_io().map_err(|error| authority_error(&error))?;
        self.inner
            .initialize_synchronized_until(segment, unit_bytes, offset, data, self.context.deadline())
            .await?;
        self.context.check_ack().map_err(|error| authority_error(&error))
    }
}

fn authority_error(error: &StreamError) -> IoError {
    IoError::MetadataConflict(error.to_string())
}
