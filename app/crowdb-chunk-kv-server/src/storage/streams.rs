// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Stable WAL identity registration and initialization by its actual writer.

use super::{storage_plan_error, ChunkKvStorage, ChunkStream, StreamName, StreamRecord, StreamRecordState};
use crowdb_chunk_stream::StreamRegistry;
use crowdb_protocol::chunk_kv::{ChunkKvRangeCatalogEntry, ChunkKvRangeCatalogPartitionState, Id128};

impl ChunkKvStorage {
    pub(super) async fn register_stream(
        &self,
        stream_name: StreamName,
        partition_id: Id128,
        metadata_group_id: u64,
    ) -> Result<(), crate::MonitorError> {
        let registry = self.streams.registry();
        let expected = StreamRecord {
            purpose: crowdb_protocol::chunk_stream::StreamPurpose::Wal,
            stream_name,
            metadata_group_id,
            record_version: 1,
            state: StreamRecordState::Active,
            consumer_kind: Some("chunk-kv-partition".into()),
            consumer_id: Some(format!("{:016x}{:016x}", partition_id.high, partition_id.low)),
            created_at_ms: StreamRecord::creation_time_ms(),
        };
        match registry
            .load(stream_name)
            .await
            .map_err(|error| storage_plan_error(&error.to_string()))?
        {
            Some(observed) if !observed.same_identity(&expected) || observed.state != expected.state => {
                return Err(storage_plan_error("WAL Stream record conflicts"));
            }
            Some(_) => {}
            None => {
                if let Err(error) = registry.create(expected.clone()).await {
                    let reconciled = registry
                        .load(stream_name)
                        .await
                        .map_err(|error| storage_plan_error(&error.to_string()))?;
                    if reconciled.as_ref().map_or(true, |record| {
                        !record.same_identity(&expected) || record.state != expected.state
                    }) {
                        return Err(storage_plan_error(&error.to_string()));
                    }
                }
            }
        }
        Ok(())
    }

    pub(super) async fn open_or_create_stream(
        &self,
        stream_name: StreamName,
        partition_id: Id128,
        writer_epoch: u64,
        metadata_group_id: u64,
    ) -> Result<ChunkStream, crate::MonitorError> {
        self.register_stream(stream_name, partition_id, metadata_group_id)
            .await?;
        match self.create_registered_stream(stream_name, writer_epoch).await {
            Ok(stream) => Ok(stream),
            Err(initialization) => self
                .open_stream(stream_name, writer_epoch)
                .await
                .map_err(|recovery| {
                    storage_plan_error(&format!(
                        "stream initialization failed: {initialization}; recovery failed: {recovery}"
                    ))
                }),
        }
    }
}

pub(super) fn is_transfer_target(entry: &ChunkKvRangeCatalogEntry) -> bool {
    matches!(
        entry.state,
        ChunkKvRangeCatalogPartitionState::Prepared | ChunkKvRangeCatalogPartitionState::TargetCatchingUp
    ) && entry.artifact.tail_overlay.is_some()
        && entry.transition_id.is_some_and(|id| {
            entry.artifact.stream_name
                == StreamName {
                    high: id.high,
                    low: id.low,
                }
        })
}
