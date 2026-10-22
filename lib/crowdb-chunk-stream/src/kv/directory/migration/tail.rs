// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_protocol::chunk_stream::{
    LegacyManifestView, StreamExtentLocation, StreamFrameFormat, StreamMigrationPhase,
    StreamMigrationProgress,
};
use crowdb_protocol::frame::{
    frame_length, parse_frame, parse_header, ChunkLocation, FrameMagic, FRAME_HEADER_PREFIX_BYTES,
};

use super::{migration_error, KvStreamMetadataStore, StreamMigrationSnapshot};
use crate::{DirectoryIndex, DirectoryMutation, Result, StreamChunkStore};

impl KvStreamMetadataStore {
    pub(super) async fn prepare_migration_tail(
        &self,
        source: &LegacyManifestView<'_>,
        progress: &mut StreamMigrationProgress,
        chunks: &dyn StreamChunkStore,
    ) -> Result<()> {
        if progress.logical_tail != source.sealed_tail {
            return Err(migration_error("legacy history ends before its published tail"));
        }
        progress.legacy_root = progress.root;
        let Some(active) = &source.active else {
            progress.phase = StreamMigrationPhase::Complete;
            return Ok(());
        };
        if active.logical_start != source.sealed_tail || active.physical_start != active.acknowledged_cursor {
            return Err(migration_error("legacy active checkpoint has an unmapped range"));
        }
        if let Some(root) = progress.root {
            let index = DirectoryIndex::new(source.stream_name, source.purpose, Some(root))?;
            let last = index
                .lookup(self, root.end_logical - 1)
                .await?
                .ok_or_else(|| migration_error("legacy checkpoint has no last location"))?;
            if (last.location.chunk_id == active.chunk_id
                && last.end_physical().map_err(migration_error)? != active.acknowledged_cursor)
                || (last.location.chunk_id != active.chunk_id && active.acknowledged_cursor != 0)
            {
                return Err(migration_error(
                    "legacy active checkpoint differs from published locations",
                ));
            }
        }
        let durable = chunks
            .durable_cursor(active.chunk_id, source.writer_epoch)
            .await?;
        if durable.offset < active.acknowledged_cursor {
            return Err(migration_error(
                "legacy durable cursor is behind its published prefix",
            ));
        }
        if !durable.sealed {
            chunks
                .seal(active.chunk_id, source.writer_epoch, durable.offset)
                .await?;
        }
        let sealed = chunks
            .durable_cursor(active.chunk_id, source.writer_epoch)
            .await?;
        if !sealed.sealed || sealed.offset < active.acknowledged_cursor {
            return Err(migration_error(
                "legacy seal did not preserve its published checkpoint",
            ));
        }
        progress.phase = if sealed.offset == active.acknowledged_cursor {
            StreamMigrationPhase::Complete
        } else {
            StreamMigrationPhase::Tail {
                physical: active.acknowledged_cursor,
                sealed_end: sealed.offset,
            }
        };
        Ok(())
    }

    pub(super) async fn convert_migration_tail(
        &self,
        snapshot: &StreamMigrationSnapshot,
        progress: &mut StreamMigrationProgress,
        chunks: &dyn StreamChunkStore,
        limit: usize,
    ) -> Result<DirectoryMutation> {
        let source = snapshot.source_view()?;
        let active = source
            .active
            .as_ref()
            .ok_or_else(|| migration_error("legacy tail has no active source chunk"))?;
        let StreamMigrationPhase::Tail {
            mut physical,
            sealed_end,
        } = progress.phase
        else {
            unreachable!()
        };
        let mut logical = progress.logical_tail;
        let mut locations = Vec::with_capacity(limit);
        while physical < sealed_end && locations.len() < limit {
            if sealed_end - physical < FRAME_HEADER_PREFIX_BYTES as u64 {
                return Err(migration_error("legacy durable cursor ends inside a header"));
            }
            let prefix = chunks
                .read(active.chunk_id, physical, FRAME_HEADER_PREFIX_BYTES)
                .await?;
            let header = parse_header(&prefix).map_err(migration_error)?;
            let length = frame_length(header).map_err(migration_error)?;
            if header.magic != FrameMagic::StreamV1
                || usize::from(header.payload_offset) != FRAME_HEADER_PREFIX_BYTES
                || length as u64 > sealed_end - physical
            {
                return Err(migration_error("legacy durable cursor crosses an invalid frame"));
            }
            let frame = chunks
                .read_verified_frame(active.chunk_id, physical, length)
                .await?;
            let parsed = parse_frame(&frame, active.chunk_id).map_err(migration_error)?;
            let location = StreamExtentLocation {
                logical_start: logical,
                location: ChunkLocation {
                    chunk_id: active.chunk_id,
                    frame_offset: physical,
                    logical_length: parsed.payload.len() as u64,
                },
                format: StreamFrameFormat::LegacyV1,
            };
            logical = location.end_logical().map_err(migration_error)?;
            physical = location.end_physical().map_err(migration_error)?;
            locations.push(location);
        }
        let index = DirectoryIndex::new(source.stream_name, source.purpose, progress.root)?;
        let mutation = index.prepare_append(self, &locations).await?;
        progress.root = mutation.index().root();
        progress.logical_tail = logical;
        progress.phase = if physical == sealed_end {
            StreamMigrationPhase::Complete
        } else {
            StreamMigrationPhase::Tail { physical, sealed_end }
        };
        Ok(mutation)
    }
}
