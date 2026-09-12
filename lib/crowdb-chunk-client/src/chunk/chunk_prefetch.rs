// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! `ChunkPrefetch` — chunk-level prefetch for the object layer.
//!
//! Pre-allocates chunks (each with 1 strip) ahead of the write cursor.
//! `ChunkWriter` holds `Arc<Chunk>` and shares it with `EcStripWriter`
//! directly. Strip-level prefetch is internal to `ChunkWriter`.

use std::sync::Arc;

use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::config::ChunkClientConfig;
use crate::traits::ChunkAllocator;
use crate::{IoError, Result};
use crowdb_common::ec::EcScheme;
use crowdb_protocol::chunkdb::rpc::{AllocateChunkRequest, Chunk, ChunkType, StripType};

/// Chunk preallocation + strip prefetch.
pub struct ChunkPrefetch {
    pub(crate) allocator: Arc<dyn ChunkAllocator>,
    pub(crate) ec_scheme: EcScheme,
    pub(crate) config: Arc<ChunkClientConfig>,
    pub(crate) chunk_type_byte: u8,
}

impl ChunkPrefetch {
    /// Construct a new prefetch task.
    pub fn new(
        allocator: Arc<dyn ChunkAllocator>,
        ec_scheme: EcScheme,
        config: Arc<ChunkClientConfig>,
        chunk_type_byte: u8,
    ) -> Self {
        Self {
            allocator,
            ec_scheme,
            config,
            chunk_type_byte,
        }
    }

    /// Spawn the prefetch task. Returns the chunk receiver + join
    /// handle. Maintains `chunk_preparation_depth` chunks ahead,
    /// each with 1 strip, then appends strips ahead of the write
    /// cursor. Sends the full cumulative `Chunk` after each
    /// strip-append.
    pub fn spawn(self, object_size: Option<u64>) -> (mpsc::Receiver<Result<Chunk>>, JoinHandle<()>) {
        let (tx, rx) = mpsc::channel(self.config.chunk_preparation_depth.max(1));
        let handle = tokio::spawn(async move {
            if let Err(e) = self.run(object_size, &tx).await {
                let _ = tx.send(Err(e)).await;
            }
        });
        (rx, handle)
    }

    /// Run the chunk-prefetch loop. Pre-allocates chunks (each with 1
    /// strip) ahead of the write cursor, bounded by the channel
    /// capacity. Each `Chunk` sent has 1 strip — `ChunkWriter`'s
    /// internal strip prefetch appends more as needed. For known-size
    /// objects, pre-allocates enough chunks to hold the entire object;
    /// for unknown-size objects, continues allocating as the bounded receiver
    /// consumes prepared chunks.
    async fn run(self, object_size: Option<u64>, tx: &mpsc::Sender<Result<Chunk>>) -> Result<()> {
        let write_granularity_kb = (self.config.read_buffer_size / 1024) as u32;
        let unit_bytes = u64::from(write_granularity_kb) * 1024;
        let strip_data_capacity = self.ec_scheme.data_num as u64 * unit_bytes;
        let strips_per_chunk = (self.config.max_chunk_size / strip_data_capacity).max(1) as u32;
        let chunk_data_capacity = strip_data_capacity * u64::from(strips_per_chunk);

        let total_chunks = object_size.map(|s| (s.div_ceil(chunk_data_capacity)) as usize);
        let mut allocated = 0usize;
        loop {
            // Stop conditions:
            // - known-size and all chunks allocated
            if let Some(total) = total_chunks {
                if allocated >= total {
                    break;
                }
            }
            // Reserve bounded receiver capacity before allocating so an
            // unconsumed extra chunk is never stranded behind a full channel.
            let permit = tx
                .reserve()
                .await
                .map_err(|_| IoError::Internal("prealloc receiver dropped".into()))?;
            let chunk = allocate_new_chunk(
                &self.allocator,
                self.ec_scheme,
                write_granularity_kb,
                self.chunk_type_byte,
                self.config.prefetch_strips_per_chunk,
            )
            .await?;

            permit.send(Ok(chunk));
            allocated += 1;
        }
        Ok(())
    }

    /// On-demand chunk allocation when the prefetch task has finished
    /// but more data remains. Allocates a new chunk with 1 strip.
    pub async fn on_demand(&self) -> Result<Chunk> {
        let write_granularity_kb = (self.config.read_buffer_size / 1024) as u32;
        allocate_new_chunk(
            &self.allocator,
            self.ec_scheme,
            write_granularity_kb,
            self.chunk_type_byte,
            self.config.prefetch_strips_per_chunk,
        )
        .await
    }
}

/// Allocate a new chunk with 1 strip and return the full `Chunk`.
pub(crate) async fn allocate_new_chunk(
    chunkdb: &dyn ChunkAllocator,
    ec_scheme: EcScheme,
    write_granularity_kb: u32,
    chunk_type_byte: u8,
    prefetch_strips_per_chunk: usize,
) -> Result<Chunk> {
    let chunk_id = crowdb_protocol::generate_chunk_id(chunk_type_byte).to_proto();
    let req = AllocateChunkRequest {
        chunk_id: Some(chunk_id),
        write_granularity: write_granularity_kb,
        strip_count: u32::try_from(prefetch_strips_per_chunk).unwrap_or(u32::MAX),
        strip_type: StripType::Ec as i32,
        data_num: ec_scheme.data_num as u32,
        code_num: ec_scheme.code_num as u32,
        copy_count: 0,
        chunk_type: ChunkType::Repo as i32,
        writer_epoch: 0,
        writer_lease_ms: 0,
        owner_key: Vec::new(),
    };
    let resp = chunkdb.allocate_chunk(req).await?;
    resp.chunk
        .ok_or_else(|| IoError::AllocationFailed("allocate_chunk response missing chunk".into()))
}
