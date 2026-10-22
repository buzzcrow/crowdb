// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use bytes::Bytes;
use crowdb_chunk_stream::{
    ChunkId, MirrorStripImage, ProductionStreamChunkStore, StreamChunkStore, StreamName,
};
use crowdb_protocol::frame::encode_append_group_views;

#[derive(Default)]
pub struct TestGroupedStrip {
    prefix: Vec<u8>,
}

impl TestGroupedStrip {
    pub async fn append(
        &mut self,
        store: &ProductionStreamChunkStore,
        name: StreamName,
        epoch: u64,
        chunk: ChunkId,
        payload: &'static [u8],
    ) -> u64 {
        let offset = self.prefix.len() as u64;
        let views = encode_append_group_views(chunk, offset, &Bytes::from_static(payload), 1).unwrap();
        let bytes = Bytes::from(
            views
                .iter()
                .flat_map(|view| view.iter().copied())
                .collect::<Vec<_>>(),
        );
        self.prefix.extend_from_slice(&bytes);
        store
            .write_mirrors_with_images(
                name,
                epoch,
                chunk,
                offset,
                bytes.clone(),
                &[MirrorStripImage {
                    block_offset: offset,
                    data: bytes,
                    full_image: Bytes::copy_from_slice(&self.prefix),
                }],
            )
            .await
            .unwrap();
        self.prefix.len() as u64
    }
}
