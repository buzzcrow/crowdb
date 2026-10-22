// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::sync::Arc;

use crowdb_chunk_stream::{
    StreamExtentPage, StreamExtentPageFence, StreamManifest, StreamName, StreamPurpose,
};
use crowdb_kv::cluster::group::PxGroup;
use crowdb_kv::cluster::kv_server::KvServer;
use crowdb_kv::cluster::local_replica::{PxLocalReplica, PxLocalReplicaRole};
use crowdb_kv::cluster::px_kv_store::PxKvStore;
use crowdb_kv_client::{ClientConfig, CrowdbKvClient};

pub struct TestMetadataServer {
    server: Arc<PxKvStore>,
    pub client: Arc<CrowdbKvClient>,
}

impl TestMetadataServer {
    pub async fn start() -> Self {
        let server = Arc::new(PxKvStore::new(1, "127.0.0.1:0".parse().unwrap()));
        server.add_group(PxGroup::new(
            1,
            PxLocalReplica::new(1, PxLocalReplicaRole::Leader),
        ));
        server.start().await.unwrap();
        // This fixture explicitly seeds its one real RPC endpoint; discovery is
        // outside the metadata adapter contract exercised here.
        let client = Arc::new(CrowdbKvClient::new(ClientConfig::new(Vec::new())));
        client.seed_leader(1, 1, server.listen_addr().unwrap().to_string());
        Self { server, client }
    }
}

impl Drop for TestMetadataServer {
    fn drop(&mut self) {
        self.server.stop();
    }
}

pub fn candidate(epoch: u64, generation: u64, count: u64) -> (StreamManifest, Vec<StreamExtentPage>) {
    let name = StreamName { high: 12, low: 41 };
    let pages: Vec<_> = (0..count)
        .map(|index| StreamExtentPage {
            stream_name: name,
            writer_epoch: epoch,
            generation,
            page_index: index,
            chunk_ids: vec![crowdb_chunk_stream::ChunkId {
                high: 4_u64 << 56,
                low: index + 1,
            }],
            logical_offsets: vec![index, index + 1],
            physical_offsets: vec![0],
            frame_lengths: vec![35],
        })
        .collect();
    let manifest = StreamManifest {
        purpose: StreamPurpose::Stream,
        stream_name: name,
        metadata_group_id: 1,
        writer_epoch: epoch,
        generation,
        trim_offset: 0,
        sealed_tail: count,
        active: None,
        extent_pages: pages
            .iter()
            .map(|page| StreamExtentPageFence {
                page_index: page.page_index,
                first_logical: page.logical_offsets[0],
                end_logical: page.logical_offsets[1],
            })
            .collect(),
        previous_generation: generation.checked_sub(1),
        closed: false,
    };
    (manifest, pages)
}
