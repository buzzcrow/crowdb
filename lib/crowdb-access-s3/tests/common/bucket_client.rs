// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use async_trait::async_trait;
use crowdb_chunk_kv_client::{
    ChunkKvClient, ChunkKvRangeCatalogSource, ChunkKvTransport, ClientConfig, Result,
};
use crowdb_protocol::{chunk_kv::*, chunk_stream::StreamName};
use std::{sync::Arc, time::Duration};

struct TestCatalog(ChunkKvRangeCatalogHead, Vec<ChunkKvRangeCatalogPage>);
#[async_trait]
impl ChunkKvRangeCatalogSource for TestCatalog {
    async fn load(&self) -> Result<(ChunkKvRangeCatalogHead, Vec<ChunkKvRangeCatalogPage>)> {
        Ok((self.0.clone(), self.1.clone()))
    }
}

pub fn client(transport: Arc<dyn ChunkKvTransport>) -> ChunkKvClient {
    let mut page = ChunkKvRangeCatalogPage {
        generation: 1,
        page_index: 0,
        entries: vec![ChunkKvRangeCatalogEntry {
            partition_id: Id128 { high: 1, low: 1 },
            range: KeyRange {
                start: Vec::new(),
                end: None,
            },
            owner: OwnerDescriptor {
                instance_id: 1,
                rpc_endpoint: "owner".into(),
            },
            owner_epoch: 1,
            state: ChunkKvRangeCatalogPartitionState::Serving,
            artifact: PartitionArtifact {
                tree_id: 1,
                stream_name: StreamName { high: 1, low: 1 },
                tail_overlay: None,
            },
            transition_id: None,
        }],
        checksum: [0; 32],
    };
    page.seal().expect("catalog page");
    let mut head = ChunkKvRangeCatalogHead {
        generation: 1,
        previous_generation: None,
        pages: vec![ChunkKvRangeCatalogPageRef {
            page_generation: 1,
            page_index: 0,
            first_key: Vec::new(),
            page_checksum: page.checksum,
        }],
        checksum: [0; 32],
    };
    head.seal().expect("catalog head");
    ChunkKvClient::new(
        ClientConfig {
            operation_timeout: Duration::from_secs(1),
            retry_backoff: Duration::from_millis(1),
            ..ClientConfig::default()
        },
        Arc::new(TestCatalog(head, vec![page])),
        transport,
    )
    .expect("client")
}
