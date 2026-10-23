// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{catalog, policy, EPOCH, INSTANCE_ID};
use crowdb_chunk_kv::memory::MemoryPartitionTree;
use crowdb_chunk_kv::{
    Partition, PartitionConfig, PartitionId, PartitionJournal, PartitionRange, PartitionTree,
    StreamPartitionJournal,
};
use crowdb_chunk_kv_server::ChunkKvService;
use crowdb_chunk_stream::memory::MemoryStreamStore;
use crowdb_chunk_stream::{
    ChunkStream, StreamBinding, StreamBindingState, StreamChunkStore, StreamConfig, StreamMetadataStore,
    StreamName, StreamRegistry,
};
use crowdb_protocol::chunk_kv::{Id128, ServingAssignment, ServingGrant};
use std::sync::Arc;

pub struct TestServingLease {
    pub service: ChunkKvService,
    pub partition: Partition,
    pub store: Arc<MemoryStreamStore>,
}

impl TestServingLease {
    pub async fn start() -> Self {
        let stream_name = StreamName { high: 30, low: 31 };
        let store = Arc::new(MemoryStreamStore::new(4_096));
        let stream = ChunkStream::create(
            StreamBinding {
                purpose: crowdb_protocol::chunk_stream::StreamPurpose::Wal,
                stream_name,
                metadata_group_id: 7,
                binding_generation: 1,
                state: StreamBindingState::Active,
                owner_kind: Some("chunk-kv-partition".into()),
            },
            EPOCH,
            StreamConfig::default(),
            store.clone() as Arc<dyn StreamRegistry>,
            store.clone() as Arc<dyn StreamMetadataStore>,
            store.clone() as Arc<dyn StreamChunkStore>,
        )
        .await
        .unwrap();
        let tree: Arc<dyn PartitionTree> = Arc::new(MemoryPartitionTree::default());
        let journal: Arc<dyn PartitionJournal> =
            Arc::new(StreamPartitionJournal::new(stream, stream_name).unwrap());
        let partition = Partition::open(
            PartitionId { high: 1, low: 2 },
            PartitionRange {
                start: Some(Vec::new()),
                end: None,
            },
            EPOCH,
            PartitionConfig::default(),
            tree,
            journal,
        )
        .unwrap();

        let (head, page) = catalog(Id128 { high: 1, low: 2 }, stream_name, EPOCH, 1, None);

        let service = ChunkKvService::new(INSTANCE_ID, 8).unwrap();
        service
            .install_catalog_and_reconcile(&head, &[page], std::slice::from_ref(&partition))
            .unwrap();
        let mut grant = ServingGrant {
            instance_id: INSTANCE_ID,
            lease_sequence: 1,
            catalog_generation: 1,
            issued_at_ms: 1_000,
            expires_at_ms: 13_000,
            assignments: vec![ServingAssignment {
                partition_id: Id128 { high: 1, low: 2 },
                owner_epoch: EPOCH,
            }],
            assignment_digest: [0; 32],
        };
        grant.seal();
        service
            .authority()
            .install(grant, &policy(), 1_000, 50_000)
            .unwrap();
        Self {
            service,
            partition,
            store,
        }
    }
}
