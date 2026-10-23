// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::sync::Arc;

use crowdb_chunk_kv::memory::MemoryPartitionTree;
use crowdb_chunk_kv::{
    ChunkKvError, MutationOperation, Partition, PartitionConfig, PartitionId, PartitionJournal,
    PartitionRange, RequestId, StreamPartitionJournal, TransitionId,
};
use crowdb_chunk_stream::memory::MemoryStreamStore;
use crowdb_chunk_stream::{
    ChunkStream, StreamBinding, StreamBindingState, StreamConfig, StreamMetadataStore, StreamName,
    StreamPurpose,
};

const EPOCH: u64 = 7;
const NAME: StreamName = StreamName { high: 9, low: 11 };
const ID: PartitionId = PartitionId { high: 9, low: 12 };

struct TestPartition {
    partition: Partition,
    store: Arc<MemoryStreamStore>,
    journal: Arc<dyn PartitionJournal>,
    config: PartitionConfig,
}

impl TestPartition {
    async fn open() -> Self {
        let store = Arc::new(MemoryStreamStore::new(512));
        let stream = ChunkStream::create(
            StreamBinding {
                purpose: StreamPurpose::Wal,
                stream_name: NAME,
                metadata_group_id: 1,
                binding_generation: 1,
                state: StreamBindingState::Active,
                owner_kind: Some("chunk-kv-partition".into()),
            },
            EPOCH,
            StreamConfig {
                extent_page_entries: 4,
                ..StreamConfig::default()
            },
            store.clone(),
            store.clone(),
            store.clone(),
        )
        .await
        .unwrap();
        let journal: Arc<dyn PartitionJournal> = Arc::new(StreamPartitionJournal::new(stream, NAME).unwrap());
        let tree = Arc::new(MemoryPartitionTree::default());
        let config = PartitionConfig {
            retained_results: 2,
            metadata_reclaim_pages_per_pass: 2,
            ..PartitionConfig::default()
        };
        let partition =
            Partition::open(ID, range(), EPOCH, config.clone(), tree.clone(), journal.clone()).unwrap();
        Self {
            partition,
            store,
            journal,
            config,
        }
    }

    async fn write(&self, sequence: u64) -> crowdb_chunk_kv::MutationResponse {
        self.partition
            .mutate(EPOCH, request(sequence), operation(sequence))
            .await
            .unwrap()
    }

    async fn drain(&self) {
        for _ in 0..256 {
            if !self.partition.maintain_serving_metadata(EPOCH).await.unwrap() {
                return;
            }
        }
        panic!("bounded maintenance did not drain its finite metadata backlog");
    }
}

fn range() -> PartitionRange {
    PartitionRange {
        start: None,
        end: None,
    }
}

fn request(sequence: u64) -> RequestId {
    RequestId {
        client_high: 1,
        client_low: 2,
        client_sequence: sequence,
    }
}

fn operation(sequence: u64) -> MutationOperation {
    MutationOperation::Put {
        key: b"key".to_vec(),
        value: sequence.to_be_bytes().to_vec(),
    }
}

#[tokio::test]
async fn superseded_directories_are_bounded_and_old_checkpoints_keep_retry_history() {
    let fixture = TestPartition::open().await;
    let checkpoint = fixture.partition.checkpoint(EPOCH).await.unwrap();
    for round in 0..20 {
        for sequence in (round * 8 + 1)..=(round * 8 + 8) {
            fixture.write(sequence).await;
        }
        let checkpoints = fixture.partition.metrics().snapshot().checkpoints;
        fixture.drain().await;
        assert_eq!(fixture.partition.metrics().snapshot().checkpoints, checkpoints);
        let current = fixture.store.load_current(NAME).await.unwrap().unwrap();
        assert_eq!(
            fixture.store.extent_page_count().await,
            current.extent_pages.len()
        );
        fixture.drain().await;
        assert_eq!(fixture.partition.metrics().snapshot().checkpoints, checkpoints);
    }
    let expected = fixture.write(161).await;
    fixture.drain().await;
    assert_eq!(checkpoint.replay_offset, 0);
    fixture.partition.suspend_for_transfer(EPOCH).await.unwrap();
    let recovered = Partition::recover(
        ID,
        range(),
        EPOCH,
        checkpoint,
        fixture.config,
        Arc::new(MemoryPartitionTree::default()),
        fixture.journal,
    )
    .await
    .unwrap();
    assert_eq!(
        recovered.get(EPOCH, b"key", None).await.unwrap().unwrap().value,
        161_u64.to_be_bytes()
    );
    assert_eq!(
        recovered
            .mutate(EPOCH, request(161), operation(161))
            .await
            .unwrap(),
        expected
    );
    assert_eq!(
        recovered.mutate(EPOCH, request(1), operation(1)).await,
        Err(ChunkKvError::RequestExpired)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_transition_pin_keeps_source_wal_and_extent_pages() {
    let fixture = TestPartition::open().await;
    for sequence in 1..=16 {
        fixture.write(sequence).await;
    }
    let transition = TransitionId { high: 90, low: 91 };
    let (_, checkpoint) = tokio::join!(
        fixture.partition.maintain_serving_metadata(EPOCH),
        fixture.partition.checkpoint_with_pin(EPOCH, transition),
    );
    let checkpoint = checkpoint.unwrap();
    let retained = fixture.store.load_current(NAME).await.unwrap().unwrap();
    for sequence in 17..=32 {
        fixture.write(sequence).await;
    }
    let before = fixture.store.extent_page_count().await;
    assert!(!fixture.partition.maintain_serving_metadata(EPOCH).await.unwrap());
    assert_eq!(fixture.store.extent_page_count().await, before);
    assert!(matches!(
        fixture
            .partition
            .reclaim_published_checkpoint(EPOCH, &checkpoint)
            .await,
        Err(ChunkKvError::SplitRetry(_))
    ));
    for fence in retained.extent_pages {
        assert!(fixture
            .store
            .load_extent_page(NAME, retained.writer_epoch, retained.generation, fence.page_index)
            .await
            .unwrap()
            .is_some());
    }
    assert!(!fixture
        .journal
        .read_window(checkpoint.replay_offset, 4096)
        .await
        .unwrap()
        .is_empty());
    fixture.partition.release_generation_pin(transition).unwrap();
    fixture.drain().await;
    assert!(fixture.store.extent_page_count().await < before);
    assert_eq!(
        fixture.partition.maintain_serving_metadata(EPOCH + 1).await,
        Err(ChunkKvError::StaleEpoch)
    );
}

#[tokio::test]
async fn physical_wal_trim_rejects_a_checkpoint_without_durable_retry_floors() {
    let fixture = TestPartition::open().await;
    let initial = fixture.partition.checkpoint(EPOCH).await.unwrap();
    for sequence in 1..=8 {
        fixture.write(sequence).await;
    }
    let checkpoint = fixture.partition.checkpoint(EPOCH).await.unwrap();
    assert!(checkpoint.replay_offset > 0);
    assert!(matches!(
        fixture
            .partition
            .reclaim_published_checkpoint(EPOCH, &checkpoint)
            .await,
        Err(ChunkKvError::MaintenanceDegraded(_))
    ));
    assert!(!fixture
        .journal
        .read_window(initial.replay_offset, 4096)
        .await
        .unwrap()
        .is_empty());
    let recovered = Partition::recover(
        ID,
        range(),
        EPOCH,
        initial,
        fixture.config,
        Arc::new(MemoryPartitionTree::default()),
        fixture.journal,
    )
    .await
    .unwrap();
    assert_eq!(
        recovered.mutate(EPOCH, request(1), operation(1)).await,
        Err(ChunkKvError::RequestExpired)
    );
}
