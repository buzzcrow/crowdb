// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_chunk_kv::{ChunkKvError, MutationOperation, Partition, RequestId};
use crowdb_chunk_kv_server::{BootstrapPartitionConfig, ChunkKvServerConfig, ChunkKvStorage};
use crowdb_chunk_stream::memory::MemoryStreamStore;
use crowdb_chunk_stream::{
    ChunkStream, StreamConfig, StreamName, StreamPurpose, StreamRecord, StreamRecordState,
};
use crowdb_protocol::chunk_kv::Id128;
use crowdb_tree_ffi::chunk::test_util::TestChunkBackend;
use std::sync::Arc;

#[path = "common/process.rs"]
mod test_process;

fn assert_resource_log(partition: &Partition, runtime: &Arc<crowdb_tree_ffi::TreeRuntime>) {
    use crowdb_common::metrics::MetricsSampler;
    let service = Arc::new(
        crowdb_chunk_kv_server::ChunkKvService::new(test_process::TestProcess::authority(7), 8).unwrap(),
    );
    service.install_partition(partition).unwrap();
    let mut sampler = crowdb_chunk_kv_server::metrics::TreeResources::new(runtime.clone(), &service);
    sampler.sample();
    let mut bytes = Vec::new();
    sampler.flush(&mut bytes, "test").unwrap();
    let log = String::from_utf8(bytes).unwrap();
    assert!(log.contains("host.tree.resource.cpu_workers"), "{log}");
    assert!(log.contains("chunk.tree.45.resource.writer"), "{log}");
    assert!(log.contains("allocated_sample_max="), "{log}");
}

#[tokio::test]
async fn bootstrap_resume_finishes_its_first_checkpoint_and_does_not_reinitialize_it() {
    let mut server_config = ChunkKvServerConfig::load(
        &std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("conf/crowdb_chunk_kv_server_config.toml"),
    )
    .unwrap();
    server_config.partition.max_key_bytes = 3;
    server_config.partition.batch_requests = 2;
    server_config.validate().unwrap();
    let config = BootstrapPartitionConfig {
        partition_id: Id128 { high: 1, low: 11 },
        tree_id: 45,
        stream_name: StreamName { high: 2, low: 11 },
        owner_epoch: 4,
        metadata_group_id: 7,
    };
    let name = StreamName { high: 2, low: 11 };
    let store = Arc::new(MemoryStreamStore::new(4096));
    let stream = create_stream(name, &store).await;
    let mut backend = TestChunkBackend::new(45, 4).unwrap();
    let pages = backend.page_store(0).unwrap();
    let tree_config = crowdb_tree_ffi::Config {
        runtime: Some(backend.runtime.clone()),
        ..server_config.native_tree_options()
    };
    let partition = ChunkKvStorage::open_bootstrap_partition_for_tests(
        &config,
        server_config.partition.clone(),
        tree_config.clone(),
        pages,
        stream,
    )
    .await
    .unwrap();
    assert_resource_log(&partition, &backend.runtime);
    assert_partition_limit(&partition).await;
    partition
        .mutate(
            4,
            RequestId {
                client_high: 1,
                client_low: 11,
                client_sequence: 1,
            },
            MutationOperation::Put {
                key: Vec::new(),
                value: b"crowdb-bootstrap".to_vec(),
            },
        )
        .await
        .unwrap();
    partition.suspend_for_transfer(4).await.unwrap();
    partition.close().await.unwrap();
    let stream = reopen(name, &store).await;
    backend.next_writer().unwrap();
    let pages = backend.page_store(0).unwrap();
    let recovered = ChunkKvStorage::recover_bootstrap_partition_for_tests(
        &config,
        server_config.partition.clone(),
        tree_config.clone(),
        pages.clone(),
        stream,
    )
    .await
    .unwrap();
    assert!(recovered.get(4, b"", None).await.unwrap().is_none());
    assert_partition_limit(&recovered).await;
    let generation = pages.chunk_manifest_generation().unwrap();
    assert!(generation > 0);
    recovered.suspend_for_transfer(4).await.unwrap();
    recovered.close().await.unwrap();
    let stream = reopen(name, &store).await;
    backend.next_writer().unwrap();
    let pages = backend.page_store(0).unwrap();
    let recovered = ChunkKvStorage::recover_bootstrap_partition_for_tests(
        &config,
        server_config.partition.clone(),
        tree_config,
        pages.clone(),
        stream,
    )
    .await
    .unwrap();
    assert_eq!(pages.chunk_manifest_generation().unwrap(), generation);
    recovered.activate_recovered(4).unwrap();
    assert_partition_limit(&recovered).await;
    assert!(recovered.get(4, b"", None).await.unwrap().is_none());
    recovered.suspend_for_shutdown(4).await.unwrap();
    recovered.close().await.unwrap();
}

async fn reopen(name: StreamName, store: &Arc<MemoryStreamStore>) -> ChunkStream {
    ChunkStream::open(
        name,
        4,
        StreamConfig::default(),
        store.clone(),
        store.clone(),
        store.clone(),
    )
    .await
    .unwrap()
}

async fn assert_partition_limit(partition: &Partition) {
    let result = partition
        .mutate(
            4,
            RequestId {
                client_high: 1,
                client_low: 12,
                client_sequence: 1,
            },
            MutationOperation::Put {
                key: b"four".to_vec(),
                value: b"value".to_vec(),
            },
        )
        .await;
    assert!(matches!(result, Err(ChunkKvError::InvalidRequest(_))));
}

async fn create_stream(name: StreamName, store: &Arc<MemoryStreamStore>) -> ChunkStream {
    ChunkStream::create(
        StreamRecord {
            purpose: StreamPurpose::Wal,
            stream_name: name,
            metadata_group_id: 7,
            record_version: 1,
            state: StreamRecordState::Active,
            consumer_kind: Some("chunk-kv-partition".into()),
            consumer_id: None,
            created_at_ms: StreamRecord::creation_time_ms(),
        },
        4,
        StreamConfig::default(),
        store.clone(),
        store.clone(),
        store.clone(),
    )
    .await
    .unwrap()
}
