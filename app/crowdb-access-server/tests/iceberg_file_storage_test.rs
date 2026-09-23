#[path = "common/iceberg_stack.rs"]
mod common;

use std::sync::Arc;

use common::TestIcebergStack;
use crowdb_access_iceberg::catalog::{ActiveCatalogRecord, CatalogContext, CatalogStore, RootState};
use crowdb_access_iceberg::file::{
    ByteRange, ContentFormat, FileContent, FileIdentity, FileKind, FileReader, FileRecord, FileRepository,
    FileTreeWriter, NativeFileBlocks, TableLocation,
};
use crowdb_access_iceberg::key::{CatalogId, FileId, IcebergKey, OperationId, SystemScope, TableId};
use crowdb_access_iceberg::operation::mutation_identity;
use crowdb_access_iceberg::record::StorageRecord;
use crowdb_chunk_client::{ChunkIoClient, ChunkIoClientConfig, SmallWritePolicy};

async fn chunks(stack: &TestIcebergStack) -> ChunkIoClient {
    ChunkIoClient::connect(ChunkIoClientConfig {
        management_seeds: stack.cluster.mgmt_endpoints.clone(),
        diskio_connections_per_endpoint: 2,
        diskio_rpc_workers: 1,
        small_write: SmallWritePolicy {
            min_pipelines: 1,
            max_pipelines: 1,
            memory_budget: 8 * 1024 * 1024,
            mirror_copies: 1,
            conversion_enabled: false,
            ..SmallWritePolicy::default()
        },
    })
    .await
    .unwrap()
}

async fn seed_root(stack: &TestIcebergStack, context: CatalogContext) {
    let key = IcebergKey::System {
        scope: SystemScope::ActiveRoot,
        suffix: Vec::new(),
    }
    .encode()
    .unwrap();
    let bytes = StorageRecord::Active(ActiveCatalogRecord {
        context,
        operation: OperationId::random(),
        state: RootState::Ready,
    })
    .encode()
    .unwrap();
    stack
        .store()
        .await
        .compare_exchange(&key, None, &bytes, mutation_identity(&key, None, &bytes))
        .await
        .unwrap();
}

async fn read_all(mut reader: FileReader) -> Vec<u8> {
    let mut bytes = Vec::new();
    while let Some(frame) = reader.next().await.unwrap() {
        assert!(frame.len() <= 4096);
        bytes.extend_from_slice(&frame);
    }
    bytes
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_file_tree_publication_and_ranges_survive_catalog_storage_restart() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .try_init();
    let mut stack = TestIcebergStack::start().await;
    let context = CatalogContext {
        catalog: CatalogId::random(),
        activation_epoch: 1,
    };
    seed_root(&stack, context).await;
    let owner = FileIdentity {
        table: TableLocation {
            catalog: context.catalog,
            table: TableId::random(),
        },
        file: FileId::random(),
    };
    let client = chunks(&stack).await;
    let blocks = Arc::new(NativeFileBlocks::new(client.clone()));
    let mut writer = FileTreeWriter::new(blocks.clone(), owner, 16 * 1024).unwrap();
    let bytes: Vec<u8> = (0..50_000)
        .map(|index| u8::try_from(index % 251).unwrap())
        .collect();
    for piece in bytes[..25_000].chunks(3000) {
        writer.push(piece).await.unwrap();
    }
    let checkpoint = writer.checkpoint().await.unwrap();
    drop(writer);
    client.shutdown_small_writes().await.unwrap();
    drop(blocks);
    drop(client);
    let client = chunks(&stack).await;
    let blocks = Arc::new(NativeFileBlocks::new(client.clone()));
    let mut writer = FileTreeWriter::restore(blocks.clone(), owner, 16 * 1024, &checkpoint)
        .await
        .unwrap();
    for piece in bytes[25_000..].chunks(3000) {
        writer.push(piece).await.unwrap();
    }
    let tree = writer.finish().await.unwrap();
    assert_eq!(tree.root.as_ref().unwrap().height, 1);
    let candidate = FileRecord {
        file: owner.file,
        location: owner.table.file("data/native.bin").unwrap(),
        kind: FileKind::Data,
        format: ContentFormat::Parquet,
        length: tree.length,
        digest: tree.digest,
        content: FileContent::Chunks { root: tree.root },
        hint: None,
    };
    let repository = FileRepository::new(stack.store().await);
    assert_eq!(repository.publish(context, &candidate).await.unwrap(), candidate);
    let reader = FileReader::new(blocks.clone(), candidate.clone(), None, 4096).unwrap();
    assert_eq!(read_all(reader).await, bytes);
    client.shutdown_small_writes().await.unwrap();
    drop(repository);
    drop(blocks);
    drop(client);
    stack.chunk_kv.restart().await;
    let repository = FileRepository::new(stack.store().await);
    let recovered = repository
        .load(context, &candidate.location)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(recovered, candidate);
    let client = chunks(&stack).await;
    let reader = FileReader::new(
        Arc::new(NativeFileBlocks::new(client.clone())),
        recovered,
        Some(ByteRange {
            start: 16_380,
            end: 33_000,
        }),
        4096,
    )
    .unwrap();
    assert_eq!(read_all(reader).await, bytes[16_380..33_000]);
    assert_eq!(repository.publish(context, &candidate).await.unwrap(), candidate);
    client.shutdown_small_writes().await.unwrap();
}
