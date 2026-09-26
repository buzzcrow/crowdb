#[path = "common/iceberg_stack.rs"]
#[allow(dead_code)]
mod common;

use std::sync::Arc;

use common::TestIcebergStack;
use crowdb_access_iceberg::{
    catalog::{CatalogRepository, ClearBounds, ManagementPrivilege},
    file::{
        ContentFormat, FileContent, FileIdentity, FileKind, FileReader, FileRecord, FileRepository,
        FileTreeWriter, NativeFileBlocks, TableLocation,
    },
    key::{FileId, OperationId, TableId},
    operation::{ManagementAction, ManagementRequest, RequestIdentity},
};
use crowdb_chunk_client::{ChunkIoClient, ChunkIoClientConfig, SmallWritePolicy};
use crowdb_diskdb_client::{DiskdbClient, DiskdbClientError, DiskdbRpcTransport};
use crowdb_protocol::{
    common::ChunkId,
    diskdb::rpc::{
        AllocateBlocksRequest, CommitBlocksRequest, CompactZoneRequest, FreeBlocksRequest, Segment,
    },
};

async fn seed_catalog(stack: &TestIcebergStack) -> crowdb_access_iceberg::catalog::CatalogContext {
    let repository = CatalogRepository::new(stack.store().await, ClearBounds::default()).unwrap();
    let now = common::now_ms();
    repository
        .execute(
            ManagementRequest {
                identity: RequestIdentity {
                    operation: OperationId::random(),
                    issued_ms: now,
                },
                principal: "manager".into(),
                action: ManagementAction::Initialize,
                expected_epoch: 0,
                display_name: "capacity".into(),
                confirmation: None,
                capabilities: None,
            },
            ManagementPrivilege::Manage,
            now,
        )
        .await
        .unwrap();
    repository.status().await.unwrap().0.context
}

async fn chunks(stack: &TestIcebergStack) -> ChunkIoClient {
    ChunkIoClient::connect(ChunkIoClientConfig {
        management_seeds: stack.cluster.mgmt_endpoints.clone(),
        diskio_connections_per_endpoint: 1,
        diskio_rpc_workers: 1,
        small_write: SmallWritePolicy {
            min_pipelines: 1,
            max_pipelines: 1,
            memory_budget: 8 * 1024 * 1024,
            chunk_capacity: 1024 * 1024 * 1024,
            mirror_copies: 1,
            conversion_enabled: false,
            ..SmallWritePolicy::default()
        },
    })
    .await
    .unwrap()
}

async fn fill_disk(client: &DiskdbClient) -> Vec<Segment> {
    let mut held = Vec::new();
    for sequence in 1..=256_u64 {
        let mut allocated = None;
        for units in [1024, 128, 1] {
            match client
                .allocate_blocks(AllocateBlocksRequest {
                    disk_group_id: 100,
                    unit_count: units,
                    count: 1,
                    exclude_disk_ids: Vec::new(),
                    owner_chunk: Some(ChunkId {
                        high: 77,
                        low: sequence,
                    }),
                    allow_disk_reuse: false,
                })
                .await
            {
                Ok(response) => {
                    allocated = Some(response.segments);
                    break;
                }
                Err(DiskdbClientError::NoSpace(_)) => {}
                Err(error) => panic!("disk allocation failed unexpectedly: {error}"),
            }
        }
        let Some(segments) = allocated else { break };
        assert_eq!(
            client
                .commit_blocks(CommitBlocksRequest {
                    segments: segments.clone()
                })
                .await
                .unwrap()
                .committed_count,
            u32::try_from(segments.len()).unwrap()
        );
        held.extend(segments);
    }
    assert!(!held.is_empty());
    assert_eq!(
        client.query_disk_group(100).await.unwrap().disk_groups[0].free_bytes,
        0
    );
    held
}

async fn write_file(
    stack: &TestIcebergStack,
    client: &ChunkIoClient,
    owner: FileIdentity,
) -> Result<FileRecord, crowdb_access_iceberg::file::FileIoError> {
    let blocks = Arc::new(NativeFileBlocks::new(client.clone(), stack.store().await));
    let mut writer = FileTreeWriter::new(blocks, owner, 16 * 1024).unwrap();
    writer.push(&vec![31; 32 * 1024]).await?;
    let tree = writer.finish().await?;
    Ok(FileRecord {
        file: owner.file,
        location: owner.table.file("data/capacity.parquet").unwrap(),
        kind: FileKind::Data,
        format: ContentFormat::Parquet,
        length: tree.length,
        digest: tree.digest,
        content: FileContent::Chunks { root: tree.root },
        hint: None,
    })
}

async fn read_file(stack: &TestIcebergStack, client: &ChunkIoClient, file: FileRecord) -> Vec<u8> {
    let blocks = Arc::new(NativeFileBlocks::new(client.clone(), stack.store().await));
    let mut reader = FileReader::new(blocks, file, None, 4096).unwrap();
    let mut bytes = Vec::new();
    while let Some(frame) = reader.next().await.unwrap() {
        bytes.extend_from_slice(&frame);
    }
    bytes
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn full_simulated_disk_preserves_file_authority_then_recovers_after_compaction() {
    let stack = TestIcebergStack::start().await;
    let context = seed_catalog(&stack).await;
    let owner = FileIdentity {
        table: TableLocation {
            catalog: context.catalog,
            table: TableId::random(),
        },
        file: FileId::random(),
    };
    let committed_owner = FileIdentity {
        table: owner.table,
        file: FileId::random(),
    };
    let client = chunks(&stack).await;
    let mut committed = write_file(&stack, &client, committed_owner).await.unwrap();
    committed.location = owner.table.file("data/committed.parquet").unwrap();
    let files = FileRepository::new(stack.store().await);
    files.publish(context, &committed).await.unwrap();
    client.shutdown_small_writes().await.unwrap();
    drop(client);
    let disk = DiskdbClient::new(
        stack.cluster.make_service_registry_client(),
        Arc::new(DiskdbRpcTransport::new()),
    );
    disk.refresh_endpoints().await.unwrap();
    let held = fill_disk(&disk).await;
    let client = chunks(&stack).await;
    let failure = write_file(&stack, &client, owner).await.unwrap_err();
    assert!(matches!(
        failure,
        crowdb_access_iceberg::file::FileIoError::Write(_)
    ));
    let location = owner.table.file("data/capacity.parquet").unwrap();
    assert!(files.load(context, &location).await.unwrap().is_none());
    assert_eq!(
        files.load(context, &committed.location).await.unwrap(),
        Some(committed.clone())
    );
    assert_eq!(
        read_file(&stack, &client, committed.clone()).await,
        vec![31; 32 * 1024]
    );
    drop(client);
    for batch in held.chunks(100) {
        assert_eq!(
            disk.free_blocks(FreeBlocksRequest {
                segments: batch.to_vec()
            })
            .await
            .unwrap()
            .freed_count as usize,
            batch.len()
        );
    }
    let compacted = disk
        .compact_zone(CompactZoneRequest {
            disk_id: held[0].disk_id,
            zone_indices: Vec::new(),
        })
        .await
        .unwrap();
    assert!(compacted.zones.iter().all(|zone| zone.success));
    assert!(disk.query_disk_group(100).await.unwrap().disk_groups[0].free_bytes > 0);
    let client = chunks(&stack).await;
    let file = write_file(&stack, &client, owner).await.unwrap();
    files.publish(context, &file).await.unwrap();
    assert_eq!(files.load(context, &location).await.unwrap(), Some(file.clone()));
    assert_eq!(read_file(&stack, &client, file).await, vec![31; 32 * 1024]);
    client.shutdown_small_writes().await.unwrap();
}
