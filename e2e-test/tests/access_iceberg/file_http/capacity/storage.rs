// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::*;
pub(super) async fn seed_catalog(
    stack: &TestNativeStorage,
) -> crowdb_access_iceberg::catalog::CatalogContext {
    let repository = CatalogRepository::new(stack.store().await, ClearBounds::default()).unwrap();
    let now = now_ms();
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

pub(super) async fn chunks(stack: &TestNativeStorage) -> ChunkIoClient {
    ChunkIoClient::connect(ChunkIoClientConfig {
        management_seeds: stack.seeds.clone(),
        diskio_connections_per_endpoint: 1,
        diskio_rpc_workers: 1,
        small_write: SmallWritePolicy {
            object_limit: 1024 * 1024,
            min_pipelines: 1,
            max_pipelines: 1,
            memory_budget: 8 * 1024 * 1024,
            chunk_capacity: 1024 * 1024 * 1024,
            mirror_copies: 2,
            conversion_enabled: false,
            ..SmallWritePolicy::new(crowdb_protocol::chunkdb::rpc::ChunkType::IcebergTable)
        },
    })
    .await
    .unwrap()
}

pub(super) async fn fill_disk(client: &DiskdbClient, disk_group_id: u64) -> Vec<Segment> {
    let mut held = Vec::new();
    let mut sequence = 1_u64;
    loop {
        let mut allocated = None;
        for units in [1024, 128, 1] {
            match client
                .allocate_blocks(AllocateBlocksRequest {
                    disk_group_id,
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
        sequence = sequence
            .checked_add(1)
            .expect("disk fill allocation sequence overflowed");
    }
    assert!(!held.is_empty());
    assert_eq!(
        client.query_disk_group(disk_group_id).await.unwrap().disk_groups[0].free_bytes,
        0
    );
    held
}

pub(super) async fn write_file(
    stack: &TestNativeStorage,
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

pub(super) async fn read_file(
    stack: &TestNativeStorage,
    client: &ChunkIoClient,
    file: FileRecord,
) -> Vec<u8> {
    let blocks = Arc::new(NativeFileBlocks::new(client.clone(), stack.store().await));
    let mut reader = FileReader::new(blocks, file, None, 4096).unwrap();
    let mut bytes = Vec::new();
    while let Some(frame) = reader.next().await.unwrap() {
        bytes.extend_from_slice(&frame);
    }
    bytes
}

pub(super) async fn restore_capacity(disk: &DiskdbClient, held: &[Segment]) {
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
    let mut compacted_disks = Vec::new();
    for segment in held {
        if compacted_disks.contains(&segment.disk_id) {
            continue;
        }
        let compacted = disk
            .compact_zone(CompactZoneRequest {
                disk_id: segment.disk_id,
                zone_indices: Vec::new(),
            })
            .await
            .unwrap();
        assert!(compacted.zones.iter().all(|zone| zone.success));
        compacted_disks.push(segment.disk_id);
    }
    for group in 1000..1006 {
        assert!(disk.query_disk_group(group).await.unwrap().disk_groups[0].free_bytes > 0);
    }
}
