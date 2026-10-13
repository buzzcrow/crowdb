// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::sync::Arc;

use super::super::native::TestNativeStorage;
use super::super::now_ms;
mod workspace;
use crowdb_access_iceberg::{
    catalog::{CatalogContext, CatalogRepository, CatalogStore, ClearBounds, ManagementPrivilege},
    file::{
        location_key, ContentFormat, FileContent, FileIdentity, FileKind, FileLocation, FileReader,
        FileRecord, FileRepository, FileTreeWriter, NativeFileBlocks, TableLocation,
    },
    gc::{GcLimits, GcPhase, GcRepository, GcStalledReason, GcTask, GcWorker},
    key::{FileId, OperationId, TableId},
    operation::{mutation_identity, ManagementAction, ManagementRequest, RequestIdentity},
    record::StorageRecord,
    table::{head_key, TableHead, TableLifecycle, TablePurgeTask},
};
use crowdb_chunk_client::{ChunkIoClient, ChunkIoClientConfig, SmallWritePolicy};
use crowdb_diskdb_client::{DiskdbClient, DiskdbClientError, DiskdbRpcTransport};
use crowdb_protocol::{
    common::ChunkId,
    diskdb::rpc::{
        AllocateBlocksRequest, CommitBlocksRequest, CompactZoneRequest, FreeBlocksRequest, Segment,
    },
};
use sha2::{Digest, Sha256};
use workspace as gc_capacity;

mod gc;
mod storage;
use gc::{assert_gc_workspace_recovered, assert_gc_workspace_stall, seed_gc_workspace_task};
use storage::{chunks, fill_disk, read_file, seed_catalog, write_file};
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires the owned protected storage fixture"]
async fn full_simulated_disk_preserves_file_authority_then_recovers_after_compaction() {
    let stack = TestNativeStorage::new();
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
    let (workspace, gc_repository, gc_task, gc_file, gc_limits) =
        seed_gc_workspace_task(&stack, context).await;
    client.shutdown_small_writes().await.unwrap();
    drop(client);
    let disk = DiskdbClient::new(
        crowdb_kv_client::ServiceRegistryClient::new(crowdb_kv_client::CrowdbKvClient::new(
            crowdb_kv_client::ClientConfig::new(stack.seeds.clone()),
        )),
        Arc::new(DiskdbRpcTransport::new()),
    );
    disk.refresh_endpoints().await.unwrap();
    let mut held = Vec::new();
    for group in 1000..1006 {
        held.extend(fill_disk(&disk, group).await);
    }
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
    let stalled = assert_gc_workspace_stall(
        &stack,
        &client,
        &workspace,
        &gc_repository,
        gc_task,
        &gc_file,
        gc_limits,
    )
    .await;
    drop(client);
    storage::restore_capacity(&disk, &held).await;
    let client = chunks(&stack).await;
    let file = write_file(&stack, &client, owner).await.unwrap();
    files.publish(context, &file).await.unwrap();
    assert_eq!(files.load(context, &location).await.unwrap(), Some(file.clone()));
    assert_eq!(read_file(&stack, &client, file).await, vec![31; 32 * 1024]);
    assert_gc_workspace_recovered(
        &stack,
        &client,
        &workspace,
        gc_repository,
        stalled,
        &gc_file,
        gc_limits,
    )
    .await;
    assert_eq!(
        files.load(context, &committed.location).await.unwrap(),
        Some(committed.clone())
    );
    assert_eq!(read_file(&stack, &client, committed).await, vec![31; 32 * 1024]);
    client.shutdown_small_writes().await.unwrap();
}
