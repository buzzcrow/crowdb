// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::sync::Arc;

use bytes::Bytes;
use crowdb_access_iceberg::{
    catalog::{CatalogContext, RoutedCatalogStore},
    file::{
        ChunkDirectory, ChunkRoot, ContentFormat, FileBlockStore, FileContent, FileIdentity, FileKind,
        FileReader, FileRecord, FileRepository, FileTreeWriter, NativeFileBlocks, TableLocation,
    },
    key::{FileId, NamespaceId, TableId},
};
use crowdb_chunk_client::{ChunkIoClient, ChunkIoClientConfig, ChunkIoWriter, SmallWritePolicy};
use crowdb_protocol::{
    chunkdb::rpc::{ChunkType, Location},
    common::ChunkId,
};
use md5::{Digest, Md5};

use crate::access_iceberg::native::TestNativeStorage;

pub(super) struct TestPayloads {
    pub client: ChunkIoClient,
    pub dead: [FileRecord; 2],
    pub live: FileRecord,
    pub tree_roots: Vec<ChunkRoot>,
    pub chunk: ChunkId,
    pub dead_ranges: Vec<(u64, u64)>,
    pub live_range: (u64, u64),
}

pub(super) async fn write_payloads(
    stack: &TestNativeStorage,
    store: Arc<RoutedCatalogStore>,
    context: CatalogContext,
    dead_table: TableId,
    live_table: TableId,
) -> TestPayloads {
    let client = connect(stack).await;
    let owner = FileIdentity {
        table: table(context, dead_table),
        file: FileId::random(),
    };
    let blocks = Arc::new(NativeFileBlocks::new(client.clone(), store.clone()));
    let external = locations(&client, owner.table, "external.bin", 17).await;
    let mut writer = FileTreeWriter::new(blocks.clone(), owner, 16 * 1024).unwrap();
    writer.push(&vec![31; 16 * 1024]).await.unwrap();
    // A live owner is placed between the two dead leaves in the same pipeline.
    let live = locations(&client, table(context, live_table), "live.bin", 61).await;
    writer.push(&vec![31; 16 * 1024]).await.unwrap();
    let tree = writer.finish().await.unwrap();
    let tree_file = FileRecord {
        file: owner.file,
        location: owner.table.file("data/tree.bin").unwrap(),
        kind: FileKind::Unbound,
        format: ContentFormat::Opaque,
        length: tree.length,
        digest: tree.digest,
        content: FileContent::Chunks { root: tree.root },
        hint: None,
    };
    let roots = tree_roots(blocks.as_ref(), &tree_file).await;
    let files = FileRepository::new(store);
    for file in [&external, &tree_file, &live] {
        assert_eq!(files.publish(context, file).await.unwrap(), *file);
    }
    client.shutdown_small_writes().await.unwrap();
    let external_location = one_location(&external);
    let live_location = one_location(&live);
    let chunk = external_location.chunk_id.unwrap();
    assert_eq!(live_location.chunk_id, Some(chunk));
    assert!(roots.iter().all(|root| root.chunk == chunk));
    assert_eq!(roots.len(), 3, "two leaves and one directory must be real blocks");
    let live_range = extent(&live_location);
    let mut dead_ranges = vec![extent(&external_location)];
    dead_ranges.extend(
        roots
            .iter()
            .map(|root| (root.offset, root.offset + root.physical_length)),
    );
    assert!(dead_ranges
        .iter()
        .all(|(a, b)| *b <= live_range.0 || live_range.1 <= *a));
    assert!(roots
        .iter()
        .any(|root| root.height == 0 && root.offset < live_range.0));
    assert!(roots
        .iter()
        .any(|root| root.height == 0 && root.offset >= live_range.1));
    TestPayloads {
        client,
        dead: [external, tree_file],
        live,
        tree_roots: roots,
        chunk,
        dead_ranges,
        live_range,
    }
}

async fn connect(stack: &TestNativeStorage) -> ChunkIoClient {
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
            ..SmallWritePolicy::new(ChunkType::IcebergTable)
        },
    })
    .await
    .unwrap()
}

fn table(context: CatalogContext, table: TableId) -> TableLocation {
    TableLocation {
        catalog: context.catalog,
        table,
        storage_namespace: Some(NamespaceId::from_bytes(table.as_bytes()).unwrap()),
    }
}

async fn locations(client: &ChunkIoClient, table: TableLocation, name: &str, value: u8) -> FileRecord {
    let bytes = Bytes::from(vec![value; 512]);
    let mut writer = client
        .prepare_small_write_for_key(bytes.len(), b"gc-notification-corpus")
        .await
        .unwrap();
    writer.on_data(bytes.clone()).await.unwrap();
    let locations = writer.finish_durable().await.unwrap();
    FileRecord::from_uploaded_locations(
        FileId::random(),
        table.file(&format!("data/{name}")).unwrap(),
        &locations,
        bytes.len() as u64,
        format!("{:x}", Md5::digest(&bytes)),
    )
    .unwrap()
}

async fn tree_roots(blocks: &NativeFileBlocks, file: &FileRecord) -> Vec<ChunkRoot> {
    let FileContent::Chunks { root: Some(root) } = &file.content else {
        panic!("tree root missing")
    };
    assert_eq!(root.height, 1);
    let bytes = blocks.read(root).await.unwrap();
    let owner = FileIdentity {
        table: file.location.table(),
        file: file.file,
    };
    let directory = ChunkDirectory::decode(&bytes, owner, 1, file.length).unwrap();
    assert_eq!(directory.entries.len(), 2);
    let mut roots = vec![root.clone()];
    roots.extend(directory.entries.into_iter().map(|entry| entry.root));
    roots
}

fn one_location(file: &FileRecord) -> Location {
    let locations = file.content.locations(file.length).unwrap().unwrap();
    assert_eq!(locations.len(), 1);
    locations.into_iter().next().unwrap()
}

fn extent(location: &Location) -> (u64, u64) {
    (
        location.offset,
        location.offset.checked_add(location.length).unwrap(),
    )
}

pub(super) async fn assert_live(
    store: Arc<RoutedCatalogStore>,
    context: CatalogContext,
    data: &TestPayloads,
) {
    assert_eq!(
        FileRepository::new(store.clone())
            .load(context, &data.live.location)
            .await
            .unwrap(),
        Some(data.live.clone())
    );
    let blocks = Arc::new(NativeFileBlocks::new(data.client.clone(), store));
    let mut reader = FileReader::new(blocks, data.live.clone(), None, 4096).unwrap();
    let mut bytes = Vec::new();
    while let Some(part) = reader.next().await.unwrap() {
        bytes.extend_from_slice(&part);
    }
    assert_eq!(bytes, vec![61; 512]);
}
