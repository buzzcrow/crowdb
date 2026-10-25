// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crate::access_iceberg::native::TestNativeStorage;
use crowdb_access_iceberg::{
    catalog::{CatalogContext, RoutedCatalogStore},
    file::{FileRecord, FileRepository, TableLocation},
    key::{FileId, NamespaceId, TableId},
};
use crowdb_chunk_client::{
    ChunkClientConfig, ChunkIoClient, ChunkIoClientConfig, LargeWritePolicy, SmallWritePolicy,
};
use crowdb_chunkdb::{
    routing::{BindingCache, BindingTable},
    storage::{range_retirement::RetiredByteRange, ChunkStore},
};
use crowdb_common::ec::EcScheme;
use crowdb_kv_client::{ChunkSlotMapClient, CrowdbKvClient};
use crowdb_protocol::chunkdb::rpc::{Chunk, ChunkState, ChunkType, Location};
use md5::{Digest, Md5};
use std::sync::Arc;

const MIB: usize = 1024 * 1024;

async fn connect(stack: &TestNativeStorage) -> ChunkIoClient {
    ChunkIoClient::connect(ChunkIoClientConfig {
        management_seeds: stack.seeds.clone(),
        diskio_connections_per_endpoint: 1,
        diskio_rpc_workers: 1,
        small_write: SmallWritePolicy {
            mirror_copies: 2,
            conversion_enabled: false,
            ..SmallWritePolicy::new(ChunkType::IcebergTable)
        },
    })
    .await
    .unwrap()
}

fn large_policy() -> LargeWritePolicy {
    LargeWritePolicy {
        ec_scheme: EcScheme {
            data_num: 4,
            code_num: 1,
        },
        client: Arc::new(ChunkClientConfig {
            chunk_type: ChunkType::IcebergTable,
            large_mirror_copies: Some(2),
            max_chunk_size: 64 * MIB as u64,
            prefetch_strips_per_chunk: 2,
            large_prefetch_max_strips_per_batch: 32,
            large_parallel_strip_writes: 4,
            large_held_buffers: 4,
            parity_depth: 2,
            chunk_preparation_depth: 1,
            large_write_repair_attempts: 3,
            read_buffer_size: MIB,
            max_cached_buffer: 4 * MIB,
            memory_budget: 0,
        }),
    }
}

async fn chunk_store(stack: &TestNativeStorage) -> ChunkStore {
    let kv = Arc::new(CrowdbKvClient::new(crowdb_kv_client::ClientConfig::new(
        stack.seeds.clone(),
    )));
    let bindings = BindingCache::new();
    bindings
        .replace(BindingTable::new(
            ChunkSlotMapClient::new(kv.clone()).read_storage().await.unwrap(),
        ))
        .unwrap();
    ChunkStore::new(kv, bindings)
}

pub(super) struct TestLargeRange {
    pub file: FileRecord,
    chunks: ChunkStore,
    before: Chunk,
    location: Location,
}
impl TestLargeRange {
    pub(super) async fn write(
        stack: &TestNativeStorage,
        store: Arc<RoutedCatalogStore>,
        context: CatalogContext,
        table: TableId,
    ) -> Self {
        let client = connect(stack).await;
        let policy = large_policy();
        let bytes = vec![37_u8; 16 * MIB];
        let written = client
            .prepare_large_write(Some(bytes.len() as u64), policy)
            .write_stream(bytes.as_slice())
            .await
            .unwrap();
        assert_eq!(written.locations.len(), 1);
        let location = written.locations[0].clone();
        assert!(location.length > 8 * MIB as u64);
        assert_eq!(location.offset, 0);
        assert_eq!(location.logical_length, bytes.len() as u64);
        let table = TableLocation {
            catalog: context.catalog,
            table,
            storage_namespace: Some(NamespaceId::from_bytes(table.as_bytes()).unwrap()),
        };
        let file = FileRecord::from_uploaded_locations(
            FileId::random(),
            table.file("data/large.bin").unwrap(),
            &written.locations,
            bytes.len() as u64,
            format!("{:x}", Md5::digest(&bytes)),
        )
        .unwrap();
        assert_eq!(
            FileRepository::new(store).publish(context, &file).await.unwrap(),
            file
        );
        let logical_bytes = bytes.len();
        {
            let verified = client.read_object(&written.locations).await.unwrap();
            let mut offset = 0;
            for part in verified {
                assert_eq!(part.as_ref(), &bytes[offset..offset + part.len()]);
                offset += part.len();
            }
            assert_eq!(offset, bytes.len());
        }
        drop(bytes);
        drop(client);
        let chunks = chunk_store(stack).await;
        let before = chunks.get_chunk(&location.chunk_id.unwrap()).await.unwrap();
        assert_eq!(before.id, location.chunk_id);
        assert_eq!(before.chunk_type, ChunkType::IcebergTable as i32);
        assert_eq!(before.state, ChunkState::Sealed as i32);
        assert_eq!(before.writer_epoch, 0);
        assert_eq!(before.acknowledged_cursor, location.length);
        assert_eq!(u64::from(before.sealed_length), location.length.div_ceil(1024));
        assert!(chunks
            .get_chunk_range_retirement(&location.chunk_id.unwrap())
            .await
            .unwrap()
            .is_none());
        println!(
            "gc_matrix_large_seed {}",
            serde_json::json!({"chunk":before.id,"location":location,"logical_bytes":logical_bytes,"seed_payload_verified":true,"sealed_extent":before.acknowledged_cursor,"writer_epoch":before.writer_epoch,"layout_modify_ts":before.modify_ts})
        );
        Self {
            file,
            chunks,
            before,
            location,
        }
    }
    pub(super) async fn verify(&self) -> Result<serde_json::Value, String> {
        let id = self.location.chunk_id.ok_or("large location lost its chunk")?;
        let chunk = self
            .chunks
            .get_chunk(&id)
            .await
            .map_err(|error| error.to_string())?;
        if chunk != self.before {
            return Err("range retirement changed the sealed chunk layout".into());
        }
        let record = self
            .chunks
            .get_chunk_range_retirement(&id)
            .await
            .map_err(|error| error.to_string())?
            .ok_or("large range retirement is missing")?;
        if record.ranges()
            != [RetiredByteRange {
                start: self.location.offset,
                end: self.location.offset + self.location.length,
            }]
        {
            return Err(format!(
                "large range retirement is not the exact sealed extent: {record:?}"
            ));
        }
        if record.revision > chunk.modify_ts {
            return Err("range retirement revision exceeds its sealed layout".into());
        }
        let value = serde_json::to_value(&record).map_err(|error| error.to_string())?;
        for (field, expected) in [
            ("version", serde_json::json!(2)),
            ("chunk_id", serde_json::json!(chunk.id)),
            ("writer_epoch", serde_json::json!(chunk.writer_epoch)),
            ("create_ts_ms", serde_json::json!(chunk.create_ts_ms)),
            ("sealed_ts_ms", serde_json::json!(chunk.sealed_ts_ms)),
            (
                "sealed_bytes",
                serde_json::json!(u64::from(chunk.sealed_length) * 1024),
            ),
        ] {
            if value[field] != expected {
                return Err(format!("range retirement changed {field}: {value}"));
            }
        }
        Ok(
            serde_json::json!({"retirement":value,"location":self.location,"scope":"exact accepted sealed range and unchanged layout; seed payload read verified; GC read/copy IO and physical free not measured"}),
        )
    }
}
