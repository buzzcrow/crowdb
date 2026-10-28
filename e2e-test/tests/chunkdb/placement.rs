// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_chunkdb_client::{ChunkdbClient, ChunkdbRpcTransport};
use crowdb_paxos_kv_client::ServiceRegistryClient;
use crowdb_protocol::chunkdb::rpc::{AllocateChunkRequest, ChunkType, DeleteChunkRequest, Strip, StripType};
use std::sync::Arc;

#[tokio::test]
#[ignore = "requires the owned protected Chunkdb container fixture"]
async fn mirror_and_ec_matrix_publish_protected_geometry() {
    let client = ChunkdbClient::new(
        ServiceRegistryClient::new(crate::common::hardware::kv_client()),
        Arc::new(ChunkdbRpcTransport::new()),
    );
    for (kind, copies, data, parity) in [
        (StripType::Mirror, 2, 0, 0),
        (StripType::Mirror, 3, 0, 0),
        (StripType::Ec, 0, 2, 1),
        (StripType::Ec, 0, 4, 2),
        (StripType::Ec, 0, 8, 4),
    ] {
        let chunk = client
            .allocate_chunk(AllocateChunkRequest {
                write_granularity: 1,
                strip_count: 1,
                strip_type: kind as i32,
                copy_count: copies,
                data_num: data,
                code_num: parity,
                chunk_type: ChunkType::S3 as i32,
                ..Default::default()
            })
            .await
            .unwrap()
            .chunk
            .unwrap();
        assert_eq!(chunk.strips.len(), 1);
        let strip = &chunk.strips[0];
        match strip.strip.as_ref().unwrap() {
            Strip::MirrorStrip(mirror) => assert_eq!(mirror.segments.len(), copies as usize),
            Strip::EcStrip(ec) => {
                assert_eq!(ec.data_num, data);
                assert_eq!(ec.code_num, parity);
                assert_eq!(ec.segments.len(), (data + parity) as usize);
            }
        }
        let safety = strip.placement_assessment.as_ref().unwrap();
        assert!(safety.rack_protected && safety.node_protected && safety.disk_protected);
        assert!(!strip.placement_repair_required);
        let loss_budget = if copies > 0 { copies - 1 } else { parity };
        assert_eq!(safety.loss_budget, loss_budget);
        assert!(safety.max_fragments_per_rack <= loss_budget);
        assert!(safety.max_fragments_per_node <= loss_budget);
        assert!(safety.max_fragments_per_disk <= loss_budget);
        client
            .delete_chunk(DeleteChunkRequest { chunk_id: chunk.id })
            .await
            .unwrap();
    }
}
