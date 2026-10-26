// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[path = "common/rdma_chunk_kv.rs"]
mod fixture;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "run through tools/rdma/run-chunk-kv.py with explicit transport and topology"]
async fn real_chunk_kv_service_preserves_large_controls_and_typed_results() {
    crowdb_rpc_ffi::init_test_logging();
    let profile = fixture::TestChunkKvProfile::load();
    match std::env::var("CROWDB_RDMA_CHUNK_KV_ROLE").unwrap().as_str() {
        "server" => profile.server().await,
        "client" => profile.client().await,
        other => panic!("invalid Chunk-KV test role: {other}"),
    }
}
