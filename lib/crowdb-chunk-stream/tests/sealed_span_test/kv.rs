// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::*;
use crowdb_kv_client::{GetOutcome, ReadMode};
use crowdb_protocol::key::{BinaryKey, StreamManifestHeadKey, StreamSealedChunkSpanKey};

#[tokio::test]
async fn kv_span_and_head_use_one_revision_and_invalid_spans_do_not_publish() {
    let server = TestMetadataServer::start().await;
    let metadata = KvStreamMetadataStore::new(server.client.clone(), 1, 1).unwrap();
    let directory = metadata.directory_metadata();
    check_invalid(directory).await;
    let first = directory.load_directory_head(Test::NAME).await.unwrap().unwrap();
    let (mutation, head, span) = successor(directory, &first).await;
    let mutation = mutation.with_sealed_chunk_span(span).unwrap();
    directory
        .publish_directory(Some(&first), head, &mutation)
        .await
        .unwrap();
    let mut revisions = Vec::new();
    for key in [
        StreamManifestHeadKey {
            stream_name: Test::NAME,
        }
        .to_bytes(),
        StreamSealedChunkSpanKey {
            stream_name: Test::NAME,
            chunk_id: span.chunk_id,
        }
        .to_bytes(),
    ] {
        let GetOutcome::Found { revision, .. } = server
            .client
            .get(1, 1, &key, ReadMode::Linearizable, None)
            .await
            .unwrap()
        else {
            panic!("atomic head/span record missing")
        };
        revisions.push(revision);
    }
    assert_eq!(revisions[0], revisions[1]);
    let cold = KvStreamMetadataStore::new(server.client.clone(), 1, 1).unwrap();
    assert_eq!(
        cold.directory_metadata()
            .load_directory_span(Test::NAME, span.chunk_id)
            .await
            .unwrap(),
        Some(span)
    );
    assert_eq!(
        cold.directory_metadata()
            .scan_directory_spans(Test::NAME, None, 1)
            .await
            .unwrap()
            .entries,
        [span.chunk_id]
    );
}
