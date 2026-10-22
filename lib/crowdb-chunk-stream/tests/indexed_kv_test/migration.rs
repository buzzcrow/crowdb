// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_protocol::chunk_stream::{encode_stream_manifest_record, StreamHeadRecord};
use crowdb_protocol::key::{StreamExtentPageKey, StreamLegacySourceKey, StreamManifestHeadKey};

use super::*;

#[tokio::test]
async fn migration_fences_cached_legacy_publishers_and_cold_loads_the_exact_source() {
    for raw in [true, false] {
        let server = TestMetadataServer::start().await;
        let old = KvStreamMetadataStore::new(server.client.clone(), 1, 1).unwrap();
        let (mut legacy, mut pages) = metadata_server::candidate(3, 7, 17);
        legacy.stream_name = Test::NAME;
        for page in &mut pages {
            page.stream_name = Test::NAME;
        }
        let source = if raw {
            bincode::serialize(&legacy).unwrap()
        } else {
            encode_stream_manifest_record(&legacy).unwrap()
        };
        let head_key = StreamManifestHeadKey {
            stream_name: Test::NAME,
        }
        .to_bytes();
        let mut ops: Vec<_> = pages
            .iter()
            .map(|page| BatchOp::Put {
                key: Bytes::from(
                    StreamExtentPageKey {
                        stream_name: Test::NAME,
                        writer_epoch: 3,
                        generation: 7,
                        page_index: page.page_index,
                    }
                    .to_bytes(),
                ),
                value: Bytes::from(bincode::serialize(page).unwrap()),
            })
            .collect();
        ops.push(BatchOp::Put {
            key: Bytes::copy_from_slice(&head_key),
            value: Bytes::copy_from_slice(&source),
        });
        server.client.batch_write(1, 1, &ops).await.unwrap();
        assert_eq!(old.load_current(Test::NAME).await.unwrap(), Some(legacy.clone()));
        let metadata = KvStreamMetadataStore::new(server.client.clone(), 1, 1).unwrap();
        let migration = metadata
            .begin_directory_migration(Test::NAME, 4, [9; 16])
            .await
            .unwrap();
        assert_eq!(migration.source_bytes().as_ref(), source);
        assert_eq!(migration.source_view().unwrap().fence_count(), 17);
        assert_eq!(migration.marker().legacy.generation, 7);
        assert_eq!(migration.marker().generation, 8);
        assert_eq!(migration.marker().writer_epoch, 4);
        let bytes = get(&server, &head_key).await.unwrap().0;
        assert_eq!(
            crowdb_protocol::chunk_stream::decode_stream_head_record(&bytes).unwrap(),
            StreamHeadRecord::Migrating(migration.marker().clone())
        );
        let (mut stale, mut new_pages) = metadata_server::candidate(3, 8, 17);
        stale.stream_name = Test::NAME;
        for page in &mut new_pages {
            page.stream_name = Test::NAME;
        }
        assert!(old.publish(Some((3, 7)), stale, new_pages).await.is_err());
        let cold = KvStreamMetadataStore::new(server.client.clone(), 1, 1).unwrap();
        let resumed = cold
            .begin_directory_migration(Test::NAME, 4, [9; 16])
            .await
            .unwrap();
        assert_eq!(resumed.marker(), migration.marker());
        assert_eq!(resumed.revision(), migration.revision());
        assert_eq!(resumed.source_bytes(), migration.source_bytes());
        assert!(matches!(
            cold.begin_directory_migration(Test::NAME, 4, [8; 16]).await,
            Err(StreamError::StaleWriter)
        ));
        assert!(matches!(
            cold.begin_directory_migration(Test::NAME, 5, [8; 16]).await,
            Err(StreamError::StaleWriter)
        ));
        for page in pages {
            let key = StreamExtentPageKey {
                stream_name: Test::NAME,
                writer_epoch: 3,
                generation: 7,
                page_index: page.page_index,
            }
            .to_bytes();
            assert_eq!(
                get(&server, &key).await.unwrap().0.as_ref(),
                bincode::serialize(&page).unwrap()
            );
        }
        assert!(
            cold.load_directory_root(Test::NAME, migration.marker().legacy)
                .await
                .unwrap()
                .is_none(),
            "frozen legacy source must not occupy the indexed root key"
        );
    }
}

#[tokio::test]
async fn migration_never_replaces_the_head_without_an_exact_saved_source() {
    let server = TestMetadataServer::start().await;
    let metadata = KvStreamMetadataStore::new(server.client.clone(), 1, 1).unwrap();
    let (legacy, _) = metadata_server::candidate(3, 7, 0);
    metadata.publish(None, legacy.clone(), vec![]).await.unwrap();
    let name = legacy.stream_name;
    assert!(matches!(
        metadata.begin_directory_migration(name, 2, [9; 16]).await,
        Err(StreamError::StaleWriter)
    ));
    assert!(matches!(
        metadata.begin_directory_migration(name, 4, [0; 16]).await,
        Err(StreamError::InvalidRequest(_))
    ));
    let source = encode_stream_manifest_record(&legacy).unwrap();
    let source_key = StreamLegacySourceKey {
        stream_name: name,
        source: crowdb_protocol::chunk_stream::legacy_stream_source_id(&source),
    }
    .to_bytes();
    server
        .client
        .put_cas(1, 1, &source_key, b"conflicting backup", 0)
        .await
        .unwrap();
    assert!(matches!(
        metadata.begin_directory_migration(name, 4, [9; 16]).await,
        Err(StreamError::Corruption(_))
    ));
    assert_eq!(metadata.load_current(name).await.unwrap(), Some(legacy));
    assert!(metadata.load_directory_migration(name).await.unwrap().is_none());
}

#[tokio::test]
async fn missing_or_corrupt_source_does_not_undo_a_durable_migration_fence() {
    let server = TestMetadataServer::start().await;
    let metadata = KvStreamMetadataStore::new(server.client.clone(), 1, 1).unwrap();
    let (legacy, _) = metadata_server::candidate(3, 7, 0);
    metadata.publish(None, legacy.clone(), vec![]).await.unwrap();
    let snapshot = metadata
        .begin_directory_migration(legacy.stream_name, 4, [9; 16])
        .await
        .unwrap();
    let source_key = StreamLegacySourceKey {
        stream_name: legacy.stream_name,
        source: snapshot.marker().source,
    }
    .to_bytes();
    let head_key = StreamManifestHeadKey {
        stream_name: legacy.stream_name,
    }
    .to_bytes();
    let frozen = get(&server, &head_key).await.unwrap();
    server.client.delete(1, 1, &source_key, None).await.unwrap();
    assert!(matches!(
        metadata.load_directory_migration(legacy.stream_name).await,
        Err(StreamError::ReadUnavailable(_))
    ));
    server
        .client
        .put_cas(1, 1, &source_key, b"corrupt", 0)
        .await
        .unwrap();
    assert!(matches!(
        metadata.load_directory_migration(legacy.stream_name).await,
        Err(StreamError::Corruption(_))
    ));
    assert_eq!(get(&server, &head_key).await.unwrap(), frozen);
}
