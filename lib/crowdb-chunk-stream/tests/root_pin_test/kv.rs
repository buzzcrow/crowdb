// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::*;

async fn record(server: &TestMetadataServer, key: &[u8]) -> (bytes::Bytes, u64) {
    let GetOutcome::Found { value, revision } = server
        .client
        .get(1, 1, key, ReadMode::Linearizable, None)
        .await
        .unwrap()
    else {
        panic!("expected record")
    };
    (value, revision)
}

#[tokio::test]
async fn kv_pins_use_the_same_revision_and_lease_contract() {
    let server = TestMetadataServer::start().await;
    let metadata = KvStreamMetadataStore::new(server.client.clone(), 1, 1).unwrap();
    check_lifecycle(metadata.directory_metadata()).await;
}

#[tokio::test]
async fn late_pins_fail_after_root_retirement_but_existing_exact_views_survive() {
    let server = TestMetadataServer::start().await;
    let metadata = KvStreamMetadataStore::new(server.client.clone(), 1, 1).unwrap();
    let directory = metadata.directory_metadata();
    let head = publish(directory).await;
    let original = directory.pin_directory_root(pin(&head, 1, None)).await.unwrap();
    let control_key = StreamDirectoryControlKey {
        stream_name: Test::NAME,
    }
    .to_bytes();
    let (control, before_revision) = record(&server, &control_key).await;
    let reader = directory
        .pin_directory_root(pin(&head, 2, Some(now_ms() + 10_000)))
        .await
        .unwrap();
    let root_key = StreamDirectoryRootKey {
        stream_name: Test::NAME,
        identity: head.identity(),
    }
    .to_bytes();
    let retirement = [
        BatchOp::Put {
            key: control_key.clone().into(),
            value: control.clone(),
        },
        BatchOp::Delete {
            key: root_key.clone().into(),
        },
    ];
    assert!(server
        .client
        .batch_write_cas(1, 1, &retirement, &control_key, before_revision)
        .await
        .is_err());
    let mutation = DirectoryIndex::new(Test::NAME, head.purpose, head.root)
        .unwrap()
        .prepare_append(directory, &Test::locations(1, 1))
        .await
        .unwrap();
    let previous = directory.load_directory_head(Test::NAME).await.unwrap().unwrap();
    directory
        .publish_directory(
            Some(&previous),
            Test::head(&mutation, Some(previous.head())),
            &mutation,
        )
        .await
        .unwrap();
    let (_, current_revision) = record(&server, &control_key).await;
    server
        .client
        .batch_write_cas(1, 1, &retirement, &control_key, current_revision)
        .await
        .unwrap();
    assert!(directory
        .load_directory_root(Test::NAME, head.identity())
        .await
        .unwrap()
        .is_none());
    assert!(matches!(
        directory.pin_directory_root(pin(&head, 3, None)).await,
        Err(StreamError::ReadUnavailable(_))
    ));
    assert_eq!(
        directory
            .pin_directory_root(original.pin().clone())
            .await
            .unwrap()
            .pin(),
        original.pin()
    );
    let renewed = directory
        .renew_directory_pin(&reader, now_ms() + 20_000)
        .await
        .unwrap();
    assert_eq!(renewed.pin().head, head);
    assert_eq!(renewed.pin().captured_tail, 1);
    directory.release_directory_pin(&original).await.unwrap();
    directory.release_directory_pin(&renewed).await.unwrap();
}

#[tokio::test]
async fn an_expired_persisted_pin_cannot_be_renewed_or_recreated() {
    let server = TestMetadataServer::start().await;
    let metadata = KvStreamMetadataStore::new(server.client.clone(), 1, 1).unwrap();
    let directory = metadata.directory_metadata();
    let head = publish(directory).await;
    let live = directory
        .pin_directory_root(pin(&head, 1, Some(now_ms() + 10_000)))
        .await
        .unwrap();
    let mut expired = live.pin().clone();
    expired.expires_at_ms = Some(1);
    let control_key = StreamDirectoryControlKey {
        stream_name: Test::NAME,
    }
    .to_bytes();
    let (control, revision) = record(&server, &control_key).await;
    let pin_key = StreamRootPinKey {
        stream_name: Test::NAME,
        pin_id: expired.id,
    }
    .to_bytes();
    server
        .client
        .batch_write_cas(
            1,
            1,
            &[
                BatchOp::Put {
                    key: control_key.clone().into(),
                    value: control,
                },
                BatchOp::Put {
                    key: pin_key.into(),
                    value: encode_stream_root_pin(&expired).unwrap().into(),
                },
            ],
            &control_key,
            revision,
        )
        .await
        .unwrap();
    let old = directory
        .load_directory_pin(Test::NAME, expired.id)
        .await
        .unwrap()
        .unwrap();
    let (_, before_revision) = record(&server, &control_key).await;
    assert!(matches!(
        directory.renew_directory_pin(&old, now_ms() + 20_000).await,
        Err(StreamError::ReadUnavailable(_))
    ));
    assert!(directory.pin_directory_root(expired.clone()).await.is_err());
    assert_eq!(record(&server, &control_key).await.1, before_revision);
    assert_eq!(
        directory
            .load_directory_pin(Test::NAME, expired.id)
            .await
            .unwrap()
            .unwrap()
            .pin(),
        &expired
    );
}

#[tokio::test]
async fn lifecycle_operations_cannot_recreate_missing_retention_control() {
    let server = TestMetadataServer::start().await;
    let metadata = KvStreamMetadataStore::new(server.client.clone(), 1, 1).unwrap();
    let directory = metadata.directory_metadata();
    let head = publish(directory).await;
    let live = directory
        .pin_directory_root(pin(&head, 1, Some(now_ms() + 10_000)))
        .await
        .unwrap();
    let control_key = StreamDirectoryControlKey {
        stream_name: Test::NAME,
    }
    .to_bytes();
    let (_, revision) = record(&server, &control_key).await;
    server
        .client
        .batch_write_cas(
            1,
            1,
            &[BatchOp::Delete {
                key: control_key.clone().into(),
            }],
            &control_key,
            revision,
        )
        .await
        .unwrap();
    assert!(matches!(
        directory.renew_directory_pin(&live, now_ms() + 20_000).await,
        Err(StreamError::ReadUnavailable(_))
    ));
    assert!(matches!(
        directory.release_directory_pin(&live).await,
        Err(StreamError::ReadUnavailable(_))
    ));
    assert!(matches!(
        server
            .client
            .get(1, 1, &control_key, ReadMode::Linearizable, None)
            .await
            .unwrap(),
        GetOutcome::NotFound
    ));
    assert_eq!(
        directory
            .load_directory_pin(Test::NAME, live.pin().id)
            .await
            .unwrap()
            .unwrap()
            .pin(),
        live.pin()
    );
}
