// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{part, repository, session, CompletionPart, MetadataKey, TenantId};
use std::sync::Arc;
use std::time::Duration;

#[tokio::test]
async fn concurrent_distinct_parts_publish_without_reuploading() {
    let (repository, _, _) = repository().await;
    let repository = Arc::new(repository);
    let session = session();
    repository.begin(&session).await.unwrap();
    let mut tasks = Vec::new();
    for number in 1..=10 {
        let repository = Arc::clone(&repository);
        let session = session.clone();
        tasks.push(tokio::spawn(async move {
            let mut value = part();
            value.number = number;
            let saved = repository.put_stream_part(&session, &value, 110).await.unwrap();
            assert!(
                saved.is_some(),
                "part {number} was rejected by session contention"
            );
            assert_eq!(repository.part(&session, number).await.unwrap(), saved);
        }));
    }
    tokio::time::timeout(Duration::from_secs(5), async {
        for task in tasks {
            task.await.unwrap();
        }
    })
    .await
    .unwrap();
    let current = repository.load(&session).await.unwrap().unwrap();
    assert_eq!(current.part_count, 10);
    assert_eq!(current.staged_bytes, 50);
    assert!(current.pending.is_none());
}

#[tokio::test]
async fn orphan_generation_does_not_poison_retry_or_changed_part() {
    for identical in [true, false] {
        let (repository, _, store) = repository().await;
        let session = session();
        repository.begin(&session).await.unwrap();
        let mut orphan = part();
        orphan.modified_ms = 109;
        let key = MetadataKey::multipart_part_generation(
            &TenantId::new(b"tenant".to_vec()).unwrap(),
            session.bucket_id,
            &session.upload_id,
            orphan.number,
            orphan.revision,
        )
        .unwrap();
        store.put_if_absent(key, orphan.encode().unwrap()).await.unwrap();
        let mut incoming = part();
        incoming.locations[0].offset += 39;
        if !identical {
            incoming.raw_md5 = [8; 16];
        }
        let saved = repository
            .put_stream_part(&session, &incoming, 110)
            .await
            .unwrap()
            .expect("an orphan cannot permanently block publication");
        assert_eq!(saved.revision, if identical { 1 } else { 2 });
        assert_eq!(
            saved.locations,
            if identical {
                orphan.locations.clone()
            } else {
                incoming.locations
            }
        );
        assert_eq!(
            repository.part_generation(&session, 1, 1).await.unwrap(),
            Some(orphan)
        );
        assert_eq!(repository.part(&session, 1).await.unwrap(), Some(saved));
        let current = repository.load(&session).await.unwrap().unwrap();
        assert_eq!(current.part_count, 1);
        assert_eq!(current.staged_bytes, 5);
        assert!(current.pending.is_none());
        let frozen = repository
            .freeze_completion(
                &session,
                &[CompletionPart {
                    number: 1,
                    etag: format!("\"{}\"", if identical { "09" } else { "08" }.repeat(16)),
                }],
                120,
            )
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            frozen.selection.as_ref().unwrap()[0].revision,
            if identical { 1 } else { 2 }
        );
    }
}
