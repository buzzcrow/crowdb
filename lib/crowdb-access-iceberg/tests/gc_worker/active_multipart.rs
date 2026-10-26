// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::sync::{atomic::Ordering, Arc};

use crowdb_access_iceberg::{
    catalog::CatalogStore,
    file::{FileBlockStore, FileIdentity, FileTreeWriter, MultipartPart, MultipartPhase},
    gc::{CandidatePhase, GcCandidate, GcLimits, GcPhase, GcRepository, GcTask, GcWorker},
    key::{FileId, IcebergKey},
    operation::mutation_identity,
    record::StorageRecord,
};

use super::{fixture, multipart_fixtures, published_file_exists};

#[tokio::test]
async fn active_multipart_cleanup_reclaims_expired_part_without_touching_published_file() {
    let (fixture, blocks, _, limits, published) = fixture(false).await;
    let published_blocks = blocks.blocks.values.load_full();
    let mut session = multipart_fixtures::session();
    session.context = fixture.context;
    session.owner.table = fixture.table;
    session.location = fixture.table.file("multipart/object.parquet").unwrap();
    session.phase = MultipartPhase::Aborted;
    session.part_count = 1;
    session.staged_bytes = 20;
    let owner = FileIdentity {
        table: fixture.table,
        file: FileId::random(),
    };
    let mut writer = FileTreeWriter::new(blocks.clone(), owner, 128).unwrap();
    writer.push(&[7; 20]).await.unwrap();
    let part = MultipartPart {
        upload: session.upload,
        number: 1,
        revision: 1,
        modified_ms: 100,
        owner,
        tree: Some(writer.finish().await.unwrap()),
        stream: None,
    };
    let session_key = session.key().encode().unwrap();
    let part_key = part.key().encode().unwrap();
    for (key, record) in [
        (session.key(), StorageRecord::MultipartSession(Box::new(session))),
        (part.key(), StorageRecord::MultipartPart(Box::new(part.clone()))),
    ] {
        let key = key.encode().unwrap();
        let bytes = record.encode().unwrap();
        fixture
            .store
            .compare_exchange(&key, None, &bytes, mutation_identity(&key, None, &bytes))
            .await
            .unwrap();
    }
    let repository = GcRepository::new(fixture.store.clone());
    let mut task = repository
        .admit_multipart(fixture.context, 2000, limits)
        .await
        .unwrap();
    let discovered_ms = 2_000 + 2 * GcTask::PURGE_DELAY_MS;
    task = finish_at(&repository, blocks.clone(), limits, task, discovered_ms).await;
    assert_eq!(task.phase, GcPhase::Complete, "{task:?}");
    assert_eq!(task.deleted, 0);
    assert_eq!(blocks.deletes.load(Ordering::Relaxed), 0);
    assert!(fixture.store.get(&part_key).await.unwrap().is_some());
    assert!(fixture.store.get(&session_key).await.unwrap().is_some());
    assert!(published_file_exists(&fixture, published).await);
    let candidate_key = GcCandidate::deleted_key(&GcCandidate::part_file(&part).unwrap());
    let candidate = load_candidate(fixture.store.as_ref(), &candidate_key).await;
    assert_eq!(candidate.phase, CandidatePhase::Retained);
    assert_eq!(candidate.first_seen_ms, discovered_ms);
    assert_eq!(
        candidate.not_before_ms,
        candidate.first_seen_ms + GcTask::PURGE_DELAY_MS
    );
    let previous = task.clone();
    let next_period_ms = candidate.first_seen_ms + 30 * 60_000;
    task = repository
        .admit_multipart(fixture.context, next_period_ms, limits)
        .await
        .unwrap();
    assert_eq!(task.phase, GcPhase::Discover);
    assert_eq!(task.identity, previous.identity);
    assert_eq!(task.created_ms, previous.created_ms);
    assert_eq!(task.not_before_ms, previous.not_before_ms);
    assert_eq!(task.sweep_round, previous.sweep_round);
    task = finish_at(&repository, blocks.clone(), limits, task, next_period_ms).await;
    assert_eq!(task.phase, GcPhase::Complete, "{task:?}");
    assert_eq!(task.deleted, 1);
    assert_eq!(task.reclaimed_bytes, part.tree.as_ref().unwrap().length);
    assert_eq!(task.sweep_round, previous.sweep_round + 1);
    let completed = load_candidate(fixture.store.as_ref(), &candidate_key).await;
    assert_eq!(completed.phase, CandidatePhase::Complete);
    assert_eq!(completed.first_seen_ms, candidate.first_seen_ms);
    assert_eq!(completed.not_before_ms, candidate.not_before_ms);
    assert_eq!(completed.completed_round, task.sweep_round);
    assert_eq!(*blocks.blocks.values.load_full(), *published_blocks);
    assert!(fixture.store.get(&part_key).await.unwrap().is_none());
    assert!(fixture.store.get(&session_key).await.unwrap().is_none());
    assert!(published_file_exists(&fixture, published).await);
}

async fn load_candidate(store: &dyn CatalogStore, key: &IcebergKey) -> GcCandidate {
    let value = store.get(&key.encode().unwrap()).await.unwrap().unwrap();
    let StorageRecord::GcCandidate(candidate) = StorageRecord::decode(key, &value.bytes).unwrap() else {
        panic!("candidate")
    };
    *candidate
}

async fn finish_at(
    repository: &GcRepository,
    blocks: Arc<dyn FileBlockStore>,
    limits: GcLimits,
    mut task: GcTask,
    now_ms: u64,
) -> GcTask {
    for _ in 0..200 {
        task = GcWorker::new(repository.clone(), blocks.clone(), limits)
            .unwrap()
            .step(&task, now_ms)
            .await
            .unwrap();
        if task.phase == GcPhase::Complete {
            return task;
        }
    }
    panic!("incomplete GC: {task:?}");
}
