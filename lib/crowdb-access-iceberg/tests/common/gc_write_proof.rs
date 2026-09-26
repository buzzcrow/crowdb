use super::*;
use crowdb_access_iceberg::file::{FileBlockStore, FileIdentity, FileWriteIntent};

#[tokio::test]
async fn a_changed_unfenced_head_terminates_the_old_live_proof_without_deleting_files() {
    let (store, mut task, files) = fixture(3, 0).await;
    let repository = GcRepository::new(store.clone());
    let worker = GcWorker::new(
        repository,
        Arc::new(blocks::TestBlocks::default()),
        GcLimits {
            minimum_retention_ms: 1,
            ..GcLimits::default()
        },
    )
    .unwrap();
    for _ in 0..20 {
        task = worker.step(&task, 1000).await.unwrap();
        if task.phase == GcPhase::Roots {
            break;
        }
    }
    assert_eq!(task.phase, GcPhase::Roots);
    let mut head = task.head.clone().unwrap();
    let key = head_key(head.catalog, head.table);
    let encoded = key.encode().unwrap();
    let before = store.get(&encoded).await.unwrap().unwrap();
    head.operation_fence += 2;
    let bytes = StorageRecord::TableHead(Box::new(head)).encode().unwrap();
    store
        .compare_exchange(
            &encoded,
            Some(&before.bytes),
            &bytes,
            mutation_identity(&encoded, Some(&before.bytes), &bytes),
        )
        .await
        .unwrap();
    task = worker.run(&task, 1001).await.unwrap();
    assert_eq!(task.phase, GcPhase::Complete);
    assert_eq!(
        task.stalled,
        crowdb_access_iceberg::gc::GcStalledReason::ChangedAuthority
    );
    assert_eq!(task.deleted, 0);
    for file in files {
        assert!(store
            .get(
                &crowdb_access_iceberg::file::file_key(task.context.catalog, file)
                    .encode()
                    .unwrap()
            )
            .await
            .unwrap()
            .is_some());
    }
}

#[tokio::test]
async fn live_proof_keeps_write_intents_for_reachable_owners_and_fences_orphans() {
    let (store, mut task, _) = fixture(3, 0).await;
    let head = task.head.as_ref().unwrap();
    let blocks = Arc::new(blocks::TestBlocks::default());
    let mut intents = Vec::new();
    for file in [head.metadata_file, FileId::random()] {
        let owner = FileIdentity {
            table: head.metadata_location.table(),
            file,
        };
        let root = blocks.put(owner, 0, b"unpublished block").await.unwrap();
        let intent = FileWriteIntent {
            identity: OperationId::random(),
            owner,
            root,
            created_ms: 1,
            not_before_ms: 0,
            deleting: false,
        };
        intent.register(store.clone()).await.unwrap();
        intents.push(intent);
    }
    let repository = GcRepository::new(store.clone());
    let limits = GcLimits {
        minimum_retention_ms: 1,
        ..GcLimits::default()
    };
    for pass in 0..2 {
        let worker = GcWorker::new(repository.clone(), blocks.clone(), limits).unwrap();
        for _ in 0..150 {
            task = worker.step(&task, 1000 + pass * 100).await.unwrap();
            if task.phase == GcPhase::Complete {
                break;
            }
        }
        assert_eq!(task.phase, GcPhase::Complete);
        if pass == 0 {
            let head = task.head.as_ref().unwrap();
            let key = head_key(head.catalog, head.table);
            let value = store.get(&key.encode().unwrap()).await.unwrap().unwrap();
            let StorageRecord::TableHead(head) = StorageRecord::decode(&key, &value.bytes).unwrap() else {
                panic!()
            };
            task = GcTask::plan(task.context, OperationId::random(), Some(*head), 1001, limits).unwrap();
            let key = task.key().encode().unwrap();
            let bytes = StorageRecord::GcTask(Box::new(task.clone())).encode().unwrap();
            store
                .compare_exchange(&key, None, &bytes, mutation_identity(&key, None, &bytes))
                .await
                .unwrap();
        }
    }
    assert!(store
        .get(&intents[0].key().encode().unwrap())
        .await
        .unwrap()
        .is_some());
    assert!(store
        .get(&FileWriteIntent::fence_key(intents[0].owner).encode().unwrap())
        .await
        .unwrap()
        .is_none());
    assert!(store
        .get(&FileWriteIntent::fence_key(intents[1].owner).encode().unwrap())
        .await
        .unwrap()
        .is_some());
    assert_eq!(blocks.values.load().len(), 2);
    assert!(task.deferred_ranges);
}
