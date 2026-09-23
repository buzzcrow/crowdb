#[path = "common/file_blocks.rs"]
mod blocks;
#[path = "common/store.rs"]
mod common;
#[path = "common/file.rs"]
mod file;
#[path = "common/multipart.rs"]
mod fixtures;

use std::sync::{atomic::Ordering, Arc};

use crowdb_access_iceberg::catalog::{CatalogError, CatalogStore, RootState};
use crowdb_access_iceberg::file::{
    FileIdentity, FileTreeWriter, MultipartPart, MultipartPhase, MultipartRepository, MultipartSession,
};
use crowdb_access_iceberg::key::FileId;
use crowdb_access_iceberg::operation::mutation_identity;
use crowdb_access_iceberg::record::StorageRecord;

async fn setup() -> (file::TestFile, MultipartSession) {
    let fixture = file::TestFile::new(common::TestStore::default()).await;
    let mut session = fixtures::session();
    session.context = fixture.context;
    session.owner = FileIdentity {
        table: fixture.table,
        file: fixture.record("file", b"{}").file,
    };
    session.location = fixture.table.file("file").unwrap();
    (fixture, session)
}

async fn part(session: &MultipartSession, number: u16, revision: u64, length: usize) -> MultipartPart {
    let owner = FileIdentity {
        file: FileId::random(),
        ..session.owner
    };
    let mut writer = FileTreeWriter::new(Arc::new(blocks::TestBlocks::default()), owner, 32).unwrap();
    writer.push(&vec![5; length]).await.unwrap();
    MultipartPart {
        upload: session.upload,
        number,
        revision,
        owner,
        tree: writer.finish().await.unwrap(),
    }
}

async fn load(repository: &MultipartRepository, session: &MultipartSession) -> MultipartSession {
    repository
        .load(session.context, session.upload)
        .await
        .unwrap()
        .unwrap()
}

#[tokio::test]
async fn each_lost_part_mutation_reply_recovers_exact_counts_on_a_new_instance() {
    for replacement in [false, true] {
        for lost in 1..=3 {
            let (fixture, initial) = setup().await;
            let repository = MultipartRepository::new(fixture.store.clone());
            let mut session = repository.begin(&initial, 100).await.unwrap();
            if replacement {
                let first = part(&session, 1, 1, 80).await;
                assert!(repository.reserve_part(&session, &first, 101).await.unwrap());
                session = load(&repository, &session).await;
                assert!(repository.settle_part(&session).await.unwrap());
                session = load(&repository, &session).await;
            }
            let candidate = part(&session, 1, if replacement { 2 } else { 1 }, 30).await;
            fixture.store.fail_after.store(
                fixture.store.writes.load(Ordering::SeqCst) + lost,
                Ordering::SeqCst,
            );
            let reserved = repository.reserve_part(&session, &candidate, 102).await;
            assert_eq!(reserved.is_err(), lost == 1);
            session = load(&repository, &session).await;
            assert_eq!(session.part_count, 1);
            assert_eq!(session.staged_bytes, 30);
            assert!(session.pending.is_some());
            assert!(matches!(
                repository.part(&session, 1).await,
                Err(CatalogError::Busy)
            ));
            let recovery = MultipartRepository::new(fixture.store.clone());
            let settled = recovery.settle_part(&session).await;
            assert_eq!(settled.is_err(), lost != 1);
            session = load(&recovery, &session).await;
            if session.pending.is_some() {
                assert!(recovery.settle_part(&session).await.unwrap());
                session = load(&recovery, &session).await;
            }
            assert!(session.pending.is_none());
            assert_eq!((session.part_count, session.staged_bytes), (1, 30));
            assert_eq!(recovery.part(&session, 1).await.unwrap(), Some(candidate.clone()));
            assert!(matches!(
                recovery.part(&initial, 1).await,
                Err(CatalogError::Busy)
            ));
            let value = fixture
                .store
                .get(&candidate.key().encode().unwrap())
                .await
                .unwrap()
                .unwrap();
            assert_eq!(
                StorageRecord::decode(&candidate.key(), &value.bytes).unwrap(),
                StorageRecord::MultipartPart(Box::new(candidate))
            );
        }
    }
}

#[tokio::test]
async fn session_replay_and_abort_preserve_authority_after_response_loss() {
    let (fixture, initial) = setup().await;
    let repository = MultipartRepository::new(fixture.store.clone());
    fixture
        .store
        .fail_after
        .store(fixture.store.writes.load(Ordering::SeqCst) + 1, Ordering::SeqCst);
    assert!(repository.begin(&initial, 100).await.is_err());
    assert_eq!(repository.begin(&initial, 101).await.unwrap(), initial);
    let mut changed = initial.clone();
    changed.principal = [9; 32];
    assert!(matches!(
        repository.begin(&changed, 101).await,
        Err(CatalogError::Conflict)
    ));
    fixture
        .store
        .fail_after
        .store(fixture.store.writes.load(Ordering::SeqCst) + 1, Ordering::SeqCst);
    assert!(repository.abort(&initial).await.is_err());
    let aborted = load(&repository, &initial).await;
    assert_eq!(aborted.phase, MultipartPhase::Aborted);
    assert_eq!(
        repository.begin(&initial, initial.expires_ms).await.unwrap(),
        aborted
    );
    assert!(repository
        .reserve_part(&aborted, &part(&aborted, 1, 1, 1).await, 101)
        .await
        .is_err());
    assert!(!repository.abort(&initial).await.unwrap());
}

#[tokio::test]
async fn competing_part_and_abort_share_one_fence_and_stale_helpers_cannot_rewrite() {
    let (fixture, initial) = setup().await;
    let first = MultipartRepository::new(fixture.store.clone());
    let second = MultipartRepository::new(fixture.store.clone());
    first.begin(&initial, 100).await.unwrap();
    let candidate = part(&initial, 1, 1, 20).await;
    let (reserved, aborted) = tokio::join!(
        first.reserve_part(&initial, &candidate, 101),
        second.abort(&initial)
    );
    assert_ne!(reserved.unwrap(), aborted.unwrap());
    let mut current = load(&first, &initial).await;
    if current.phase == MultipartPhase::Aborted {
        assert!(first.reserve_part(&current, &candidate, 101).await.is_err());
        return;
    }
    let stale = current.clone();
    assert!(matches!(second.abort(&current).await, Err(CatalogError::Busy)));
    assert!(second.settle_part(&current).await.unwrap());
    current = load(&first, &current).await;
    let replacement = part(&current, 1, 2, 40).await;
    assert!(first.reserve_part(&current, &replacement, 102).await.unwrap());
    current = load(&first, &current).await;
    assert!(first.settle_part(&current).await.unwrap());
    assert!(!second.settle_part(&stale).await.unwrap());
    current = load(&first, &current).await;
    assert_eq!((current.part_count, current.staged_bytes), (1, 40));
    assert!(first.abort(&current).await.unwrap());
    assert!(!second.settle_part(&stale).await.unwrap());
}

#[tokio::test]
async fn limits_expiry_and_pending_snapshots_fail_before_unjournaled_part_writes() {
    let (fixture, mut initial) = setup().await;
    initial.limits.max_parts = 2;
    initial.limits.max_file_bytes = 100;
    initial.limits.max_staged_bytes = 100;
    let repository = MultipartRepository::new(fixture.store.clone());
    repository.begin(&initial, 100).await.unwrap();
    let candidate = part(&initial, 1, 1, 80).await;
    assert!(repository
        .reserve_part(&initial, &candidate, initial.expires_ms)
        .await
        .is_err());
    assert!(repository.reserve_part(&initial, &candidate, 99).await.is_err());
    assert!(repository.reserve_part(&initial, &candidate, 101).await.unwrap());
    let pending = load(&repository, &initial).await;
    let mut forged = pending.clone();
    forged.pending.as_mut().unwrap().after.owner.file = FileId::random();
    assert!(!repository.settle_part(&forged).await.unwrap());
    assert!(fixture
        .store
        .get(&candidate.key().encode().unwrap())
        .await
        .unwrap()
        .is_none());
    assert!(repository.settle_part(&pending).await.unwrap());
    let session = load(&repository, &initial).await;
    let writes = fixture.store.writes.load(Ordering::SeqCst);
    assert!(repository
        .reserve_part(&session, &part(&session, 2, 1, 30).await, 101)
        .await
        .is_err());
    assert!(repository
        .reserve_part(&session, &part(&session, 3, 1, 1).await, 101)
        .await
        .is_err());
    assert!(repository
        .reserve_part(&session, &part(&session, 1, 1, 1).await, 101)
        .await
        .is_err());
    assert_eq!(fixture.store.writes.load(Ordering::SeqCst), writes);
    fixture.root(fixture.context, RootState::Fencing).await;
    assert!(matches!(
        repository.abort(&session).await,
        Err(CatalogError::Busy)
    ));
}

#[tokio::test]
async fn abort_retains_frozen_completion_evidence_without_physical_deletion() {
    let (fixture, mut session) = setup().await;
    session.phase = MultipartPhase::Completing;
    session.part_count = 1;
    session.completion = Some(fixtures::completion(&session));
    let key = session.key().encode().unwrap();
    let value = StorageRecord::MultipartSession(Box::new(session.clone()))
        .encode()
        .unwrap();
    fixture
        .store
        .compare_exchange(&key, None, &value, mutation_identity(&key, None, &value))
        .await
        .unwrap();
    let repository = MultipartRepository::new(fixture.store.clone());
    assert!(repository.abort(&session).await.unwrap());
    let aborted = load(&repository, &session).await;
    assert_eq!(aborted.completion, session.completion);
    assert_eq!(aborted.phase, MultipartPhase::Aborted);
}
