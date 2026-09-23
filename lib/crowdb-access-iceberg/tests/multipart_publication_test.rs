#[path = "common/file_blocks.rs"]
mod blocks;
#[path = "common/store.rs"]
mod common;
#[path = "common/file.rs"]
mod file;
#[path = "common/multipart.rs"]
mod fixtures;
#[path = "common/multipart_recovery_store.rs"]
mod scan;

use std::sync::{atomic::Ordering, Arc};

use crowdb_access_iceberg::catalog::{CatalogError, CatalogStore};
use crowdb_access_iceberg::file::{
    FileIdentity, FileRecord, FileRepository, FileTree, FileTreeWriter, MultipartPart, MultipartPhase,
    MultipartRecovery, MultipartRepository, MultipartSelection, MultipartSession, SelectedPart,
};
use crowdb_access_iceberg::key::FileId;
use crowdb_access_iceberg::operation::mutation_identity;

async fn setup() -> (
    file::TestFile,
    Arc<blocks::TestBlocks>,
    MultipartSession,
    FileTree,
    FileRecord,
) {
    let fixture = file::TestFile::new(common::TestStore::default()).await;
    let blocks = Arc::new(blocks::TestBlocks::default());
    let mut session = fixtures::session();
    let sealed = fixture.record("metadata.json", b"{}");
    session.context = fixture.context;
    session.owner = FileIdentity {
        table: fixture.table,
        file: sealed.file,
    };
    session.location = sealed.location.clone();
    let repository = MultipartRepository::new(fixture.store.clone());
    repository.begin(&session, 100).await.unwrap();
    let owner = FileIdentity {
        file: FileId::random(),
        ..session.owner
    };
    let mut writer = FileTreeWriter::new(blocks.clone(), owner, 8).unwrap();
    writer.push(b"{}").await.unwrap();
    let part = MultipartPart {
        upload: session.upload,
        number: 1,
        revision: 1,
        modified_ms: 101,
        owner,
        tree: writer.finish().await.unwrap(),
    };
    repository.reserve_part(&session, &part, 101).await.unwrap();
    session = load(&repository, &session).await;
    repository.settle_part(&session).await.unwrap();
    session = load(&repository, &session).await;
    let selection = MultipartSelection::new(vec![SelectedPart {
        number: 1,
        revision: 1,
        digest: part.tree.digest,
    }])
    .unwrap();
    repository
        .freeze_completion(&session, &selection, 102)
        .await
        .unwrap();
    session = load(&repository, &session).await;
    repository
        .advance_completion(&session, blocks.clone(), 8, 8)
        .await
        .unwrap();
    session = load(&repository, &session).await;
    let tree = repository
        .assembled_tree(&session, blocks.clone(), 8)
        .await
        .unwrap();
    (fixture, blocks, session, tree, sealed)
}

async fn load(repository: &MultipartRepository, session: &MultipartSession) -> MultipartSession {
    repository
        .load(session.context, session.upload)
        .await
        .unwrap()
        .unwrap()
}

#[tokio::test]
async fn every_lost_publication_reply_recovers_the_same_seal_and_file_identity() {
    for lost in 1..=5 {
        let (fixture, blocks, mut session, tree, sealed) = setup().await;
        let repository = MultipartRepository::new(fixture.store.clone());
        fixture.store.fail_after.store(
            fixture.store.writes.load(Ordering::SeqCst) + lost,
            Ordering::SeqCst,
        );
        let prepared = repository
            .prepare_publication(&session, &tree, &sealed, 103)
            .await;
        assert_eq!(prepared.is_err(), lost <= 2);
        session = load(&repository, &session).await;
        if session.phase == MultipartPhase::Completing {
            assert!(repository
                .prepare_publication(&session, &tree, &sealed, 103)
                .await
                .unwrap());
            session = load(&repository, &session).await;
        }
        let result = repository.publish(&session).await;
        assert_eq!(result.is_err(), lost >= 3);
        let recovery = MultipartRecovery::new(fixture.store.clone(), blocks, 8, 8).unwrap();
        let report = recovery
            .recover_page(session.context, None, session.expires_ms)
            .await
            .unwrap();
        assert!(report.failures.is_empty(), "{:?}", report.failures);
        let recovery = MultipartRepository::new(fixture.store.clone());
        session = load(&recovery, &session).await;
        assert_eq!(session.phase, MultipartPhase::Published);
        assert_eq!(session.published, Some(sealed.file));
        let writes = fixture.store.writes.load(Ordering::SeqCst);
        assert_eq!(recovery.publish(&session).await.unwrap(), Some(sealed));
        assert_eq!(fixture.store.writes.load(Ordering::SeqCst), writes);
        assert!(recovery.abort(&session).await.is_err());
    }
}

#[tokio::test]
async fn equal_existing_location_replays_its_original_identity_and_different_bytes_never_overwrite() {
    for equal in [true, false] {
        let (fixture, _, session, tree, sealed) = setup().await;
        let existing = fixture.record("metadata.json", if equal { b"{}" } else { b"[]" });
        FileRepository::new(fixture.store.clone())
            .publish(session.context, &existing)
            .await
            .unwrap();
        let repository = MultipartRepository::new(fixture.store.clone());
        repository
            .prepare_publication(&session, &tree, &sealed, 103)
            .await
            .unwrap();
        let publishing = load(&repository, &session).await;
        let result = repository.publish(&publishing).await;
        if equal {
            assert_eq!(result.unwrap(), Some(existing.clone()));
            let published = load(&repository, &session).await;
            assert_eq!(published.published, Some(existing.file));
            assert_eq!(
                repository.publish(&published).await.unwrap(),
                Some(existing.clone())
            );
        } else {
            assert!(matches!(result, Err(CatalogError::Conflict)));
            let conflicted = load(&repository, &session).await;
            assert_eq!(conflicted.phase, MultipartPhase::Conflicted);
            assert_eq!(conflicted.completion, publishing.completion);
            assert!(matches!(
                repository.publish(&conflicted).await,
                Err(CatalogError::Conflict)
            ));
        }
        assert_eq!(
            FileRepository::new(fixture.store.clone())
                .load(session.context, &session.location)
                .await
                .unwrap(),
            Some(existing)
        );
    }
}

#[tokio::test]
async fn abort_and_frozen_publication_share_one_fence_without_allowing_post_abort_publication() {
    let (fixture, _, session, tree, sealed) = setup().await;
    let first = MultipartRepository::new(fixture.store.clone());
    let second = MultipartRepository::new(fixture.store.clone());
    let (prepared, aborted) = tokio::join!(
        first.prepare_publication(&session, &tree, &sealed, 103),
        second.abort(&session)
    );
    assert_ne!(prepared.unwrap(), aborted.unwrap());
    let current = load(&first, &session).await;
    if current.phase == MultipartPhase::Publishing {
        assert!(second.abort(&current).await.is_err());
        assert_eq!(first.publish(&current).await.unwrap(), Some(sealed));
    } else {
        assert_eq!(current.phase, MultipartPhase::Aborted);
        assert!(first.publish(&current).await.is_err());
        assert!(FileRepository::new(fixture.store.clone())
            .load(session.context, &session.location)
            .await
            .unwrap()
            .is_none());
    }
}

#[tokio::test]
async fn changed_byte_identity_incomplete_progress_and_corrupt_intent_fail_before_publication() {
    let (fixture, _, session, tree, sealed) = setup().await;
    let repository = MultipartRepository::new(fixture.store.clone());
    for change in [0, 1, 2] {
        let mut changed = sealed.clone();
        match change {
            0 => changed.file = FileId::random(),
            1 => changed.location = fixture.table.file("elsewhere").unwrap(),
            _ => changed.digest = [5; 32],
        }
        assert!(repository
            .prepare_publication(&session, &tree, &changed, 103)
            .await
            .is_err());
    }
    let mut incomplete = session.clone();
    incomplete.completion = Some(fixtures::completion(&session));
    assert!(repository
        .prepare_publication(&incomplete, &tree, &sealed, 103)
        .await
        .is_err());
    assert!(repository
        .prepare_publication(&session, &tree, &sealed, session.expires_ms)
        .await
        .is_err());
    repository
        .prepare_publication(&session, &tree, &sealed, 103)
        .await
        .unwrap();
    let publishing = load(&repository, &session).await;
    let key = publishing
        .completion
        .as_ref()
        .unwrap()
        .publication
        .as_ref()
        .unwrap()
        .page_key(0)
        .unwrap()
        .encode()
        .unwrap();
    let value = fixture.store.get(&key).await.unwrap().unwrap();
    fixture
        .store
        .compare_exchange(
            &key,
            Some(&value.bytes),
            b"bad",
            mutation_identity(&key, Some(&value.bytes), b"bad"),
        )
        .await
        .unwrap();
    assert!(repository.publish(&publishing).await.is_err());
    assert!(FileRepository::new(fixture.store.clone())
        .load(session.context, &session.location)
        .await
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn lost_conflict_marker_reply_retains_a_terminal_nonpublishing_outcome() {
    let (fixture, blocks, session, tree, sealed) = setup().await;
    let existing = fixture.record("metadata.json", b"[]");
    FileRepository::new(fixture.store.clone())
        .publish(session.context, &existing)
        .await
        .unwrap();
    let repository = MultipartRepository::new(fixture.store.clone());
    repository
        .prepare_publication(&session, &tree, &sealed, 103)
        .await
        .unwrap();
    let publishing = load(&repository, &session).await;
    fixture
        .store
        .fail_after
        .store(fixture.store.writes.load(Ordering::SeqCst) + 1, Ordering::SeqCst);
    assert!(repository.publish(&publishing).await.is_err());
    let conflicted = load(&repository, &session).await;
    assert_eq!(conflicted.phase, MultipartPhase::Conflicted);
    assert!(conflicted.published.is_none());
    let recovery = MultipartRecovery::new(fixture.store.clone(), blocks, 8, 8).unwrap();
    let report = recovery
        .recover_page(session.context, None, session.expires_ms)
        .await
        .unwrap();
    assert_eq!(report.retained, 1);
    assert!(report.failures.is_empty());
    assert_eq!(
        FileRepository::new(fixture.store.clone())
            .load(session.context, &session.location)
            .await
            .unwrap(),
        Some(existing)
    );
}
