#[path = "common/file_blocks.rs"]
mod blocks;
#[path = "common/store.rs"]
mod common;
#[path = "common/file.rs"]
mod file;
#[path = "common/multipart.rs"]
mod fixtures;

use std::sync::{atomic::Ordering, Arc};

use crowdb_access_iceberg::file::{
    FileIdentity, FileReader, FileTreeWriter, MultipartPart, MultipartPhase, MultipartRepository,
    MultipartSelection, MultipartSession, SelectedPart,
};
use crowdb_access_iceberg::key::FileId;

async fn setup() -> (
    file::TestFile,
    Arc<blocks::TestBlocks>,
    MultipartSession,
    MultipartSelection,
) {
    let fixture = file::TestFile::new(common::TestStore::default()).await;
    let blocks = Arc::new(blocks::TestBlocks::default());
    let mut session = fixtures::session();
    session.context = fixture.context;
    session.owner = FileIdentity {
        table: fixture.table,
        file: fixture.record("file", b"{}").file,
    };
    session.location = fixture.table.file("file").unwrap();
    let repository = MultipartRepository::new(fixture.store.clone());
    repository.begin(&session, 100).await.unwrap();
    let mut selected = Vec::new();
    for (number, bytes) in [(1, b"abcdefghij".as_slice()), (3, b""), (5, b"0123456")] {
        let owner = FileIdentity {
            file: FileId::random(),
            ..session.owner
        };
        let mut writer = FileTreeWriter::new(blocks.clone(), owner, 8).unwrap();
        writer.push(bytes).await.unwrap();
        let part = MultipartPart {
            upload: session.upload,
            number,
            revision: 1,
            owner,
            tree: writer.finish().await.unwrap(),
        };
        selected.push(SelectedPart {
            number,
            revision: 1,
            digest: part.tree.digest,
        });
        assert!(repository.reserve_part(&session, &part, 101).await.unwrap());
        session = load(&repository, &session).await;
        assert!(repository.settle_part(&session).await.unwrap());
        session = load(&repository, &session).await;
    }
    (
        fixture,
        blocks,
        session,
        MultipartSelection::new(selected).unwrap(),
    )
}

async fn load(repository: &MultipartRepository, session: &MultipartSession) -> MultipartSession {
    repository
        .load(session.context, session.upload)
        .await
        .unwrap()
        .unwrap()
}

#[test]
fn frozen_selection_is_ordered_versioned_and_independently_bounded() {
    let parts: Vec<_> = (1..=10_000)
        .map(|number| SelectedPart {
            number,
            revision: 1,
            digest: [9; 32],
        })
        .collect();
    let selection = MultipartSelection::new(parts.clone()).unwrap();
    let bytes = selection.encode();
    assert_eq!(bytes.len(), 420_007);
    assert_eq!(MultipartSelection::decode(&bytes).unwrap(), selection);
    for length in [0, 6, 7, bytes.len() - 1] {
        assert!(MultipartSelection::decode(&bytes[..length]).is_err());
    }
    let mut corrupt = bytes.clone();
    corrupt[4] = 2;
    assert!(MultipartSelection::decode(&corrupt).is_err());
    corrupt = bytes;
    corrupt.push(0);
    assert!(MultipartSelection::decode(&corrupt).is_err());
    assert!(MultipartSelection::new(Vec::new()).is_err());
    assert!(MultipartSelection::new(vec![parts[0], parts[0]]).is_err());
    assert!(MultipartSelection::new(vec![parts[1], parts[0]]).is_err());
    assert!(MultipartSelection::new(vec![SelectedPart {
        revision: 0,
        ..parts[0]
    }])
    .is_err());
}

#[tokio::test]
async fn lost_selection_and_progress_replies_resume_exact_bytes_on_new_instances() {
    for lost in 1..=2 {
        let (fixture, blocks, initial, selection) = setup().await;
        let repository = MultipartRepository::new(fixture.store.clone());
        fixture.store.fail_after.store(
            fixture.store.writes.load(Ordering::SeqCst) + lost,
            Ordering::SeqCst,
        );
        assert!(repository
            .freeze_completion(&initial, &selection, 102)
            .await
            .is_err());
        let mut session = load(&repository, &initial).await;
        if session.phase == MultipartPhase::Open {
            assert!(repository
                .freeze_completion(&session, &selection, 102)
                .await
                .unwrap());
            session = load(&repository, &session).await;
        }
        assert_eq!(session.phase, MultipartPhase::Completing);
        assert!(!repository
            .freeze_completion(&initial, &selection, 102)
            .await
            .unwrap());
        let mut steps = 0;
        while session.completion.as_ref().unwrap().progress.next_part < 3 {
            let recovery = MultipartRepository::new(fixture.store.clone());
            let before = session.completion.as_ref().unwrap().progress.completed_bytes;
            fixture
                .store
                .fail_after
                .store(fixture.store.writes.load(Ordering::SeqCst) + 1, Ordering::SeqCst);
            assert!(recovery
                .advance_completion(&session, blocks.clone(), 4, 8)
                .await
                .is_err());
            let old = session;
            session = load(&recovery, &old).await;
            assert!(session.completion.as_ref().unwrap().progress.completed_bytes - before <= 4);
            assert!(!recovery
                .advance_completion(&old, blocks.clone(), 4, 8)
                .await
                .unwrap());
            steps += 1;
            assert!(steps <= 6);
        }
        assert_eq!(steps, 6);
        let tree = repository
            .assembled_tree(&session, blocks.clone(), 8)
            .await
            .unwrap();
        assert_eq!(tree.length, 17);
        let mut reader = FileReader::from_tree(blocks, session.owner, tree, None, 3).unwrap();
        let mut bytes = Vec::new();
        while let Some(frame) = reader.next().await.unwrap() {
            bytes.extend_from_slice(&frame);
        }
        assert_eq!(bytes, b"abcdefghij0123456");
        assert_eq!(session.phase, MultipartPhase::Completing);
        assert!(session.published.is_none());
    }
}

#[tokio::test]
async fn selection_mismatch_and_abort_never_publish_or_advance_partial_bytes() {
    for changed in [0, 1, 2] {
        let (fixture, blocks, initial, selection) = setup().await;
        let repository = MultipartRepository::new(fixture.store.clone());
        let mut parts = selection.parts().to_vec();
        match changed {
            0 => parts[0].digest = [4; 32],
            1 => parts[0].revision += 1,
            _ => parts[0].number = 2,
        }
        let selection = MultipartSelection::new(parts).unwrap();
        assert!(repository
            .freeze_completion(&initial, &selection, 102)
            .await
            .unwrap());
        let session = load(&repository, &initial).await;
        let writes = blocks.writes.load(Ordering::SeqCst);
        assert!(repository
            .advance_completion(&session, blocks.clone(), 4, 8)
            .await
            .is_err());
        assert_eq!(blocks.writes.load(Ordering::SeqCst), writes);
        assert_eq!(load(&repository, &session).await, session);
        assert!(repository
            .assembled_tree(&session, blocks.clone(), 8)
            .await
            .is_err());
        assert!(repository.abort(&session).await.unwrap());
        assert!(!repository
            .advance_completion(&session, blocks.clone(), 4, 8)
            .await
            .unwrap());
        assert_eq!(blocks.writes.load(Ordering::SeqCst), writes);
        let aborted = load(&repository, &session).await;
        assert_eq!(aborted.completion, session.completion);
        assert!(aborted.published.is_none());
    }
}

#[tokio::test]
async fn completion_rejects_invalid_work_limits_and_unpersisted_progress() {
    let (fixture, blocks, initial, selection) = setup().await;
    let repository = MultipartRepository::new(fixture.store.clone());
    assert!(repository
        .freeze_completion(&initial, &selection, initial.expires_ms)
        .await
        .is_err());
    assert!(repository
        .freeze_completion(&initial, &selection, 102)
        .await
        .unwrap());
    let session = load(&repository, &initial).await;
    for (step, block) in [(0, 8), (1_048_577, 8), (4, 0), (4, 262_145)] {
        assert!(repository
            .advance_completion(&session, blocks.clone(), step, block)
            .await
            .is_err());
    }
    let mut forged = session.clone();
    forged.completion = Some(fixtures::completion(&session));
    assert!(!repository
        .advance_completion(&forged, blocks, 4, 8)
        .await
        .unwrap());
    fixture
        .root(
            fixture.context,
            crowdb_access_iceberg::catalog::RootState::Fencing,
        )
        .await;
    assert!(repository.load(session.context, session.upload).await.is_err());
}
