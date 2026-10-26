#[path = "common/file_blocks.rs"]
mod blocks;
#[path = "common/store.rs"]
mod common;
#[path = "common/file.rs"]
mod file;
#[path = "common/multipart.rs"]
mod fixtures;

use sha2::Digest;
use std::sync::{atomic::Ordering, Arc};

use crowdb_access_iceberg::file::{
    FileContent, FileIdentity, FileReader, FileTreeWriter, MultipartPart, MultipartPhase,
    MultipartRepository, MultipartSelection, MultipartSession, MultipartStreamPart, SelectedPart,
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
            modified_ms: 101,
            owner,
            tree: Some(writer.finish().await.unwrap()),
            stream: None,
        };
        selected.push(SelectedPart {
            number,
            revision: 1,
            digest: part.selection_digest(),
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

#[test]
fn streamed_selection_rejects_oversized_snapshot_before_copying_part_locations() {
    let session = fixtures::session();
    let part = MultipartPart {
        upload: session.upload,
        number: 1,
        revision: 1,
        modified_ms: 100,
        owner: session.owner,
        tree: None,
        stream: Some(MultipartStreamPart {
            length: 1,
            content: FileContent::Locations {
                bytes: vec![0; crowdb_access_iceberg::operation::MAX_PAYLOAD_BYTES],
                etag: "00000000000000000000000000000000".into(),
            },
        }),
    };
    assert!(matches!(
        MultipartSelection::with_stream_parts(&[part]),
        Err(crowdb_access_iceberg::error::ValidationError::RecordTooLarge)
    ));
    assert!(matches!(
        MultipartSelection::decode(&vec![0; crowdb_access_iceberg::operation::MAX_PAYLOAD_BYTES + 1]),
        Err(crowdb_access_iceberg::error::ValidationError::RecordTooLarge)
    ));
}

#[tokio::test]
async fn selection_reply_loss_allows_full_input_retry_without_byte_progress_writes() {
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
                .unwrap()
                .is_applied());
            session = load(&repository, &session).await;
        }
        assert_eq!(session.phase, MultipartPhase::Completing);
        let mutations = fixture.store.writes.load(Ordering::SeqCst);
        let reads = fixture.store.reads.load(Ordering::SeqCst);
        let tree = repository
            .assemble(&session, &selection, blocks.clone(), 1000, 8)
            .await
            .unwrap();
        assert_eq!(fixture.store.writes.load(Ordering::SeqCst), mutations);
        // Two request-local session/root guards and one authority read per selected part.
        assert_eq!(fixture.store.reads.load(Ordering::SeqCst) - reads, 7);
        assert_eq!(load(&repository, &session).await, session);
        let mut reader = FileReader::from_tree(blocks.clone(), session.owner, tree.clone(), None, 3).unwrap();
        let mut bytes = Vec::new();
        while let Some(frame) = reader.next().await.unwrap() {
            bytes.extend_from_slice(&frame);
        }
        assert_eq!(bytes, b"abcdefghij0123456");
        assert!(!blocks
            .values
            .load()
            .values()
            .any(|bytes| bytes.starts_with(b"ICFW")));
        let retry = MultipartRepository::new(fixture.store.clone())
            .assemble(&session, &selection, blocks, 1000, 8)
            .await
            .unwrap();
        assert_eq!((retry.length, retry.digest), (tree.length, tree.digest));
    }
}

#[tokio::test]
async fn uncertain_copy_has_no_resumable_state_and_full_request_retry_is_exact() {
    let (fixture, blocks, initial, selection) = setup().await;
    let repository = MultipartRepository::new(fixture.store.clone());
    assert!(repository
        .freeze_completion(&initial, &selection, 102)
        .await
        .unwrap()
        .is_applied());
    let session = load(&repository, &initial).await;
    let mutations = fixture.store.writes.load(Ordering::SeqCst);
    blocks
        .fail_after
        .store(blocks.writes.load(Ordering::SeqCst) + 1, Ordering::SeqCst);
    assert!(repository
        .assemble(&session, &selection, blocks.clone(), 1000, 8)
        .await
        .is_err());
    assert_eq!(load(&repository, &session).await, session);
    assert_eq!(fixture.store.writes.load(Ordering::SeqCst), mutations);
    let tree = MultipartRepository::new(fixture.store.clone())
        .assemble(&session, &selection, blocks.clone(), 1000, 8)
        .await
        .unwrap();
    assert_eq!(tree.length, 17);
    assert_eq!(
        tree.digest,
        <[u8; 32]>::from(sha2::Sha256::digest(b"abcdefghij0123456"))
    );
    assert_eq!(load(&repository, &session).await, session);
}

#[tokio::test]
async fn abort_during_copy_rejects_the_result_without_publishing_or_resuming_work() {
    let (fixture, blocks, initial, selection) = setup().await;
    let repository = MultipartRepository::new(fixture.store.clone());
    assert!(repository
        .freeze_completion(&initial, &selection, 102)
        .await
        .unwrap()
        .is_applied());
    let session = load(&repository, &initial).await;
    blocks.pause_reads.store(true, Ordering::SeqCst);
    let copy = repository.assemble(&session, &selection, blocks.clone(), 1000, 8);
    tokio::pin!(copy);
    tokio::select! {
        result = &mut copy => panic!("copy finished before its blocked read: {result:?}"),
        () = blocks.read_entered.notified() => {}
    }
    assert!(repository.abort(&session).await.unwrap());
    blocks.pause_reads.store(false, Ordering::SeqCst);
    blocks.read_release.notify_one();
    assert!(copy.await.is_err());
    let current = load(&repository, &session).await;
    assert_eq!(current.phase, MultipartPhase::Aborted);
    assert_eq!(current.completion, session.completion);
    assert!(current.published.is_none());
}

#[tokio::test]
async fn selection_mismatch_and_abort_never_publish_or_copy_partial_bytes() {
    for changed in 0..3 {
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
            .unwrap()
            .is_applied());
        let session = load(&repository, &initial).await;
        let writes = blocks.writes.load(Ordering::SeqCst);
        assert!(repository
            .assemble(&session, &selection, blocks.clone(), 1000, 8)
            .await
            .is_err());
        assert_eq!(blocks.writes.load(Ordering::SeqCst), writes);
        assert_eq!(load(&repository, &session).await, session);
        assert!(repository.abort(&session).await.unwrap());
        assert!(repository
            .assemble(&session, &selection, blocks.clone(), 1000, 8)
            .await
            .is_err());
        assert_eq!(blocks.writes.load(Ordering::SeqCst), writes);
        let aborted = load(&repository, &session).await;
        assert_eq!(aborted.completion, session.completion);
        assert!(aborted.published.is_none());
    }
}

#[tokio::test]
async fn complete_byte_admission_checks_the_entire_selection_before_any_physical_copy() {
    let (fixture, blocks, initial, selection) = setup().await;
    let repository = MultipartRepository::new(fixture.store.clone());
    assert!(repository
        .freeze_completion(&initial, &selection, initial.expires_ms)
        .await
        .is_err());
    assert!(repository
        .freeze_completion(&initial, &selection, 102)
        .await
        .unwrap()
        .is_applied());
    let session = load(&repository, &initial).await;
    let writes = blocks.writes.load(Ordering::SeqCst);
    let reads = blocks.reads.load(Ordering::SeqCst);
    for (limit, block) in [(0, 8), (16, 8), (1000, 0), (1000, 262_145)] {
        assert!(repository
            .assemble(&session, &selection, blocks.clone(), limit, block)
            .await
            .is_err());
    }
    assert_eq!(blocks.writes.load(Ordering::SeqCst), writes);
    assert_eq!(blocks.reads.load(Ordering::SeqCst), reads);
    let mut forged = session.clone();
    forged.completion = Some(fixtures::completion(&session));
    assert!(repository
        .assemble(&forged, &selection, blocks, 1000, 8)
        .await
        .is_err());
    fixture
        .root(
            fixture.context,
            crowdb_access_iceberg::catalog::RootState::Fencing,
        )
        .await;
    assert!(repository.load(session.context, session.upload).await.is_err());
}
