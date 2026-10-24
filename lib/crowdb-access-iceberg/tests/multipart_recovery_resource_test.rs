#[path = "common/file_blocks.rs"]
mod blocks;
#[path = "common/store.rs"]
mod common;
#[path = "common/file.rs"]
#[allow(dead_code)]
mod file;
#[path = "common/multipart.rs"]
#[allow(dead_code)]
mod fixtures;
#[path = "common/multipart_recovery_store.rs"]
mod scan;

use std::sync::{atomic::Ordering, Arc};

use crowdb_access_iceberg::{
    catalog::RootState,
    file::{
        FileIdentity, FileTreeWriter, MultipartPart, MultipartPhase, MultipartRecovery, MultipartRepository,
        MultipartSelection, MultipartSession, SelectedPart,
    },
    key::FileId,
};

async fn completing(fixture: &file::TestFile, blocks: Arc<blocks::TestBlocks>) -> MultipartSession {
    let repository = MultipartRepository::new(fixture.store.clone());
    let mut session = fixtures::session();
    session.context = fixture.context;
    session.owner.table = fixture.table;
    session.location = fixture.table.file(&session.upload.to_string()).unwrap();
    repository.begin(&session, 100).await.unwrap();
    let owner = FileIdentity {
        file: FileId::random(),
        ..session.owner
    };
    let mut writer = FileTreeWriter::new(blocks, owner, 8).unwrap();
    writer.push(b"0123456789").await.unwrap();
    let part = MultipartPart {
        upload: session.upload,
        number: 1,
        revision: 1,
        modified_ms: 101,
        owner,
        tree: Some(writer.finish().await.unwrap()),
        stream: None,
    };
    repository.reserve_part(&session, &part, 101).await.unwrap();
    session = repository
        .load(session.context, session.upload)
        .await
        .unwrap()
        .unwrap();
    repository.settle_part(&session).await.unwrap();
    session = repository
        .load(session.context, session.upload)
        .await
        .unwrap()
        .unwrap();
    let selection = MultipartSelection::new(vec![SelectedPart {
        number: 1,
        revision: 1,
        digest: part.selection_digest(),
    }])
    .unwrap();
    repository
        .freeze_completion(&session, &selection, 102)
        .await
        .unwrap();
    repository
        .load(session.context, session.upload)
        .await
        .unwrap()
        .unwrap()
}

#[tokio::test]
async fn background_sweeps_leave_client_copy_state_and_physical_bytes_untouched() {
    let fixture = file::TestFile::new(common::TestStore::default()).await;
    let blocks = Arc::new(blocks::TestBlocks::default());
    let repository = MultipartRepository::new(fixture.store.clone());
    let session = completing(&fixture, blocks.clone()).await;
    let recovery = MultipartRecovery::new(fixture.store.clone());
    let writes = fixture.store.writes.load(Ordering::SeqCst);
    let block_writes = blocks.writes.load(Ordering::SeqCst);
    let block_reads = blocks.reads.load(Ordering::SeqCst);
    blocks.pause_reads.store(true, Ordering::SeqCst);
    for _ in 0..3 {
        let report = recovery.recover_page(fixture.context, None, 103).await.unwrap();
        assert_eq!((report.progressed, report.retained), (0, 1));
        assert!(report.failures.is_empty());
    }
    assert_eq!(fixture.store.writes.load(Ordering::SeqCst), writes);
    assert_eq!(blocks.writes.load(Ordering::SeqCst), block_writes);
    assert_eq!(blocks.reads.load(Ordering::SeqCst), block_reads);
    assert_eq!(
        repository.load(session.context, session.upload).await.unwrap(),
        Some(session)
    );
}

#[tokio::test]
async fn expiry_remains_metadata_only_and_retired_catalogs_cannot_authorize_it() {
    for retire in [false, true] {
        let fixture = file::TestFile::new(common::TestStore::default()).await;
        let blocks = Arc::new(blocks::TestBlocks::default());
        let repository = MultipartRepository::new(fixture.store.clone());
        let session = completing(&fixture, blocks.clone()).await;
        let recovery = MultipartRecovery::new(fixture.store.clone());
        if retire {
            let mut replacement = fixture.context;
            replacement.activation_epoch += 1;
            fixture.root(replacement, RootState::Ready).await;
        }
        let physical = blocks.values.load_full();
        let result = recovery
            .recover_page(fixture.context, None, session.expires_ms)
            .await;
        assert_eq!(*blocks.values.load_full(), *physical);
        if retire {
            assert!(result.is_err());
        } else {
            let report = result.unwrap();
            assert_eq!((report.progressed, report.deferred), (1, 0));
            let current = repository
                .load(session.context, session.upload)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(current.phase, MultipartPhase::Aborted);
        }
    }
}

#[tokio::test]
async fn resource_sweeps_visit_every_session_with_bounded_pages_without_a_copy_observation_pass() {
    let fixture = file::TestFile::new(common::TestStore::default()).await;
    let blocks = Arc::new(blocks::TestBlocks::default());
    let recovery = MultipartRecovery::new(fixture.store.clone());
    for _ in 0..9 {
        completing(&fixture, blocks.clone()).await;
    }
    let mut continuation = None;
    let mut retained = 0;
    loop {
        let report = recovery
            .recover_page(fixture.context, continuation, 104)
            .await
            .unwrap();
        assert!(report.retained <= 4);
        assert_eq!((report.progressed, report.deferred), (0, 0));
        assert!(report.failures.is_empty());
        retained += report.retained;
        continuation = report.continuation;
        if continuation.is_none() {
            break;
        }
    }
    assert_eq!(retained, 9);
}
