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
        tree: writer.finish().await.unwrap(),
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
        digest: part.tree.digest,
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
async fn observed_recovery_defers_active_copy_but_resumes_unchanged_revisions() {
    let fixture = file::TestFile::new(common::TestStore::default()).await;
    let blocks = Arc::new(blocks::TestBlocks::default());
    let repository = MultipartRepository::new(fixture.store.clone());
    let session = completing(&fixture, blocks.clone()).await;
    let recovery = MultipartRecovery::new(fixture.store.clone(), blocks.clone(), 4, 8).unwrap();
    let writes = fixture.store.writes.load(Ordering::SeqCst);
    let observation = recovery.observe_page(fixture.context, None).await.unwrap();
    assert_eq!(fixture.store.writes.load(Ordering::SeqCst), writes);
    assert!(repository
        .advance_completion(&session, blocks.clone(), 4, 8)
        .await
        .unwrap());
    let writes = fixture.store.writes.load(Ordering::SeqCst);
    let block_writes = blocks.writes.load(Ordering::SeqCst);
    let report = recovery.recover_observed_page(observation, 103).await.unwrap();
    assert_eq!((report.progressed, report.deferred), (0, 1));
    assert!(report.failures.is_empty());
    assert_eq!(fixture.store.writes.load(Ordering::SeqCst), writes);
    assert_eq!(blocks.writes.load(Ordering::SeqCst), block_writes);
    for expected in [8, 10] {
        let observation = recovery.observe_page(fixture.context, None).await.unwrap();
        let report = recovery.recover_observed_page(observation, 104).await.unwrap();
        assert_eq!((report.progressed, report.deferred), (1, 0));
        assert!(report.failures.is_empty());
        let current = repository
            .load(session.context, session.upload)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(current.completion.unwrap().progress.completed_bytes, expected);
    }
    let observation = recovery.observe_page(fixture.context, None).await.unwrap();
    let report = recovery.recover_observed_page(observation, 105).await.unwrap();
    assert_eq!(report.awaiting_seal, [session.upload]);
}

#[tokio::test]
async fn stale_observations_cannot_delay_expiry_or_authorize_retired_catalogs() {
    for retire in [false, true] {
        let fixture = file::TestFile::new(common::TestStore::default()).await;
        let blocks = Arc::new(blocks::TestBlocks::default());
        let repository = MultipartRepository::new(fixture.store.clone());
        let session = completing(&fixture, blocks.clone()).await;
        let recovery = MultipartRecovery::new(fixture.store.clone(), blocks.clone(), 4, 8).unwrap();
        let observation = recovery.observe_page(fixture.context, None).await.unwrap();
        assert!(repository
            .advance_completion(&session, blocks.clone(), 4, 8)
            .await
            .unwrap());
        if retire {
            let mut replacement = fixture.context;
            replacement.activation_epoch += 1;
            fixture.root(replacement, RootState::Ready).await;
        }
        let writes = blocks.writes.load(Ordering::SeqCst);
        let result = recovery
            .recover_observed_page(observation, session.expires_ms)
            .await;
        assert_eq!(blocks.writes.load(Ordering::SeqCst), writes);
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
async fn observed_sweeps_keep_fixed_pages_and_recover_new_sessions_on_later_visits() {
    let fixture = file::TestFile::new(common::TestStore::default()).await;
    let blocks = Arc::new(blocks::TestBlocks::default());
    let recovery = MultipartRecovery::new(fixture.store.clone(), blocks.clone(), 4, 8).unwrap();
    let empty = recovery.observe_page(fixture.context, None).await.unwrap();
    for _ in 0..9 {
        completing(&fixture, blocks.clone()).await;
    }
    let report = recovery.recover_observed_page(empty, 103).await.unwrap();
    assert_eq!((report.progressed, report.deferred), (0, 4));
    let mut continuation = None;
    let mut progressed = 0;
    loop {
        let observation = recovery
            .observe_page(fixture.context, continuation)
            .await
            .unwrap();
        let report = recovery.recover_observed_page(observation, 104).await.unwrap();
        assert!(report.progressed <= 4);
        assert_eq!(report.deferred, 0);
        assert!(report.failures.is_empty());
        progressed += report.progressed;
        continuation = report.continuation;
        if continuation.is_none() {
            break;
        }
    }
    assert_eq!(progressed, 9);
}
