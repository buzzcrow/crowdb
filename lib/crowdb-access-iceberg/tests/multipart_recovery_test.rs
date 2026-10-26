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

use crowdb_access_iceberg::catalog::{CatalogStore, RootState};
use crowdb_access_iceberg::file::{
    FileIdentity, FileTreeWriter, MultipartPart, MultipartPhase, MultipartRecovery, MultipartRecoveryScan,
    MultipartRepository, MultipartSelection, MultipartSession, SelectedPart,
};
use crowdb_access_iceberg::key::{CatalogId, FileId, OperationId};
use crowdb_access_iceberg::operation::mutation_identity;

fn session(fixture: &file::TestFile) -> MultipartSession {
    let mut session = fixtures::session();
    session.context = fixture.context;
    session.owner = FileIdentity {
        table: fixture.table,
        file: fixture.record("file", b"{}").file,
    };
    session.location = fixture.table.file(&session.upload.to_string()).unwrap();
    session
}

#[tokio::test]
async fn bounded_sweeps_settle_abandoned_part_mutations_then_expire_without_deleting_bytes() {
    let fixture = file::TestFile::new(common::TestStore::default()).await;
    let blocks = Arc::new(blocks::TestBlocks::default());
    let repository = MultipartRepository::new(fixture.store.clone());
    let mut uploads = Vec::new();
    for _ in 0..9 {
        let session = session(&fixture);
        repository.begin(&session, 100).await.unwrap();
        let owner = FileIdentity {
            file: FileId::random(),
            ..session.owner
        };
        let mut writer = FileTreeWriter::new(blocks.clone(), owner, 8).unwrap();
        writer.push(b"retained").await.unwrap();
        let part = MultipartPart {
            upload: session.upload,
            number: 1,
            revision: 1,
            modified_ms: 101,
            owner,
            tree: Some(writer.finish().await.unwrap()),
            stream: None,
        };
        assert!(repository.reserve_part(&session, &part, 101).await.unwrap());
        uploads.push((session, part));
    }
    let physical = blocks.values.load_full();
    for _ in 0..2 {
        let recovery = MultipartRecovery::new(fixture.store.clone());
        let mut cursor = None;
        let mut progressed = 0;
        loop {
            let report = recovery
                .recover_page(fixture.context, cursor, 1100)
                .await
                .unwrap();
            assert!(report.failures.is_empty(), "{:?}", report.failures);
            assert!(report.progressed <= 4);
            assert_eq!(report.retained + report.deferred + report.awaiting_seal.len(), 0);
            progressed += report.progressed;
            cursor = report.continuation;
            if cursor.is_none() {
                break;
            }
        }
        assert_eq!(progressed, 9);
    }
    assert_eq!(*blocks.values.load_full(), *physical);
    for (initial, part) in uploads {
        let current = repository
            .load(initial.context, initial.upload)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(current.phase, MultipartPhase::Aborted);
        assert!(current.pending.is_none());
        assert_eq!((current.part_count, current.staged_bytes), (1, 8));
        assert_eq!(repository.part(&current, 1).await.unwrap(), Some(part));
    }
}

#[tokio::test]
async fn recovery_waits_for_client_completion_without_touching_payload_and_expiry_still_aborts() {
    let fixture = file::TestFile::new(common::TestStore::default()).await;
    let blocks = Arc::new(blocks::TestBlocks::default());
    let repository = MultipartRepository::new(fixture.store.clone());
    let initial = session(&fixture);
    repository.begin(&initial, 100).await.unwrap();
    let owner = FileIdentity {
        file: FileId::random(),
        ..initial.owner
    };
    let mut writer = FileTreeWriter::new(blocks.clone(), owner, 8).unwrap();
    writer.push(b"0123456789").await.unwrap();
    let part = MultipartPart {
        upload: initial.upload,
        number: 1,
        revision: 1,
        modified_ms: 101,
        owner,
        tree: Some(writer.finish().await.unwrap()),
        stream: None,
    };
    repository.reserve_part(&initial, &part, 101).await.unwrap();
    let pending = repository
        .load(initial.context, initial.upload)
        .await
        .unwrap()
        .unwrap();
    repository.settle_part(&pending).await.unwrap();
    let current = repository
        .load(initial.context, initial.upload)
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
        .freeze_completion(&current, &selection, 102)
        .await
        .unwrap();
    let mutations = fixture.store.writes.load(Ordering::SeqCst);
    let physical_reads = blocks.reads.load(Ordering::SeqCst);
    let physical_writes = blocks.writes.load(Ordering::SeqCst);
    let recovery = MultipartRecovery::new(fixture.store.clone());
    for _ in 0..3 {
        let report = recovery.recover_page(initial.context, None, 103).await.unwrap();
        assert_eq!((report.progressed, report.retained), (0, 1));
        assert!(report.failures.is_empty());
        let current = repository
            .load(initial.context, initial.upload)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(current.completion.unwrap().progress.completed_bytes, 0);
    }
    assert_eq!(fixture.store.writes.load(Ordering::SeqCst), mutations);
    assert_eq!(blocks.reads.load(Ordering::SeqCst), physical_reads);
    assert_eq!(blocks.writes.load(Ordering::SeqCst), physical_writes);
    let report = recovery.recover_page(initial.context, None, 1100).await.unwrap();
    assert_eq!(report.progressed, 1);
    assert_eq!(
        repository
            .load(initial.context, initial.upload)
            .await
            .unwrap()
            .unwrap()
            .phase,
        MultipartPhase::Aborted
    );
}

#[tokio::test]
async fn recovery_rejects_corrupt_pages_foreign_cursors_and_retired_contexts_before_work() {
    let fixture = file::TestFile::new(common::TestStore::default()).await;
    let repository = MultipartRepository::new(fixture.store.clone());
    let mut sessions = Vec::new();
    for _ in 0..5 {
        let initial = session(&fixture);
        repository.begin(&initial, 100).await.unwrap();
        sessions.push(initial);
    }
    let recovery = MultipartRecovery::new(fixture.store.clone());
    let report = recovery.recover_page(fixture.context, None, 101).await.unwrap();
    assert_eq!(report.retained, 4);
    let cursor = report.continuation.unwrap();
    assert!(MultipartRecoveryScan {
        catalog: CatalogId::random(),
        continuation: Some(cursor.clone())
    }
    .request()
    .is_err());
    let mut invalid = cursor;
    invalid.catalog_generation = 0;
    assert!(recovery
        .recover_page(fixture.context, Some(invalid), 101)
        .await
        .is_err());
    sessions.sort_by_key(|session| session.key().encode().unwrap());
    let key = sessions[0].key().encode().unwrap();
    let before = fixture.store.get(&key).await.unwrap().unwrap();
    fixture
        .store
        .compare_exchange(
            &key,
            Some(&before.bytes),
            b"bad",
            mutation_identity(&key, Some(&before.bytes), b"bad"),
        )
        .await
        .unwrap();
    let writes = fixture.store.writes.load(Ordering::SeqCst);
    assert!(recovery.recover_page(fixture.context, None, 1100).await.is_err());
    assert_eq!(fixture.store.writes.load(Ordering::SeqCst), writes);
    fixture.root(fixture.context, RootState::Fencing).await;
    assert!(recovery.recover_page(fixture.context, None, 1100).await.is_err());
}

#[test]
fn session_cursor_rejects_payload_keys_and_limits_its_scan() {
    let initial = fixtures::session();
    let mut completion = fixtures::completion(&initial);
    completion.selection.operation = OperationId::random();
    let request = MultipartRecoveryScan {
        catalog: initial.context.catalog,
        continuation: None,
    }
    .request()
    .unwrap();
    assert_eq!(request.max_items, 4);
    let cursor = crowdb_chunk_kv_client::MultiScanContinuation {
        original_start: request.start,
        original_end: request.end,
        direction: request.direction,
        last_key: completion.selection.page_key(0).unwrap().encode().unwrap(),
        catalog_generation: 1,
    };
    assert!(MultipartRecoveryScan {
        catalog: initial.context.catalog,
        continuation: Some(cursor)
    }
    .request()
    .is_err());
}
