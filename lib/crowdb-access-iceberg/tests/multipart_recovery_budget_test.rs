#[path = "common/file_blocks.rs"]
mod blocks;
#[path = "common/store.rs"]
mod common;
#[path = "common/file.rs"]
mod file;
#[path = "common/multipart_recovery_store.rs"]
mod scan;

use std::sync::{atomic::Ordering, Arc};
use std::time::Duration;

use crowdb_access_iceberg::file::{
    FileIdentity, FileTreeWriter, MultipartLimits, MultipartPart, MultipartPhase, MultipartRecovery,
    MultipartRepository, MultipartSelection, MultipartSession, SelectedPart,
};
use crowdb_access_iceberg::key::{FileId, OperationId};

fn session(fixture: &file::TestFile, identity: u8, ttl_ms: u64) -> MultipartSession {
    let record = fixture.record(&format!("{identity}.json"), b"{}");
    MultipartSession {
        context: fixture.context,
        upload: OperationId::from_bytes(&[identity; 16]).unwrap(),
        owner: FileIdentity {
            table: fixture.table,
            file: record.file,
        },
        location: record.location,
        principal: [1; 32],
        revision: 1,
        created_ms: 100,
        expires_ms: 100 + ttl_ms,
        limits: MultipartLimits {
            max_parts: 1,
            max_part_bytes: 100,
            max_file_bytes: 100,
            max_staged_bytes: 100,
            ttl_ms,
        },
        phase: MultipartPhase::Open,
        part_count: 0,
        staged_bytes: 0,
        completion: None,
        published: None,
        pending: None,
        credit: None,
    }
}

#[tokio::test]
async fn slow_first_session_does_not_starve_later_expiry_or_advance_unfinished_bytes() {
    let fixture = file::TestFile::new(common::TestStore::default()).await;
    let blocks = Arc::new(blocks::TestBlocks::default());
    let repository = MultipartRepository::new(fixture.store.clone());
    let mut first = session(&fixture, 1, 1000);
    let second = session(&fixture, 2, 50);
    repository.begin(&first, 100).await.unwrap();
    repository.begin(&second, 100).await.unwrap();
    let owner = FileIdentity {
        file: FileId::random(),
        ..first.owner
    };
    let mut writer = FileTreeWriter::new(blocks.clone(), owner, 8).unwrap();
    writer.push(b"{}").await.unwrap();
    let part = MultipartPart {
        upload: first.upload,
        number: 1,
        revision: 1,
        owner,
        tree: writer.finish().await.unwrap(),
    };
    repository.reserve_part(&first, &part, 101).await.unwrap();
    first = repository
        .load(first.context, first.upload)
        .await
        .unwrap()
        .unwrap();
    repository.settle_part(&first).await.unwrap();
    first = repository
        .load(first.context, first.upload)
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
        .freeze_completion(&first, &selection, 102)
        .await
        .unwrap();
    first = repository
        .load(first.context, first.upload)
        .await
        .unwrap()
        .unwrap();
    blocks.pause_reads.store(true, Ordering::SeqCst);
    let recovery = MultipartRecovery::new(fixture.store.clone(), blocks.clone(), 8, 8)
        .unwrap()
        .with_session_timeout(Duration::from_millis(20))
        .unwrap();
    let report = tokio::time::timeout(
        Duration::from_secs(1),
        recovery.recover_page(first.context, None, 200),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!((report.deferred, report.progressed), (1, 1));
    assert!(report.failures.is_empty());
    assert_eq!(
        repository
            .load(first.context, first.upload)
            .await
            .unwrap()
            .unwrap(),
        first
    );
    assert_eq!(
        repository
            .load(second.context, second.upload)
            .await
            .unwrap()
            .unwrap()
            .phase,
        MultipartPhase::Aborted
    );
    blocks.pause_reads.store(false, Ordering::SeqCst);
    let report = recovery.recover_page(first.context, None, 200).await.unwrap();
    assert_eq!((report.deferred, report.progressed, report.retained), (0, 1, 1));
    assert!(report.failures.is_empty());
    assert_eq!(
        repository
            .load(first.context, first.upload)
            .await
            .unwrap()
            .unwrap()
            .completion
            .unwrap()
            .progress
            .completed_bytes,
        2
    );
}

#[test]
fn recovery_session_timeout_rejects_missing_and_unbounded_deadlines() {
    for timeout in [Duration::ZERO, Duration::from_secs(61)] {
        let recovery = MultipartRecovery::new(
            Arc::new(common::TestStore::default()),
            Arc::new(blocks::TestBlocks::default()),
            8,
            8,
        )
        .unwrap();
        assert!(recovery.with_session_timeout(timeout).is_err());
    }
}
