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

use crowdb_access_iceberg::catalog::StoredValue;
use crowdb_access_iceberg::file::{
    MultipartCredit, MultipartPhase, MultipartRecovery, MultipartRepository, MultipartSession,
};
use crowdb_access_iceberg::key::{CatalogScope, IcebergKey, OperationId};
use crowdb_access_iceberg::record::StorageRecord;
use std::sync::{atomic::Ordering, Arc};

async fn setup() -> (file::TestFile, MultipartSession) {
    let fixture = file::TestFile::new(common::TestStore::default()).await;
    let mut session = fixtures::session();
    session.context = fixture.context;
    session.owner.table = fixture.table;
    session.owner.file = fixture.record("file", b"{}").file;
    session.location = fixture.table.file("file").unwrap();
    (fixture, session)
}
fn legacy_credit() -> MultipartCredit {
    MultipartCredit {
        policy: OperationId::random(),
        sequence: 2,
        released: false,
    }
}
fn poison_policy(fixture: &file::TestFile) -> Vec<u8> {
    let key = IcebergKey::Catalog {
        catalog: fixture.context.catalog,
        scope: CatalogScope::MultipartAdmission,
        suffix: Vec::new(),
    }
    .encode()
    .unwrap();
    let mut values = (**fixture.store.values.load()).clone();
    values.insert(
        key.clone(),
        StoredValue {
            bytes: b"retired policy must never be decoded".to_vec(),
            revision: 1,
        },
    );
    fixture.store.values.store(Arc::new(values));
    key
}
fn install_session(fixture: &file::TestFile, session: &MultipartSession) {
    let mut values = (**fixture.store.values.load()).clone();
    values.insert(
        session.key().encode().unwrap(),
        StoredValue {
            bytes: StorageRecord::MultipartSession(Box::new(session.clone()))
                .encode()
                .unwrap(),
            revision: session.revision,
        },
    );
    fixture.store.values.store(Arc::new(values));
}
fn recovery(fixture: &file::TestFile) -> MultipartRecovery {
    MultipartRecovery::new(
        fixture.store.clone(),
        Arc::new(blocks::TestBlocks::default()),
        8,
        32,
    )
    .unwrap()
}

#[tokio::test]
async fn empty_sweeps_only_check_context_and_never_read_or_recreate_a_legacy_policy() {
    let (fixture, session) = setup().await;
    poison_policy(&fixture);
    let before = fixture.store.values.load_full();
    let reads = fixture.store.reads.load(Ordering::SeqCst);
    let writes = fixture.store.writes.load(Ordering::SeqCst);
    let report = recovery(&fixture)
        .recover_page(fixture.context, None, session.expires_ms)
        .await
        .unwrap();
    assert_eq!((report.progressed, report.retained, report.deferred), (0, 0, 0));
    assert!(report.failures.is_empty());
    assert_eq!(fixture.store.reads.load(Ordering::SeqCst) - reads, 2);
    assert_eq!(fixture.store.writes.load(Ordering::SeqCst), writes);
    assert_eq!(*fixture.store.values.load_full(), *before);
    assert_eq!(fixture.store.legacy_admission_calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn a_pending_legacy_reservation_cannot_recreate_an_upload_from_its_journal() {
    use crowdb_access_iceberg::file::{
        MultipartAdmissionLimits, MultipartAdmissionRecord, MultipartCreditAction, MultipartCreditMutation,
    };
    use crowdb_access_iceberg::operation::PayloadStore;
    let (fixture, mut session) = setup().await;
    let credit = legacy_credit();
    session.credit = Some(credit);
    let after = PayloadStore::new(fixture.store.clone())
        .put(
            fixture.context.catalog,
            session.upload,
            &StorageRecord::MultipartSession(Box::new(session.clone()))
                .encode()
                .unwrap(),
        )
        .await
        .unwrap();
    let record = MultipartAdmissionRecord {
        context: fixture.context,
        policy: credit.policy,
        revision: credit.sequence,
        limits: MultipartAdmissionLimits {
            max_sessions: 2,
            max_reserved_bytes: 3000,
        },
        sessions: 1,
        reserved_bytes: session.limits.max_staged_bytes,
        pending: Some(MultipartCreditMutation {
            action: MultipartCreditAction::Reserve,
            upload: session.upload,
            reservation_bytes: session.limits.max_staged_bytes,
            before: None,
            after,
        }),
    };
    let mut values = (**fixture.store.values.load()).clone();
    values.insert(
        record.key().encode().unwrap(),
        StoredValue {
            bytes: StorageRecord::MultipartAdmission(Box::new(record))
                .encode()
                .unwrap(),
            revision: 2,
        },
    );
    fixture.store.values.store(Arc::new(values));
    let before = fixture.store.values.load_full();
    let report = recovery(&fixture)
        .recover_page(fixture.context, None, session.expires_ms)
        .await
        .unwrap();
    assert_eq!((report.progressed, report.retained, report.deferred), (0, 0, 0));
    assert!(report.failures.is_empty());
    assert_eq!(*fixture.store.values.load_full(), *before);
    assert_eq!(fixture.store.legacy_admission_calls.load(Ordering::SeqCst), 0);
    assert!(!fixture
        .store
        .values
        .load()
        .contains_key(&session.key().encode().unwrap()));
}

#[tokio::test]
async fn legacy_open_upload_expires_without_global_credits_and_terminal_sweeps_do_not_write() {
    let (fixture, mut session) = setup().await;
    session.credit = Some(legacy_credit());
    install_session(&fixture, &session);
    let policy = poison_policy(&fixture);
    let before = fixture.store.values.load().get(&policy).unwrap().clone();
    let report = recovery(&fixture)
        .recover_page(fixture.context, None, session.expires_ms)
        .await
        .unwrap();
    assert_eq!(report.progressed, 1);
    assert!(report.failures.is_empty());
    let terminal = MultipartRepository::new(fixture.store.clone())
        .load(fixture.context, session.upload)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(terminal.phase, MultipartPhase::Aborted);
    assert_eq!(terminal.credit, session.credit);
    assert_eq!(terminal.owner, session.owner);
    let writes = fixture.store.writes.load(Ordering::SeqCst);
    let reads = fixture.store.reads.load(Ordering::SeqCst);
    let report = recovery(&fixture)
        .recover_page(fixture.context, None, session.expires_ms)
        .await
        .unwrap();
    assert_eq!((report.progressed, report.retained, report.deferred), (0, 1, 0));
    assert!(report.failures.is_empty());
    assert_eq!(fixture.store.writes.load(Ordering::SeqCst), writes);
    assert_eq!(fixture.store.reads.load(Ordering::SeqCst) - reads, 2);
    assert_eq!(fixture.store.legacy_admission_calls.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.store.values.load().get(&policy), Some(&before));
}

#[tokio::test]
async fn published_legacy_resources_remain_unchanged_even_when_the_credit_policy_is_missing() {
    let (fixture, mut session) = setup().await;
    let files = crowdb_access_iceberg::file::FileRepository::new(fixture.store.clone());
    let record = files
        .publish(fixture.context, &fixture.record("file", b"{}"))
        .await
        .unwrap();
    session.owner.file = record.file;
    session.credit = Some(legacy_credit());
    session.phase = MultipartPhase::Published;
    session.part_count = 1;
    session.completion = Some(fixtures::completion(&session));
    let completion = session.completion.as_mut().unwrap();
    completion.progress.next_part = 1;
    completion.progress.completed_bytes = record.length;
    completion.publication = Some(completion.selection.clone());
    session.published = Some(session.owner.file);
    install_session(&fixture, &session);
    let before = fixture.store.values.load_full();
    let report = recovery(&fixture)
        .recover_page(fixture.context, None, session.expires_ms)
        .await
        .unwrap();
    assert_eq!((report.progressed, report.retained, report.deferred), (0, 1, 0));
    assert!(report.failures.is_empty());
    assert_eq!(*fixture.store.values.load_full(), *before);
    assert_eq!(fixture.store.legacy_admission_calls.load(Ordering::SeqCst), 0);
    assert_eq!(
        files.load(fixture.context, &record.location).await.unwrap(),
        Some(record)
    );
}

#[tokio::test]
async fn new_uploads_cannot_request_legacy_global_credit_state() {
    let (fixture, mut session) = setup().await;
    session.credit = Some(legacy_credit());
    let writes = fixture.store.writes.load(Ordering::SeqCst);
    assert!(MultipartRepository::new(fixture.store.clone())
        .begin(&session, 100)
        .await
        .is_err());
    assert_eq!(fixture.store.writes.load(Ordering::SeqCst), writes);
}

#[test]
fn released_receipts_are_valid_only_on_terminal_session_records() {
    use crowdb_access_iceberg::file::MultipartCredit;
    use crowdb_access_iceberg::record::StorageRecord;
    let mut initial = fixtures::session();
    initial.credit = Some(MultipartCredit {
        policy: OperationId::random(),
        sequence: 2,
        released: true,
    });
    assert!(initial.validate().is_err());
    initial.phase = MultipartPhase::Completing;
    initial.part_count = 1;
    initial.completion = Some(fixtures::completion(&initial));
    assert!(initial.validate().is_err());
    initial.phase = MultipartPhase::Aborted;
    let record = StorageRecord::MultipartSession(Box::new(initial.clone()));
    assert_eq!(
        StorageRecord::decode(&initial.key(), &record.encode().unwrap()).unwrap(),
        record
    );
    initial.credit.as_mut().unwrap().sequence = 0;
    assert!(initial.validate().is_err());
}
