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

use crowdb_access_iceberg::catalog::CatalogError;
use crowdb_access_iceberg::file::{
    MultipartAdmission, MultipartAdmissionLimits, MultipartAdmissionRecord, MultipartPhase,
    MultipartRecovery, MultipartRepository, MultipartSession,
};
use crowdb_access_iceberg::key::OperationId;
use std::sync::{atomic::Ordering, Arc};

async fn setup() -> (file::TestFile, MultipartSession, MultipartAdmissionRecord) {
    let fixture = file::TestFile::new(common::TestStore::default()).await;
    let mut initial = fixtures::session();
    initial.context = fixture.context;
    initial.owner.table = fixture.table;
    initial.owner.file = fixture.record("file", b"{}").file;
    initial.location = fixture.table.file("file").unwrap();
    let record = MultipartAdmission::new(fixture.store.clone())
        .initialize(
            fixture.context,
            MultipartAdmissionLimits {
                max_sessions: 2,
                max_reserved_bytes: 3000,
            },
        )
        .await
        .unwrap();
    (fixture, initial, record)
}

async fn policy(admission: &MultipartAdmission, initial: &MultipartSession) -> MultipartAdmissionRecord {
    admission.load(initial.context).await.unwrap().unwrap()
}

async fn session(repository: &MultipartRepository, initial: &MultipartSession) -> MultipartSession {
    repository
        .load(initial.context, initial.upload)
        .await
        .unwrap()
        .unwrap()
}

#[tokio::test]
async fn reservation_reply_loss_recovers_once_across_instances_at_every_write() {
    for lost in 1..=4 {
        let (fixture, initial, record) = setup().await;
        let admission = MultipartAdmission::new(fixture.store.clone());
        fixture.store.fail_after.store(
            fixture.store.writes.load(Ordering::SeqCst) + lost,
            Ordering::SeqCst,
        );
        assert!(admission.reserve(&record, &initial, 100).await.is_err());
        fixture.store.fail_after.store(0, Ordering::SeqCst);
        let admission = MultipartAdmission::new(fixture.store.clone());
        let record = policy(&admission, &initial).await;
        if record.pending.is_some() {
            assert!(admission.settle(&record).await.unwrap());
        }
        let record = policy(&admission, &initial).await;
        assert!(admission.reserve(&record, &initial, 100).await.unwrap());
        let record = policy(&admission, &initial).await;
        assert_eq!((record.sessions, record.reserved_bytes), (1, 1500));
        assert!(record.pending.is_none());
        let repository = MultipartRepository::new(fixture.store.clone());
        let admitted = session(&repository, &initial).await;
        assert_eq!(admitted.credit.unwrap().policy, record.policy);
        assert!(!admitted.credit.unwrap().released);
        let writes = fixture.store.writes.load(Ordering::SeqCst);
        assert!(admission.reserve(&record, &initial, 5000).await.unwrap());
        assert_eq!(fixture.store.writes.load(Ordering::SeqCst), writes);
    }
}

#[tokio::test]
async fn terminal_release_reply_loss_never_returns_credits_twice() {
    for lost in 1..=5 {
        let (fixture, initial, record) = setup().await;
        let admission = MultipartAdmission::new(fixture.store.clone());
        let repository = MultipartRepository::new(fixture.store.clone());
        assert!(admission.reserve(&record, &initial, 100).await.unwrap());
        let active = session(&repository, &initial).await;
        let record = policy(&admission, &initial).await;
        assert!(matches!(
            admission.release(&record, &active).await,
            Err(CatalogError::Conflict)
        ));
        assert!(repository.abort(&active).await.unwrap());
        let terminal = session(&repository, &initial).await;
        fixture.store.fail_after.store(
            fixture.store.writes.load(Ordering::SeqCst) + lost,
            Ordering::SeqCst,
        );
        assert!(admission.release(&record, &terminal).await.is_err());
        fixture.store.fail_after.store(0, Ordering::SeqCst);
        let admission = MultipartAdmission::new(fixture.store.clone());
        let current = policy(&admission, &initial).await;
        if current.pending.is_some() {
            assert!(admission.settle(&current).await.unwrap());
        }
        let current = policy(&admission, &initial).await;
        assert!(admission.release(&current, &terminal).await.unwrap());
        let current = policy(&admission, &initial).await;
        assert_eq!((current.sessions, current.reserved_bytes), (0, 0));
        let retained = session(&repository, &initial).await;
        assert!(retained.credit.unwrap().released);
        let writes = fixture.store.writes.load(Ordering::SeqCst);
        assert!(admission.release(&current, &terminal).await.unwrap());
        assert!(admission.release(&current, &retained).await.unwrap());
        assert_eq!(fixture.store.writes.load(Ordering::SeqCst), writes);
    }
}

#[tokio::test]
async fn admission_fences_competing_reservations_and_persisted_policy_changes() {
    let (fixture, initial, record) = setup().await;
    let first = MultipartAdmission::new(fixture.store.clone());
    let second = MultipartAdmission::new(fixture.store.clone());
    let mut other = initial.clone();
    other.upload = OperationId::random();
    let (left, right) = tokio::join!(
        first.reserve(&record, &initial, 100),
        second.reserve(&record, &other, 100)
    );
    assert_ne!(left.unwrap(), right.unwrap());
    let current = policy(&first, &initial).await;
    assert_eq!(current.sessions, 1);
    assert_eq!(
        first.initialize(initial.context, record.limits).await.unwrap(),
        current
    );
    assert!(matches!(
        first
            .initialize(
                initial.context,
                MultipartAdmissionLimits {
                    max_sessions: 3,
                    ..record.limits
                }
            )
            .await,
        Err(CatalogError::Conflict)
    ));
    let repository = MultipartRepository::new(fixture.store.clone());
    let missing = if repository
        .load(initial.context, initial.upload)
        .await
        .unwrap()
        .is_none()
    {
        &initial
    } else {
        &other
    };
    assert!(first.reserve(&current, missing, 100).await.unwrap());
    let mut overflow = initial.clone();
    overflow.upload = OperationId::random();
    assert!(first
        .reserve(&policy(&first, &initial).await, &overflow, 100)
        .await
        .is_err());
    assert!(repository
        .load(initial.context, overflow.upload)
        .await
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn sweep_recovers_precreation_credit_then_expires_and_releases_without_deletion() {
    let (fixture, initial, record) = setup().await;
    let admission = MultipartAdmission::new(fixture.store.clone());
    fixture
        .store
        .fail_after
        .store(fixture.store.writes.load(Ordering::SeqCst) + 2, Ordering::SeqCst);
    assert!(admission.reserve(&record, &initial, 100).await.is_err());
    fixture.store.fail_after.store(0, Ordering::SeqCst);
    let repository = MultipartRepository::new(fixture.store.clone());
    assert!(repository
        .load(initial.context, initial.upload)
        .await
        .unwrap()
        .is_none());
    let recovery = MultipartRecovery::new(
        fixture.store.clone(),
        Arc::new(blocks::TestBlocks::default()),
        8,
        32,
    )
    .unwrap();
    let first = recovery
        .recover_page(initial.context, None, initial.expires_ms)
        .await
        .unwrap();
    assert_eq!(first.progressed, 2);
    assert!(first.failures.is_empty());
    assert_eq!(
        session(&repository, &initial).await.phase,
        MultipartPhase::Aborted
    );
    let second = recovery
        .recover_page(initial.context, None, initial.expires_ms)
        .await
        .unwrap();
    assert_eq!(second.progressed, 1);
    assert!(second.failures.is_empty());
    assert!(session(&repository, &initial).await.credit.unwrap().released);
    let current = policy(&admission, &initial).await;
    assert_eq!((current.sessions, current.reserved_bytes), (0, 0));
    assert_eq!(
        recovery
            .recover_page(initial.context, None, initial.expires_ms)
            .await
            .unwrap()
            .retained,
        1
    );
}

#[tokio::test]
async fn superseded_helpers_cannot_recreate_released_sessions_or_change_new_credits() {
    let (fixture, initial, record) = setup().await;
    let admission = MultipartAdmission::new(fixture.store.clone());
    fixture
        .store
        .fail_after
        .store(fixture.store.writes.load(Ordering::SeqCst) + 2, Ordering::SeqCst);
    assert!(admission.reserve(&record, &initial, 100).await.is_err());
    fixture.store.fail_after.store(0, Ordering::SeqCst);
    let reserved = policy(&admission, &initial).await;
    assert!(admission.settle(&reserved).await.unwrap());
    let repository = MultipartRepository::new(fixture.store.clone());
    assert!(repository
        .abort(&session(&repository, &initial).await)
        .await
        .unwrap());
    let terminal = session(&repository, &initial).await;
    let current = policy(&admission, &initial).await;
    fixture
        .store
        .fail_after
        .store(fixture.store.writes.load(Ordering::SeqCst) + 3, Ordering::SeqCst);
    assert!(admission.release(&current, &terminal).await.is_err());
    fixture.store.fail_after.store(0, Ordering::SeqCst);
    let releasing = policy(&admission, &initial).await;
    assert!(admission.settle(&releasing).await.unwrap());
    let mut other = initial.clone();
    other.upload = OperationId::random();
    let current = policy(&admission, &initial).await;
    assert!(admission.reserve(&current, &other, 100).await.unwrap());
    let current = policy(&admission, &initial).await;
    let writes = fixture.store.writes.load(Ordering::SeqCst);
    assert!(!admission.settle(&reserved).await.unwrap());
    assert!(!admission.settle(&releasing).await.unwrap());
    assert!(admission.release(&current, &terminal).await.unwrap());
    assert_eq!(policy(&admission, &initial).await, current);
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
