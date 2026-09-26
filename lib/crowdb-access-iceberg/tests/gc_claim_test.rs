use crowdb_access_iceberg::{
    catalog::CatalogStore,
    gc::{CandidatePhase, GcCandidate, GcRepository, TreeReclaimCursor},
    key::OperationId,
    record::StorageRecord,
};
use std::sync::atomic::Ordering;

mod common {
    pub mod store;
    pub use store::TestStore;
    pub mod file;
    pub mod gc_store;
}

async fn candidate() -> (common::file::TestFile, GcCandidate) {
    let fixture = common::file::TestFile::new(common::TestStore::default()).await;
    let file = fixture.record("metadata/orphan.json", b"{}");
    let candidate = GcCandidate {
        assembly: None,
        next_root: 0,
        task: OperationId::random(),
        generation: 7,
        first_seen_ms: 1000,
        not_before_ms: 2000,
        revision: 1,
        phase: CandidatePhase::Retained,
        completed_round: 0,
        cursor: TreeReclaimCursor::new(&file).unwrap(),
        file,
        part: None,
    };
    (fixture, candidate)
}

#[tokio::test]
async fn generations_share_one_claim_without_resetting_progress_or_retention() {
    let (fixture, first) = candidate().await;
    let repository = GcRepository::new(fixture.store.clone());
    repository.claim_candidate(&first).await.unwrap();
    let mut advanced = first.clone();
    advanced.phase = CandidatePhase::Deleting;
    advanced.revision += 1;
    repository.candidate(Some(&first), &advanced).await.unwrap();
    let mut second = first.clone();
    second.task = OperationId::random();
    second.generation += 1;
    second.first_seen_ms = 3000;
    second.not_before_ms = 4000;
    assert_eq!(repository.claim_candidate(&second).await.unwrap(), advanced);
    assert!(fixture
        .store
        .get(&second.key().encode().unwrap())
        .await
        .unwrap()
        .is_none());
    let claim = fixture
        .store
        .get(&first.claim_key().encode().unwrap())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        StorageRecord::decode(&first.claim_key(), &claim.bytes).unwrap(),
        StorageRecord::GcCandidate(Box::new(first))
    );
}

#[tokio::test]
async fn lost_claim_and_candidate_replies_resume_the_same_generation() {
    for failed_write in [1, 2] {
        let (fixture, first) = candidate().await;
        let repository = GcRepository::new(fixture.store.clone());
        fixture.store.fail_after.store(
            fixture.store.writes.load(Ordering::Relaxed) + failed_write,
            Ordering::Relaxed,
        );
        assert!(repository.claim_candidate(&first).await.is_err());
        fixture.store.fail_after.store(0, Ordering::Relaxed);
        let mut second = first.clone();
        second.task = OperationId::random();
        second.generation += 1;
        assert_eq!(repository.claim_candidate(&second).await.unwrap(), first);
        assert!(fixture
            .store
            .get(&second.key().encode().unwrap())
            .await
            .unwrap()
            .is_none());
    }
}

#[tokio::test]
async fn claims_reject_changed_file_authority_and_mutable_progress_as_a_claim() {
    let (fixture, first) = candidate().await;
    let repository = GcRepository::new(fixture.store.clone());
    repository.claim_candidate(&first).await.unwrap();
    let mut changed = first.clone();
    changed.file.location = fixture.table.file("metadata/different.json").unwrap();
    assert!(repository.claim_candidate(&changed).await.is_err());
    let mut advanced = first.clone();
    advanced.phase = CandidatePhase::Deleting;
    advanced.revision += 1;
    let bytes = StorageRecord::GcCandidate(Box::new(advanced)).encode().unwrap();
    assert!(StorageRecord::decode(&first.claim_key(), &bytes).is_err());
}
