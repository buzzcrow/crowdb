#[path = "common/store.rs"]
mod common;
#[path = "common/file.rs"]
mod fixture;

use std::sync::{atomic::Ordering, Arc};

use crowdb_access_iceberg::catalog::{CatalogContext, CatalogError, CatalogStore, RootState};
use crowdb_access_iceberg::file::{file_key, location_key, FileContent, FileRepository};
use crowdb_access_iceberg::key::CatalogId;
use crowdb_access_iceberg::operation::mutation_identity;
use crowdb_protocol::chunkdb::rpc::Location;
use crowdb_protocol::common::ChunkId;
use fixture::TestFile;

#[tokio::test]
async fn immutable_publication_replays_equal_bytes_without_overwriting_or_extra_writes() {
    let fixture = TestFile::new(common::TestStore::default()).await;
    let repository = FileRepository::new(fixture.store.clone());
    let candidate = fixture.record("metadata/one.json", b"{}");
    assert!(repository
        .load(fixture.context, &candidate.location)
        .await
        .unwrap()
        .is_none());
    assert_eq!(
        repository.publish(fixture.context, &candidate).await.unwrap(),
        candidate
    );
    let writes = fixture.store.writes.load(Ordering::SeqCst);
    let equal = fixture.record("metadata/one.json", b"{}");
    assert_ne!(equal.file, candidate.file);
    assert_eq!(
        repository.publish(fixture.context, &equal).await.unwrap(),
        candidate
    );
    let different = fixture.record("metadata/one.json", b"{\"changed\":true}");
    assert!(matches!(
        repository.publish(fixture.context, &different).await,
        Err(CatalogError::Conflict)
    ));
    assert_eq!(fixture.store.writes.load(Ordering::SeqCst), writes);
    assert_eq!(
        repository
            .load(fixture.context, &candidate.location)
            .await
            .unwrap(),
        Some(candidate)
    );
}

#[tokio::test]
async fn streamed_publication_replays_equal_etag_with_different_chunk_locations() {
    let fixture = TestFile::new(common::TestStore::default()).await;
    let repository = FileRepository::new(fixture.store.clone());
    let make = |chunk_low, etag: &str| {
        let mut record = fixture.record("metadata/stream.json", b"{}");
        record.digest = [0; 32];
        record.content = FileContent::from_locations(
            &[Location {
                chunk_id: Some(ChunkId {
                    high: 7,
                    low: chunk_low,
                }),
                offset: 0,
                length: 36,
                logical_offset: 0,
                logical_length: 2,
            }],
            2,
            etag.to_owned(),
        )
        .unwrap();
        record
    };
    let first = make(1, "99914b932bd37a50b983c5e7c90ae93b");
    let replay = make(2, "99914b932bd37a50b983c5e7c90ae93b");
    let changed = make(3, "f111cdacaa915d85831037cae3622d59");
    assert_eq!(repository.publish(fixture.context, &first).await.unwrap(), first);
    let writes = fixture.store.writes.load(Ordering::SeqCst);
    assert_eq!(repository.publish(fixture.context, &replay).await.unwrap(), first);
    assert!(matches!(
        repository.publish(fixture.context, &changed).await,
        Err(CatalogError::Conflict)
    ));
    assert_eq!(fixture.store.writes.load(Ordering::SeqCst), writes);
}

#[tokio::test]
async fn every_lost_file_publication_reply_recovers_on_another_repository() {
    for lost in 1..=2 {
        let fixture = TestFile::new(common::TestStore::default()).await;
        let candidate = fixture.record("metadata/one.json", &vec![b' '; 64 * 1024]);
        fixture.store.fail_after.store(
            fixture.store.writes.load(Ordering::SeqCst) + lost,
            Ordering::SeqCst,
        );
        assert!(FileRepository::new(fixture.store.clone())
            .publish(fixture.context, &candidate)
            .await
            .is_err());
        let recovery = FileRepository::new(fixture.store.clone());
        assert_eq!(
            recovery
                .load(fixture.context, &candidate.location)
                .await
                .unwrap()
                .is_some(),
            lost == 2
        );
        assert_eq!(
            recovery.publish(fixture.context, &candidate).await.unwrap(),
            candidate
        );
        assert_eq!(
            recovery.load(fixture.context, &candidate.location).await.unwrap(),
            Some(candidate)
        );
    }
}

#[tokio::test]
async fn competing_candidates_select_one_file_and_retain_orphan_authority() {
    for equal in [false, true] {
        let fixture = TestFile::new(common::TestStore {
            file_mapping_barrier: Some(Arc::new(tokio::sync::Barrier::new(2))),
            ..common::TestStore::default()
        })
        .await;
        let first = fixture.record("metadata/one.json", b"{}");
        let second = fixture.record("metadata/one.json", if equal { b"{}" } else { b"[]" });
        let first_repository = FileRepository::new(fixture.store.clone());
        let second_repository = FileRepository::new(fixture.store.clone());
        let (first_result, second_result) = tokio::join!(
            first_repository.publish(fixture.context, &first),
            second_repository.publish(fixture.context, &second),
        );
        if equal {
            assert_eq!(first_result.unwrap(), second_result.unwrap());
        } else {
            assert_ne!(first_result.is_ok(), second_result.is_ok());
            assert!(matches!(
                first_result.err().or(second_result.err()),
                Some(CatalogError::Conflict)
            ));
        }
        for candidate in [&first, &second] {
            assert!(fixture
                .store
                .get(
                    &file_key(fixture.context.catalog, candidate.file)
                        .encode()
                        .unwrap()
                )
                .await
                .unwrap()
                .is_some());
        }
    }
}

#[tokio::test]
async fn file_identity_collisions_corrupt_bindings_and_retired_contexts_fail_closed() {
    let fixture = TestFile::new(common::TestStore::default()).await;
    let repository = FileRepository::new(fixture.store.clone());
    let candidate = fixture.record("metadata/one.json", b"{}");
    repository.publish(fixture.context, &candidate).await.unwrap();
    let mut collision = candidate.clone();
    collision.location = fixture.table.file("metadata/two.json").unwrap();
    assert!(matches!(
        repository.publish(fixture.context, &collision).await,
        Err(CatalogError::Conflict)
    ));
    let key = location_key(&candidate.location).encode().unwrap();
    let previous = fixture.store.get(&key).await.unwrap().unwrap();
    fixture
        .store
        .compare_exchange(
            &key,
            Some(&previous.bytes),
            b"bad",
            mutation_identity(&key, Some(&previous.bytes), b"bad"),
        )
        .await
        .unwrap();
    assert!(repository
        .load(fixture.context, &candidate.location)
        .await
        .is_err());
    assert!(repository.publish(fixture.context, &candidate).await.is_err());
    fixture.root(fixture.context, RootState::Fencing).await;
    assert!(matches!(
        repository.load(fixture.context, &candidate.location).await,
        Err(CatalogError::Busy)
    ));
    fixture
        .root(
            CatalogContext {
                catalog: CatalogId::random(),
                activation_epoch: 2,
            },
            RootState::Ready,
        )
        .await;
    assert!(matches!(
        repository.publish(fixture.context, &collision).await,
        Err(CatalogError::Conflict)
    ));
}
