#[path = "common/store.rs"]
mod common;
#[path = "common/file.rs"]
mod file;
#[path = "common/multipart.rs"]
mod fixtures;
#[path = "common/multipart_list_race.rs"]
mod race;
#[path = "common/multipart_recovery_store.rs"]
mod scan;

use std::sync::atomic::Ordering;

use crowdb_access_iceberg::catalog::{CatalogError, CatalogStore, RootState, StoredValue};
use crowdb_access_iceberg::file::{
    FileIdentity, FileTree, MultipartLister, MultipartPart, MultipartPartScan, MultipartPhase,
    MultipartSession,
};
use crowdb_access_iceberg::key::{FileId, OperationId};
use crowdb_access_iceberg::operation::mutation_identity;
use crowdb_access_iceberg::record::StorageRecord;
use sha2::{Digest, Sha256};

async fn setup() -> (file::TestFile, MultipartSession, Vec<MultipartPart>) {
    let fixture = file::TestFile::new(common::TestStore::default()).await;
    let mut session = fixtures::session();
    session.context = fixture.context;
    session.owner = FileIdentity {
        table: fixture.table,
        file: fixture.record("file", b"{}").file,
    };
    session.location = fixture.table.file("file").unwrap();
    session.limits.max_parts = 1000;
    session.part_count = 300;
    let parts: Vec<_> = (0..300)
        .map(|index| MultipartPart {
            upload: session.upload,
            number: 2 * index + 1,
            revision: 1,
            modified_ms: 101,
            owner: FileIdentity {
                file: FileId::random(),
                ..session.owner
            },
            tree: FileTree {
                root: None,
                length: 0,
                digest: Sha256::digest([]).into(),
            },
        })
        .collect();
    fixture.store.values.rcu(|values| {
        let mut next = (**values).clone();
        next.insert(
            session.key().encode().unwrap(),
            StoredValue {
                bytes: StorageRecord::MultipartSession(Box::new(session.clone()))
                    .encode()
                    .unwrap(),
                revision: 1,
            },
        );
        for part in &parts {
            next.insert(
                part.key().encode().unwrap(),
                StoredValue {
                    bytes: StorageRecord::MultipartPart(Box::new(part.clone()))
                        .encode()
                        .unwrap(),
                    revision: 1,
                },
            );
        }
        next
    });
    (fixture, session, parts)
}

#[tokio::test]
async fn multipart_pages_preserve_numeric_markers_across_gaps_and_cap_each_storage_scan() {
    let (fixture, session, parts) = setup().await;
    let lister = MultipartLister::new(fixture.store.clone());
    let first = lister.list(&session, 0, 1000, 101).await.unwrap();
    assert_eq!(first.parts, parts[..256]);
    assert_eq!(first.next_marker, Some(511));
    let last = lister
        .list(&session, first.next_marker.unwrap(), 1000, 101)
        .await
        .unwrap();
    assert_eq!(last.parts, parts[256..]);
    assert!(last.next_marker.is_none());
    let sparse = lister.list(&session, 510, 2, 101).await.unwrap();
    assert_eq!(sparse.parts, parts[255..257]);
    assert_eq!(sparse.next_marker, Some(513));
    let empty = lister.list(&session, 10_000, 1000, 101).await.unwrap();
    assert!(empty.parts.is_empty() && empty.next_marker.is_none());
}

#[tokio::test]
async fn list_rejects_stale_terminal_expired_and_retired_sessions_without_mutations() {
    let (fixture, session, _) = setup().await;
    let lister = MultipartLister::new(fixture.store.clone());
    let writes = fixture.store.writes.load(Ordering::SeqCst);
    for (marker, maximum, now) in [
        (10_001, 10, 101),
        (0, 0, 101),
        (0, 1001, 101),
        (0, 10, 99),
        (0, 10, session.expires_ms),
    ] {
        assert!(lister.list(&session, marker, maximum, now).await.is_err());
    }
    let mut stale = session.clone();
    stale.revision += 1;
    assert!(matches!(
        lister.list(&stale, 0, 10, 101).await,
        Err(CatalogError::Busy)
    ));
    let mut terminal = session.clone();
    terminal.phase = MultipartPhase::Aborted;
    assert!(matches!(
        lister.list(&terminal, 0, 10, 101).await,
        Err(CatalogError::Conflict)
    ));
    let mut completing = session.clone();
    completing.phase = MultipartPhase::Completing;
    completing.completion = Some(fixtures::completion(&session));
    assert!(matches!(
        lister.list(&completing, 0, 10, 101).await,
        Err(CatalogError::Busy)
    ));
    assert_eq!(fixture.store.writes.load(Ordering::SeqCst), writes);
    fixture.root(fixture.context, RootState::Fencing).await;
    assert!(matches!(
        lister.list(&session, 0, 10, 101).await,
        Err(CatalogError::Busy)
    ));
}

#[tokio::test]
async fn part_listing_never_hides_corruption_or_reads_an_adjacent_upload() {
    let (fixture, session, parts) = setup().await;
    let lister = MultipartLister::new(fixture.store.clone());
    let mut other = parts[0].clone();
    other.upload = OperationId::random();
    let key = other.key().encode().unwrap();
    let value = StorageRecord::MultipartPart(Box::new(other)).encode().unwrap();
    fixture
        .store
        .compare_exchange(&key, None, &value, mutation_identity(&key, None, &value))
        .await
        .unwrap();
    assert_eq!(lister.list(&session, 0, 1, 101).await.unwrap().parts, parts[..1]);
    let key = parts[0].key().encode().unwrap();
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
    assert!(lister.list(&session, 0, 1, 101).await.is_err());
}

#[test]
fn multipart_part_scan_bounds_exclude_other_uploads_and_session_authorities() {
    let session = fixtures::session();
    let scan = MultipartPartScan {
        catalog: session.context.catalog,
        upload: session.upload,
        after: 7,
        limit: 256,
    };
    let request = scan.request().unwrap();
    assert_eq!(request.max_items, 256);
    assert!(session.key().encode().unwrap() < request.start.unwrap());
    for (after, limit) in [(10_001, 1), (0, 0), (0, 257)] {
        assert!(MultipartPartScan {
            after,
            limit,
            ..scan.clone()
        }
        .request()
        .is_err());
    }
}

#[tokio::test]
async fn list_discards_a_page_when_the_session_changes_during_the_scan() {
    let (fixture, session, _) = setup().await;
    let lister = MultipartLister::new(std::sync::Arc::new(race::TestPartRace {
        inner: fixture.store,
        session: session.clone(),
    }));
    assert!(matches!(
        lister.list(&session, 0, 1000, 101).await,
        Err(CatalogError::Busy)
    ));
}
