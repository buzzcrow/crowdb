#[path = "common/file_blocks.rs"]
mod blocks;
#[path = "common/store.rs"]
mod common;
#[path = "common/commit_creation.rs"]
mod creation;
#[path = "common/manifest_entry.rs"]
#[allow(dead_code)]
mod fixture;
#[path = "common/manifest_list.rs"]
#[allow(dead_code)]
mod list_fixture;
#[path = "common/table_metadata.rs"]
#[allow(dead_code)]
mod metadata;
#[path = "common/namespace.rs"]
#[allow(dead_code)]
mod namespaces;
#[path = "common/parquet_metadata.rs"]
#[allow(dead_code)]
mod parquet;
#[path = "common/commit_provenance.rs"]
#[allow(dead_code)]
mod provenance;
#[path = "common/snapshot_files.rs"]
#[allow(dead_code)]
mod snapshot;
#[path = "common/manifest_stream.rs"]
#[allow(dead_code)]
mod stream;

use std::sync::atomic::Ordering;

use creation::{limits, TestInitial};
use crowdb_access_iceberg::{
    catalog::{CatalogStore, RootState},
    commit::{CandidateAuxiliaryLimits, CandidateFileSource, TableCreatePhase},
    manifest::{SnapshotFileSource, SnapshotManifestSource},
    record::StorageRecord,
    table::{head_key, name_key, TableMappingState},
};

#[tokio::test]
async fn initial_files_validate_without_a_published_head_or_historical_authority() {
    let test = TestInitial::new(true, None).await;
    let source = test.source();
    let summary = source.clone().validate_initial_snapshots(limits()).await.unwrap();
    assert_eq!(
        (summary.snapshots, summary.data_files, summary.data_rows),
        (1, 1, 10)
    );
    assert!(source
        .clone()
        .validate_snapshots(&test.fixture.document, limits())
        .await
        .is_err());
    assert!(test
        .fixture
        .namespace
        .store
        .get(
            &head_key(test.operation.candidate.catalog, test.operation.candidate.table)
                .encode()
                .unwrap()
        )
        .await
        .unwrap()
        .is_none());
    let missing_schema = TestInitial::new(false, None).await;
    assert!(missing_schema
        .source()
        .validate_initial_snapshots(limits())
        .await
        .is_err());
    let missing_parent = TestInitial::new(true, Some(98)).await;
    assert!(missing_parent
        .source()
        .validate_initial_snapshots(limits())
        .await
        .is_err());
}

#[tokio::test]
async fn changed_intent_during_manifest_io_invalidates_the_initial_source() {
    let mut test = TestInitial::new(true, None).await;
    let source = test.source();
    test.fixture.blocks.pause_reads.store(true, Ordering::SeqCst);
    let mut resolving = Box::pin(SnapshotManifestSource::resolve(
        source.as_ref(),
        &test.fixture.manifest.location,
    ));
    tokio::select! {
        result = &mut resolving => panic!("read did not pause: {result:?}"),
        () = test.fixture.blocks.read_entered.notified() => {}
    }
    test.operation.revision += 1;
    test.persist().await;
    test.fixture.blocks.pause_reads.store(false, Ordering::SeqCst);
    test.fixture.blocks.read_release.notify_one();
    assert!(resolving.await.is_err());
}

#[tokio::test]
async fn reservation_head_and_catalog_fences_are_independent() {
    for change in 0..3 {
        let test = TestInitial::new(true, None).await;
        let source = test.source();
        match change {
            0 => {
                let mut mapping = test.operation.mapping(TableMappingState::Reserved);
                mapping.operation = crowdb_access_iceberg::key::OperationId::random();
                test.fixture
                    .namespace
                    .put(
                        name_key(mapping.catalog, mapping.namespace, &mapping.name).unwrap(),
                        StorageRecord::TableMapping(mapping),
                    )
                    .await;
            }
            1 => {
                test.fixture
                    .namespace
                    .put(
                        head_key(test.operation.candidate.catalog, test.operation.candidate.table),
                        StorageRecord::TableHead(Box::new(test.operation.candidate.clone())),
                    )
                    .await;
            }
            _ => {
                let mut context = test.fixture.namespace.context;
                context.activation_epoch += 1;
                test.fixture.namespace.root(context, RootState::Ready).await;
            }
        }
        assert!(
            SnapshotFileSource::resolve(source.as_ref(), &test.fixture.manifest.location)
                .await
                .is_err()
        );
        assert!(source
            .validate_auxiliary_files(CandidateAuxiliaryLimits {
                files: 10,
                bytes: 1_000_000,
                work: 1000,
                puffin_encoded_bytes: 100_000,
                puffin_decoded_bytes: 100_000,
                parquet: snapshot::limits().position_deletes.metadata,
                partition_rows: crowdb_access_iceberg::manifest::PartitionStatisticsRowLimits {
                    page: snapshot::limits().position_deletes.page,
                    rows: 1000,
                    buffered_bytes: 64 * 1024 * 1024,
                },
            })
            .await
            .is_err());
    }
}

#[tokio::test]
async fn initial_source_rejects_foreign_candidates_wrong_phases_and_work_overflow() {
    let mut test = TestInitial::new(true, None).await;
    let source = test.source();
    let mut limited = limits();
    limited.manifest_bytes = 1;
    assert!(source.validate_initial_snapshots(limited).await.is_err());
    assert!(CandidateFileSource::for_creation(
        test.fixture.namespace.store.clone(),
        test.fixture.blocks.clone(),
        &test.operation,
        test.fixture.candidate(true),
        snapshot::limits().manifests.framing,
    )
    .is_err());
    test.operation.phase = TableCreatePhase::FilesReady;
    assert!(CandidateFileSource::for_creation(
        test.fixture.namespace.store.clone(),
        test.fixture.blocks.clone(),
        &test.operation,
        test.document.clone(),
        snapshot::limits().manifests.framing,
    )
    .is_err());
}
