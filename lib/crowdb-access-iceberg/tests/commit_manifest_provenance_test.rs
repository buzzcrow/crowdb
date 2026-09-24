#[path = "common/file_blocks.rs"]
mod blocks;
#[path = "common/store.rs"]
mod common;
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
#[path = "common/snapshot_files.rs"]
mod snapshot;
#[path = "common/manifest_stream.rs"]
#[allow(dead_code)]
mod stream;

use crowdb_access_iceberg::{
    catalog::RootState,
    commit::{CandidateFileSource, CandidateSnapshotLimits, PriorManifestLimits},
    file::{ContentFormat, FileRepository},
    manifest::{SnapshotFileSource, SnapshotManifestSource},
    record::StorageRecord,
    table::head_key,
};
use std::sync::{atomic::Ordering, Arc};

#[path = "common/commit_provenance.rs"]
mod provenance;
use provenance::{limits, TestPrior};

#[tokio::test]
async fn embedded_legacy_manifests_are_anchored_in_canonical_metadata_without_a_list() {
    let fixture = TestPrior::legacy().await;
    let source = fixture.build(limits()).await.unwrap();
    let (_, context) = source.resolve(&fixture.manifest.location).await.unwrap();
    assert_eq!(context.schema_id(), 0);
    assert!(context.field(3).is_some());
    let mut limited = limits();
    limited.manifests.manifest_bytes = fixture.manifest.length - 1;
    assert!(fixture.build(limited).await.is_err());
}

#[tokio::test]
async fn candidate_validation_checks_every_snapshot_and_current_schema_projection() {
    use sha2::{Digest, Sha256};
    let fixture = TestPrior::new().await;
    let prior = Arc::new(fixture.build(limits()).await.unwrap());
    for required in [false, true] {
        let mut value = serde_json::Value::Object(fixture.document.fields().clone());
        value["schemas"][0]["fields"][0]["required"] = serde_json::json!(required);
        let bytes = serde_json::to_vec(&value).unwrap();
        let mut head = fixture.selected.head.clone();
        head.generation += 1;
        head.metadata_file = crowdb_access_iceberg::key::FileId::random();
        head.metadata_location = fixture::table().file("metadata/candidate.json").unwrap();
        head.metadata_digest = Sha256::digest(&bytes).into();
        let document = Arc::new(
            crowdb_access_iceberg::table::TableMetadataDocument::parse(bytes, &head, metadata::limits())
                .unwrap(),
        );
        let source = Arc::new(
            CandidateFileSource::new(
                fixture.namespace.store.clone(),
                fixture.blocks.clone(),
                fixture.namespace.context,
                prior.clone(),
                document,
                limits().manifests.framing,
            )
            .unwrap(),
        );
        let checked = source
            .validate_snapshots(
                &fixture.document,
                CandidateSnapshotLimits {
                    snapshots: 10,
                    entries: 100,
                    manifest_bytes: 1_000_000,
                    ranges: 100,
                    files: snapshot::limits(),
                },
            )
            .await;
        if required {
            assert!(checked.is_err());
        } else {
            let summary = checked.unwrap();
            assert_eq!(summary.snapshots, 1);
            assert_eq!((summary.data_files, summary.data_rows), (1, 10));
        }
    }
}

#[tokio::test]
async fn candidate_sources_separate_reachable_history_from_new_definition_bound_uploads() {
    let fixture = TestPrior::new().await;
    let prior = Arc::new(fixture.build(limits()).await.unwrap());
    let upload = fixture.copy_manifest("metadata/upload.avro").await;
    for restored in [false, true] {
        let source = CandidateFileSource::new(
            fixture.namespace.store.clone(),
            fixture.blocks.clone(),
            fixture.namespace.context,
            prior.clone(),
            fixture.candidate(restored),
            limits().manifests.framing,
        )
        .unwrap();
        assert!(
            SnapshotManifestSource::resolve(&source, &fixture.manifest.location)
                .await
                .is_ok()
        );
        assert_eq!(
            SnapshotManifestSource::resolve(&source, &upload.location)
                .await
                .is_ok(),
            restored
        );
        assert_eq!(
            SnapshotFileSource::resolve(&source, &upload.location)
                .await
                .unwrap(),
            upload
        );
        let foreign = crowdb_access_iceberg::file::TableLocation {
            catalog: fixture.namespace.context.catalog,
            table: crowdb_access_iceberg::key::TableId::random(),
        }
        .file("metadata/upload.avro")
        .unwrap();
        assert!(SnapshotFileSource::resolve(&source, &foreign).await.is_err());
    }
}

#[tokio::test]
async fn a_head_change_during_canonical_manifest_reads_discards_the_recovered_context() {
    let fixture = TestPrior::new().await;
    let source = fixture.build(limits()).await.unwrap();
    fixture.blocks.pause_reads.store(true, Ordering::SeqCst);
    let mut resolution = Box::pin(source.resolve(&fixture.manifest.location));
    tokio::select! {
        result = &mut resolution => panic!("read did not pause: {result:?}"),
        () = fixture.blocks.read_entered.notified() => {}
    }
    let mut head = fixture.selected.head.clone();
    head.operation_fence += 1;
    fixture
        .namespace
        .put(
            head_key(head.catalog, head.table),
            StorageRecord::TableHead(Box::new(head)),
        )
        .await;
    fixture.blocks.pause_reads.store(false, Ordering::SeqCst);
    fixture.blocks.read_release.notify_one();
    assert!(resolution.await.is_err());
}

#[tokio::test]
async fn reachable_manifest_recovers_expired_schema_but_uploads_cannot_self_authorize() {
    let fixture = TestPrior::new().await;
    assert!(fixture.document.manifest_context(0, 0, &[], 1000).is_err());
    let source = fixture.build(limits()).await.unwrap();
    assert_eq!(source.selected(), &fixture.selected);
    let (record, context) = source.resolve(&fixture.manifest.location).await.unwrap();
    assert_eq!(record.file, fixture.manifest.file);
    assert_eq!(context.schema_id(), 0);
    assert!(context.field(3).is_some());
    assert!(context.field(4).is_none());
    let mut reader = crowdb_access_iceberg::file::FileReader::new(
        fixture.blocks.clone(),
        fixture.manifest.clone(),
        None,
        8192,
    )
    .unwrap();
    let mut bytes = Vec::new();
    while let Some(chunk) = reader.next().await.unwrap() {
        bytes.extend(chunk);
    }
    let upload = snapshot::store(
        fixture.blocks.clone(),
        "metadata/unselected.avro",
        ContentFormat::Avro,
        &bytes,
    )
    .await;
    FileRepository::new(fixture.namespace.store.clone())
        .publish(fixture.namespace.context, &upload)
        .await
        .unwrap();
    assert!(source.resolve(&upload.location).await.is_err());
    let mut reader = crowdb_access_iceberg::manifest::SnapshotManifestReader::open(
        fixture.blocks.clone(),
        Arc::new(source),
        fixture.input.list,
        fixture.input.selection,
        snapshot::limits().manifests,
    )
    .await
    .unwrap();
    assert!(reader.next_entry().await.unwrap().is_some());
    assert!(reader.next_entry().await.unwrap().is_none());
    assert_eq!(reader.finish().unwrap().entries, 1);
}

#[tokio::test]
async fn changed_head_and_retired_catalog_invalidate_prepared_provenance() {
    let fixture = TestPrior::new().await;
    let source = fixture.build(limits()).await.unwrap();
    let mut head = fixture.selected.head.clone();
    head.operation_fence += 1;
    fixture
        .namespace
        .put(
            head_key(head.catalog, head.table),
            StorageRecord::TableHead(Box::new(head)),
        )
        .await;
    assert!(source.resolve(&fixture.manifest.location).await.is_err());
    assert!(fixture.build(limits()).await.is_err());
    let fixture = TestPrior::new().await;
    let source = fixture.build(limits()).await.unwrap();
    let mut context = fixture.namespace.context;
    context.activation_epoch += 1;
    fixture.namespace.root(context, RootState::Ready).await;
    assert!(source.resolve(&fixture.manifest.location).await.is_err());
}

#[tokio::test]
async fn provenance_bounds_and_canonical_corruption_never_return_partial_authority() {
    let fixture = TestPrior::new().await;
    for limit in [
        PriorManifestLimits {
            snapshots: 0,
            ..limits()
        },
        PriorManifestLimits {
            references: 0,
            ..limits()
        },
        PriorManifestLimits {
            index_bytes: 1,
            ..limits()
        },
    ] {
        assert!(fixture.build(limit).await.is_err());
    }
    let mut limit = limits();
    limit.manifests.manifest_bytes = fixture.input.list.length;
    assert!(fixture.build(limit).await.is_err());
    let source = fixture.build(limits()).await.unwrap();
    fixture.blocks.corrupt_reads.store(true, Ordering::SeqCst);
    assert!(source.resolve(&fixture.manifest.location).await.is_err());
    assert!(fixture.build(limits()).await.is_err());
}
