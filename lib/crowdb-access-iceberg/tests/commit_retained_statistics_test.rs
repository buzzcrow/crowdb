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
#[path = "common/commit_provenance.rs"]
#[allow(dead_code)]
mod provenance;
#[path = "common/snapshot_files.rs"]
#[allow(dead_code)]
mod snapshot;
#[path = "common/partition_statistics_official.rs"]
#[allow(dead_code)]
mod statistics_official;
#[path = "common/manifest_stream.rs"]
#[allow(dead_code)]
mod stream;

use crowdb_access_iceberg::{
    commit::{CandidateAuxiliaryLimits, CandidateFileSource},
    file::{ContentFormat, FileContent, FileKind, FileRecord, FileRepository},
    record::StorageRecord,
    table::{head_key, SelectedTable},
};
use provenance::TestPrior;
use serde_json::{json, Value};
use std::sync::{atomic::Ordering, Arc};

fn limits() -> CandidateAuxiliaryLimits {
    CandidateAuxiliaryLimits {
        manifests: snapshot::limits().manifests,
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
    }
}

async fn source_document(fixture: &TestPrior, value: &Value) -> Arc<CandidateFileSource> {
    Arc::new(
        CandidateFileSource::new(
            fixture.namespace.store.clone(),
            fixture.blocks.clone(),
            fixture.namespace.context,
            Arc::new(fixture.build(provenance::limits()).await.unwrap()),
            fixture.candidate_value(value),
            provenance::limits().manifests.framing,
        )
        .unwrap(),
    )
}

async fn accepted() -> TestPrior {
    let mut fixture = TestPrior::new().await;
    let empty = snapshot::store(
        fixture.blocks.clone(),
        "metadata/empty-list.avro",
        ContentFormat::Avro,
        &snapshot::ocf(
            vec![(
                "avro.schema",
                list_fixture::TestManifestList::new().schema_bytes(),
            )],
            &[],
        ),
    )
    .await;
    FileRepository::new(fixture.namespace.store.clone())
        .publish(fixture.namespace.context, &empty)
        .await
        .unwrap();
    let bytes = data_encoding::BASE64
        .decode(statistics_official::STATS_2_TRUE.as_bytes())
        .unwrap();
    let record = snapshot::store(
        fixture.blocks.clone(),
        "metadata/rows.parquet",
        ContentFormat::Parquet,
        &bytes,
    )
    .await;
    FileRepository::new(fixture.namespace.store.clone())
        .publish(fixture.namespace.context, &record)
        .await
        .unwrap();
    let mut value = Value::Object(fixture.document.fields().clone());
    value["snapshots"][0]["manifest-list"] = json!(empty.location.to_string());
    value["schemas"] = json!([{"schema-id":1,"type":"struct","fields":[
        {"id":2,"name":"kept","type":"int","required":false}]}]);
    value["current-schema-id"] = json!(1);
    value["partition-specs"] = json!([
        {"spec-id":0,"fields":[
            {"source-id":1,"field-id":1000,"name":"old_part","transform":"identity"},
            {"source-id":2,"field-id":1001,"name":"kept_part","transform":"identity"}]},
        {"spec-id":1,"fields":[
            {"source-id":2,"field-id":1001,"name":"renamed","transform":"identity"}]}]);
    value["default-spec-id"] = json!(1);
    value["last-partition-id"] = json!(1001);
    value["partition-statistics"] = json!([{"snapshot-id":99,
        "statistics-path":record.location.to_string(),"file-size-in-bytes":record.length}]);
    source_document(&fixture, &value)
        .await
        .validate_auxiliary_files(limits())
        .await
        .unwrap();
    let bytes = serde_json::to_vec(&value).unwrap();
    let mut head = metadata::head(&bytes, 2, fixture.selected.head.table_uuid);
    head.metadata_location = fixture::table().file("metadata/accepted.json").unwrap();
    let document =
        crowdb_access_iceberg::table::TableMetadataDocument::parse(bytes, &head, metadata::limits()).unwrap();
    let record = FileRecord {
        file: head.metadata_file,
        location: head.metadata_location.clone(),
        kind: FileKind::Metadata,
        format: ContentFormat::Json,
        length: document.canonical().len() as u64,
        digest: head.metadata_digest,
        content: FileContent::select_inline(FileKind::Metadata, document.canonical()).unwrap(),
        hint: None,
    };
    FileRepository::new(fixture.namespace.store.clone())
        .publish(fixture.namespace.context, &record)
        .await
        .unwrap();
    fixture
        .namespace
        .put(
            head_key(head.catalog, head.table),
            StorageRecord::TableHead(Box::new(head.clone())),
        )
        .await;
    fixture.selected = SelectedTable {
        head,
        metadata: record,
    };
    fixture.document = document;
    fixture
}

#[tokio::test]
async fn accepted_statistics_survive_upgrade_and_partition_schema_evolution() {
    let fixture = accepted().await;
    let mut value = Value::Object(fixture.document.fields().clone());
    value["format-version"] = json!(3);
    value["next-row-id"] = json!(0);
    let source = source_document(&fixture, &value).await;
    assert!(source.validate_auxiliary_files(limits()).await.is_err());
    let summary = source
        .validate_auxiliary_files_with_prior(&fixture.document, limits())
        .await
        .unwrap();
    assert_eq!(summary.files, 1);

    value["format-version"] = json!(2);
    value.as_object_mut().unwrap().remove("next-row-id");
    value["schemas"][0]["fields"]
        .as_array_mut()
        .unwrap()
        .push(json!({"id":3,"name":"added","type":"long","required":false}));
    value["partition-specs"]
        .as_array_mut()
        .unwrap()
        .push(json!({"spec-id":2,"fields":[
        {"source-id":3,"field-id":1002,"name":"added_part","transform":"identity"}]}));
    value["default-spec-id"] = json!(2);
    value["last-partition-id"] = json!(1002);
    let source = source_document(&fixture, &value).await;
    assert!(source.validate_auxiliary_files(limits()).await.is_err());
    assert!(source
        .validate_auxiliary_files_with_prior(&fixture.document, limits())
        .await
        .is_ok());
}

#[tokio::test]
async fn retained_statistics_require_unchanged_reference_and_snapshot() {
    let fixture = accepted().await;
    let mut value = Value::Object(fixture.document.fields().clone());
    value["format-version"] = json!(3);
    value["next-row-id"] = json!(0);
    for (field, replacement) in [("timestamp-ms", json!(1001)), ("sequence-number", json!(8))] {
        let mut changed = value.clone();
        changed["snapshots"][0][field] = replacement;
        assert!(source_document(&fixture, &changed)
            .await
            .validate_auxiliary_files_with_prior(&fixture.document, limits())
            .await
            .is_err());
    }
    let bytes = data_encoding::BASE64
        .decode(statistics_official::STATS_2_TRUE.as_bytes())
        .unwrap();
    let record = snapshot::store(
        fixture.blocks.clone(),
        "metadata/copied.parquet",
        ContentFormat::Parquet,
        &bytes,
    )
    .await;
    FileRepository::new(fixture.namespace.store.clone())
        .publish(fixture.namespace.context, &record)
        .await
        .unwrap();
    value["partition-statistics"][0]["statistics-path"] = json!(record.location.to_string());
    assert!(source_document(&fixture, &value)
        .await
        .validate_auxiliary_files_with_prior(&fixture.document, limits())
        .await
        .is_err());
}

#[tokio::test]
async fn retained_statistics_still_verify_authority_bytes_and_budgets() {
    let fixture = accepted().await;
    let mut value = Value::Object(fixture.document.fields().clone());
    let source = source_document(&fixture, &value).await;
    let foreign = TestPrior::new().await;
    assert!(source
        .validate_auxiliary_files_with_prior(&foreign.document, limits())
        .await
        .is_err());
    for limited in [
        CandidateAuxiliaryLimits { work: 1, ..limits() },
        CandidateAuxiliaryLimits { bytes: 1, ..limits() },
    ] {
        assert!(source
            .validate_auxiliary_files_with_prior(&fixture.document, limited)
            .await
            .is_err());
    }
    value["partition-statistics"][0]["key-metadata"] = json!("AA==");
    assert!(source_document(&fixture, &value)
        .await
        .validate_auxiliary_files_with_prior(&fixture.document, limits())
        .await
        .is_err());
    fixture.blocks.corrupt_reads.store(true, Ordering::SeqCst);
    assert!(source
        .validate_auxiliary_files_with_prior(&fixture.document, limits())
        .await
        .is_err());
}
