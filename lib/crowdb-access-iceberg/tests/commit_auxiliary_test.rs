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
#[path = "common/manifest_stream.rs"]
#[allow(dead_code)]
mod stream;

use std::sync::Arc;

use crowdb_access_iceberg::{
    commit::{CandidateAuxiliaryLimits, CandidateFileSource},
    file::{ContentFormat, FileRepository},
};
use provenance::TestPrior;
use serde_json::{json, Value};

fn limits() -> CandidateAuxiliaryLimits {
    CandidateAuxiliaryLimits {
        files: 10,
        bytes: 1_000_000,
        work: 1000,
        puffin_encoded_bytes: 100_000,
        puffin_decoded_bytes: 100_000,
        parquet: snapshot::limits().position_deletes.metadata,
    }
}

async fn source(fixture: &TestPrior, entry: Value, field: &str) -> CandidateFileSource {
    let mut value = Value::Object(fixture.document.fields().clone());
    value[field] = json!([entry]);
    CandidateFileSource::new(
        fixture.namespace.store.clone(),
        fixture.blocks.clone(),
        fixture.namespace.context,
        Arc::new(fixture.build(provenance::limits()).await.unwrap()),
        fixture.candidate_value(&value),
        provenance::limits().manifests.framing,
    )
    .unwrap()
}

async fn statistics(fixture: &TestPrior) -> Value {
    let blob = json!({"type":"apache-datasketches-theta-v1","snapshot-id":99,
        "sequence-number":9,"fields":[3],"offset":4,"length":4,
        "properties":{"ndv":"10","extra":"allowed"}});
    let footer = serde_json::to_vec(&json!({"blobs":[blob]})).unwrap();
    let mut bytes = b"PFA1dataPFA1".to_vec();
    bytes.extend(&footer);
    bytes.extend(u32::try_from(footer.len()).unwrap().to_le_bytes());
    bytes.extend([0; 4]);
    bytes.extend(b"PFA1");
    let record = snapshot::store(
        fixture.blocks.clone(),
        "metadata/stats.puffin",
        ContentFormat::Puffin,
        &bytes,
    )
    .await;
    FileRepository::new(fixture.namespace.store.clone())
        .publish(fixture.namespace.context, &record)
        .await
        .unwrap();
    json!({"snapshot-id":99,"statistics-path":record.location.to_string(),
        "file-size-in-bytes":record.length,"file-footer-size-in-bytes":footer.len() + 16,
        "blob-metadata":[{"type":blob["type"],"snapshot-id":99,"sequence-number":9,
        "fields":[3],"properties":{"ndv":"10"}}]})
}

#[tokio::test]
async fn statistics_bind_total_footer_size_and_subset_properties_to_canonical_puffin() {
    let fixture = TestPrior::new().await;
    let entry = statistics(&fixture).await;
    let checked = source(&fixture, entry.clone(), "statistics")
        .await
        .validate_auxiliary_files(limits())
        .await
        .unwrap();
    assert_eq!((checked.files, checked.blobs), (1, 1));
    assert_eq!(checked.bytes, entry["file-size-in-bytes"].as_u64().unwrap());
    for (field, value) in [
        ("file-size-in-bytes", json!(checked.bytes + 1)),
        (
            "file-footer-size-in-bytes",
            json!(entry["file-footer-size-in-bytes"].as_u64().unwrap() - 16),
        ),
        ("key-metadata", json!("AA==")),
    ] {
        let mut invalid = entry.clone();
        invalid[field] = value;
        assert!(source(&fixture, invalid, "statistics")
            .await
            .validate_auxiliary_files(limits())
            .await
            .is_err());
    }
}

#[tokio::test]
async fn statistics_cannot_invent_blob_descriptors_or_duplicate_a_single_blob() {
    let fixture = TestPrior::new().await;
    let entry = statistics(&fixture).await;
    for (field, value) in [
        ("type", json!("different")),
        ("snapshot-id", json!(100)),
        ("sequence-number", json!(10)),
        ("fields", json!([4])),
        ("properties", json!({"ndv":"11"})),
    ] {
        let mut invalid = entry.clone();
        invalid["blob-metadata"][0][field] = value;
        assert!(source(&fixture, invalid, "statistics")
            .await
            .validate_auxiliary_files(limits())
            .await
            .is_err());
    }
    let mut invalid = entry;
    let duplicate = invalid["blob-metadata"][0].clone();
    invalid["blob-metadata"].as_array_mut().unwrap().push(duplicate);
    assert!(source(&fixture, invalid, "statistics")
        .await
        .validate_auxiliary_files(limits())
        .await
        .is_err());
}

#[tokio::test]
async fn auxiliary_file_byte_and_comparison_budgets_are_independent() {
    let fixture = TestPrior::new().await;
    let entry = statistics(&fixture).await;
    let source = source(&fixture, entry.clone(), "statistics").await;
    for limited in [
        CandidateAuxiliaryLimits { files: 0, ..limits() },
        CandidateAuxiliaryLimits {
            bytes: entry["file-size-in-bytes"].as_u64().unwrap() - 1,
            ..limits()
        },
        CandidateAuxiliaryLimits { work: 1, ..limits() },
        CandidateAuxiliaryLimits {
            puffin_encoded_bytes: 1,
            ..limits()
        },
        CandidateAuxiliaryLimits {
            puffin_decoded_bytes: 1,
            ..limits()
        },
    ] {
        assert!(source.validate_auxiliary_files(limited).await.is_err());
    }
    assert!(source
        .validate_auxiliary_files(CandidateAuxiliaryLimits {
            bytes: entry["file-size-in-bytes"].as_u64().unwrap(),
            ..limits()
        })
        .await
        .is_ok());
    fixture
        .blocks
        .corrupt_reads
        .store(true, std::sync::atomic::Ordering::SeqCst);
    assert!(source.validate_auxiliary_files(limits()).await.is_err());
}

#[tokio::test]
async fn partition_statistics_resolve_plaintext_parquet_not_puffin_or_missing_paths() {
    let fixture = TestPrior::new().await;
    let record = snapshot::data(fixture.blocks.clone(), "metadata/partition-stats.parquet").await;
    FileRepository::new(fixture.namespace.store.clone())
        .publish(fixture.namespace.context, &record)
        .await
        .unwrap();
    let mut entry = json!({"snapshot-id":99,"statistics-path":record.location.to_string(),
        "file-size-in-bytes":record.length});
    assert!(source(&fixture, entry.clone(), "partition-statistics")
        .await
        .validate_auxiliary_files(limits())
        .await
        .is_ok());
    entry["statistics-path"] = json!(fixture::table()
        .file("metadata/missing.parquet")
        .unwrap()
        .to_string());
    assert!(source(&fixture, entry, "partition-statistics")
        .await
        .validate_auxiliary_files(limits())
        .await
        .is_err());
    let puffin = statistics(&fixture).await;
    assert!(source(&fixture, puffin, "partition-statistics")
        .await
        .validate_auxiliary_files(limits())
        .await
        .is_err());
}
