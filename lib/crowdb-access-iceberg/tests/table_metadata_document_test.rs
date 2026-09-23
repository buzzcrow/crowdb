#[path = "common/file_blocks.rs"]
mod blocks;
#[path = "common/table_metadata.rs"]
#[allow(dead_code)]
mod fixture;
#[path = "common/metadata_iceberg_fixture.rs"]
mod official;

use crowdb_access_iceberg::{
    file::{ContentFormat, FileContent, FileIdentity, FileKind, FileRecord, FileTreeWriter},
    table::{read_table_metadata_document, SelectedTable, TableMetadataDocument, TableMetadataError},
};
use serde_json::json;
use sha2::{Digest, Sha256};
use std::sync::{atomic::Ordering, Arc};

#[test]
fn official_java_metadata_roundtrips_without_rewriting() {
    for bytes in official::files() {
        let value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        let version = u8::try_from(value["format-version"].as_u64().unwrap()).unwrap();
        let uuid = uuid::Uuid::parse_str(value["table-uuid"].as_str().unwrap()).unwrap();
        let head = fixture::head(&bytes, version, Some(uuid));
        let document = TableMetadataDocument::parse(bytes.clone(), &head, fixture::limits()).unwrap();
        assert_eq!(document.canonical(), bytes);
        assert_eq!(document.fields()["table-uuid"], value["table-uuid"]);
        assert!(document.snapshots().is_empty());
        assert_eq!(document.current_snapshot(), None);
    }
}

#[test]
fn version_envelopes_preserve_original_bytes_and_unknown_fields() {
    for version in 1..=3 {
        let mut value = fixture::metadata(version);
        value["future-extension"] = json!({"nested":[null,true,5,"雪"]});
        value["location"] = json!(fixture::table().to_string().trim_end_matches('/'));
        let bytes = format!(" \n{}\n ", serde_json::to_string_pretty(&value).unwrap()).into_bytes();
        let head = fixture::head(
            &bytes,
            version,
            Some(uuid::Uuid::parse_str(value["table-uuid"].as_str().unwrap()).unwrap()),
        );
        let document = TableMetadataDocument::parse(bytes.clone(), &head, fixture::limits()).unwrap();
        assert_eq!(document.canonical(), bytes);
        assert_eq!(document.fields()["future-extension"], value["future-extension"]);
        assert!(document.snapshots().is_empty());
        assert_eq!(document.current_snapshot(), None);
    }
}

#[test]
fn legacy_v1_fallback_and_upgraded_snapshots_do_not_invent_lineage() {
    let mut value = fixture::metadata(1);
    for field in [
        "schemas",
        "current-schema-id",
        "partition-specs",
        "default-spec-id",
        "sort-orders",
        "default-sort-order-id",
        "last-partition-id",
        "table-uuid",
        "refs",
    ] {
        value.as_object_mut().unwrap().remove(field);
    }
    let legacy = json!({"snapshot-id":10,"timestamp-ms":1000,"manifests":[fixture::table().file("metadata/old.avro").unwrap().to_string()]});
    value["snapshots"] = json!([legacy]);
    value["current-snapshot-id"] = json!(10);
    assert!(fixture::parse(&value).unwrap().snapshots()[&10]
        .manifest_list
        .is_none());
    for version in [2, 3] {
        let mut value = fixture::metadata(version);
        value["snapshots"] = json!([legacy]);
        value["current-snapshot-id"] = json!(10);
        value.as_object_mut().unwrap().remove("refs");
        let document = fixture::parse(&value).unwrap();
        assert_eq!(document.snapshots()[&10].sequence, 0);
        assert_eq!(document.snapshots()[&10].first_row_id, None);
    }
}

#[test]
fn malformed_required_fields_defaults_and_foreign_identities_fail() {
    for field in [
        "last-updated-ms",
        "last-column-id",
        "schemas",
        "current-schema-id",
        "partition-specs",
        "default-spec-id",
        "last-partition-id",
        "sort-orders",
        "default-sort-order-id",
        "last-sequence-number",
    ] {
        let mut value = fixture::metadata(2);
        value.as_object_mut().unwrap().remove(field);
        assert!(fixture::parse(&value).is_err(), "{field}");
    }
    let mut value = fixture::metadata(3);
    value.as_object_mut().unwrap().remove("next-row-id");
    assert!(fixture::parse(&value).is_err());
    for (field, invalid) in [
        ("current-schema-id", json!(10)),
        ("schemas", json!([])),
        ("properties", json!({"a":1})),
        ("last-column-id", json!(2_147_483_648_u64)),
        ("last-sequence-number", json!(-1)),
        ("location", json!("s3://other/table")),
    ] {
        let mut value = fixture::metadata(2);
        value[field] = invalid;
        assert!(fixture::parse(&value).is_err(), "{field}");
    }
}

#[test]
fn duplicate_keys_trailing_json_and_independent_budgets_fail() {
    let value = fixture::metadata(2);
    let canonical = serde_json::to_vec(&value).unwrap();
    let mut head = fixture::head(
        &canonical,
        2,
        Some(uuid::Uuid::parse_str(value["table-uuid"].as_str().unwrap()).unwrap()),
    );
    for malformed in [
        b"{\"format-version\":2,\"format-version\":2}".as_slice(),
        br#"{"extension":{"name":1,"name":2}}"#,
        b"{}{}",
    ] {
        head.metadata_digest = Sha256::digest(malformed).into();
        assert!(matches!(
            TableMetadataDocument::parse(malformed.to_vec(), &head, fixture::limits()),
            Err(TableMetadataError::Json(_))
        ));
    }
    head.metadata_digest = Sha256::digest(&canonical).into();
    for case in 0..5 {
        let mut limits = fixture::limits();
        match case {
            0 => {
                limits.bytes = canonical.len() - 1;
                limits.string_bytes = 1;
            }
            1 => limits.values = 1,
            2 => limits.string_bytes = 1,
            3 => limits.depth = 1,
            _ => limits.collection_entries = 0,
        }
        assert!(
            matches!(
                TableMetadataDocument::parse(canonical.clone(), &head, limits),
                Err(TableMetadataError::Bounds)
            ),
            "case {case}"
        );
    }
    head.metadata_digest[0] ^= 1;
    assert!(matches!(
        TableMetadataDocument::parse(canonical, &head, fixture::limits()),
        Err(TableMetadataError::Binding)
    ));
}

#[tokio::test]
async fn selected_canonical_reads_bind_head_and_verify_complete_digest() {
    let bytes = serde_json::to_vec(&fixture::metadata(2)).unwrap();
    let head = fixture::head(
        &bytes,
        2,
        Some(uuid::Uuid::parse_str("12345678-1234-1234-1234-123456789abc").unwrap()),
    );
    let store = Arc::new(blocks::TestBlocks::default());
    let mut writer = FileTreeWriter::new(
        store.clone(),
        FileIdentity {
            table: fixture::table(),
            file: head.metadata_file,
        },
        37,
    )
    .unwrap();
    writer.push(&bytes).await.unwrap();
    let tree = writer.finish().await.unwrap();
    let record = FileRecord {
        file: head.metadata_file,
        location: head.metadata_location.clone(),
        kind: FileKind::Metadata,
        format: ContentFormat::Json,
        length: tree.length,
        digest: tree.digest,
        content: FileContent::Chunks { root: tree.root },
        hint: None,
    };
    let mut selected = SelectedTable {
        head,
        metadata: record,
    };
    assert_eq!(
        read_table_metadata_document(store.clone(), &selected, fixture::limits())
            .await
            .unwrap()
            .canonical(),
        bytes
    );
    store.corrupt_reads.store(true, Ordering::SeqCst);
    assert!(
        read_table_metadata_document(store.clone(), &selected, fixture::limits())
            .await
            .is_err()
    );
    store.corrupt_reads.store(false, Ordering::SeqCst);
    selected.head.metadata_digest[0] ^= 1;
    let reads = store.reads.load(Ordering::SeqCst);
    assert!(matches!(
        read_table_metadata_document(store.clone(), &selected, fixture::limits()).await,
        Err(TableMetadataError::Binding)
    ));
    assert_eq!(store.reads.load(Ordering::SeqCst), reads);
}
