#[path = "common/file_blocks.rs"]
mod blocks;
#[path = "common/projection.rs"]
mod fixture;
#[path = "common/store.rs"]
mod store;

use std::sync::{atomic::Ordering, Arc};

use crowdb_access_iceberg::key::{IcebergKey, TableId};
use crowdb_access_iceberg::metadata_projection::{MetadataRead, MAX_PROJECTION_BYTES, PROJECTION_PAGE_BYTES};
use fixture::{TestProjection, TestUnavailableProjectionStore};
use sha2::{Digest, Sha256};

const JSON: &[u8] = b"{ \n \"refs\" : {\"main\": { \"snapshot-id\": 123 }}, \"unknown\" : [1,  2] }\n";

#[tokio::test]
async fn selected_children_preserve_bytes_without_reading_canonical_blocks() {
    let fixture = TestProjection::new(JSON).await;
    assert!(fixture.projection.put(&fixture.record, 7, JSON).await);
    assert!(fixture.projection.put(&fixture.record, 7, JSON).await);
    let MetadataRead::Selected(values) = fixture
        .projection
        .select(fixture.record.clone(), 7, &["refs", "unknown", "refs"])
        .await
        .unwrap()
    else {
        panic!("expected projection");
    };
    assert_eq!(values["refs"], br#"{"main": { "snapshot-id": 123 }}"#);
    assert_eq!(values["unknown"], b"[1,  2]");
    assert_eq!(fixture.blocks.reads.load(Ordering::SeqCst), 0);
    for key in fixture.store.values.load().keys() {
        assert_eq!(IcebergKey::decode(key).unwrap().encode().unwrap(), *key);
    }
    fixture.fallback(7, &[], JSON).await;
}

#[tokio::test]
async fn missing_generation_field_or_root_returns_exact_canonical_bytes() {
    let fixture = TestProjection::new(JSON).await;
    fixture.fallback(7, &["refs"], JSON).await;
    assert!(fixture.projection.put(&fixture.record, 7, JSON).await);
    fixture.fallback(8, &["refs"], JSON).await;
    fixture.fallback(7, &["missing"], JSON).await;
    fixture.fallback(7, &["refs", "missing"], JSON).await;
}

#[tokio::test]
async fn every_missing_or_corrupt_page_falls_back_without_partial_results() {
    for remove in [true, false] {
        let fixture = TestProjection::new(JSON).await;
        assert!(fixture.projection.put(&fixture.record, 7, JSON).await);
        let original = fixture.store.values.load_full();
        for key in original.keys() {
            let mut damaged = (*original).clone();
            if remove {
                damaged.remove(key);
            } else {
                damaged.get_mut(key).unwrap().bytes[0] ^= 1;
            }
            fixture.store.values.store(Arc::new(damaged));
            fixture.fallback(7, &["refs", "unknown"], JSON).await;
        }
    }
}

#[tokio::test]
async fn root_identity_version_and_bounds_are_checked_even_with_valid_checksum() {
    for field in [
        "version",
        "generation",
        "digest",
        "catalog",
        "table",
        "length",
        "children",
    ] {
        let fixture = TestProjection::new(JSON).await;
        assert!(fixture.projection.put(&fixture.record, 7, JSON).await);
        let mut values = (**fixture.store.values.load()).clone();
        let (_, root) = values.iter_mut().find(|(key, _)| key.ends_with(&[0; 4])).unwrap();
        let mut body: serde_json::Value = serde_json::from_slice(&root.bytes[32..]).unwrap();
        match field {
            "catalog" | "table" | "digest" => {
                body[field][0] = serde_json::json!(body[field][0].as_u64().unwrap() ^ 1);
            }
            "children" => body[field]["refs"]["length"] = serde_json::json!(usize::MAX),
            _ => body[field] = serde_json::json!(999),
        }
        let body = serde_json::to_vec(&body).unwrap();
        root.bytes = Sha256::digest(&body).to_vec();
        root.bytes.extend(body);
        fixture.store.values.store(Arc::new(values));
        fixture.fallback(7, &["refs"], JSON).await;
    }
}

#[tokio::test]
async fn children_span_bounded_pages_and_corrupt_last_page_falls_back() {
    let child = format!("\"{}\"", "x".repeat(PROJECTION_PAGE_BYTES * 2));
    let input = format!("{{\"refs\":{child}}}");
    let fixture = TestProjection::new(input.as_bytes()).await;
    assert!(fixture.projection.put(&fixture.record, 1, input.as_bytes()).await);
    let MetadataRead::Selected(values) = fixture
        .projection
        .select(fixture.record.clone(), 1, &["refs"])
        .await
        .unwrap()
    else {
        panic!("expected projection");
    };
    assert_eq!(values["refs"], child.as_bytes());
    assert_eq!(fixture.store.values.load().len(), 4);
    assert!(fixture
        .store
        .values
        .load()
        .values()
        .all(|value| value.bytes.len() <= PROJECTION_PAGE_BYTES));
    let mut values = (**fixture.store.values.load()).clone();
    values.last_entry().unwrap().get_mut().bytes.pop();
    fixture.store.values.store(Arc::new(values));
    fixture.fallback(1, &["refs"], input.as_bytes()).await;
}

#[tokio::test]
async fn lost_write_at_every_boundary_is_optional_and_retryable() {
    for boundary in 1..=3 {
        let fixture = TestProjection::new(JSON).await;
        fixture.store.fail_after.store(boundary, Ordering::SeqCst);
        assert!(!fixture.projection.put(&fixture.record, 7, JSON).await);
        if boundary < 3 {
            fixture.fallback(7, &["refs"], JSON).await;
        }
        assert!(fixture.projection.put(&fixture.record, 7, JSON).await);
        assert!(matches!(
            fixture
                .projection
                .select(fixture.record.clone(), 7, &["refs"])
                .await
                .unwrap(),
            MetadataRead::Selected(_)
        ));
    }
}

#[tokio::test]
async fn oversized_duplicate_and_nonobject_json_skip_optimization() {
    let oversized = format!("{{\"refs\":\"{}\"}}", "x".repeat(MAX_PROJECTION_BYTES));
    let many_fields = format!(
        "{{{}}}",
        (0..65)
            .map(|index| format!("\"{index}\":null"))
            .collect::<Vec<_>>()
            .join(",")
    );
    for input in [
        oversized.as_bytes(),
        many_fields.as_bytes(),
        br#"{"refs":{},"refs":[]}"#,
        b"[]",
        b"invalid",
    ] {
        let fixture = TestProjection::new(input).await;
        assert!(!fixture.projection.put(&fixture.record, 7, input).await);
        assert!(fixture.store.values.load().is_empty());
        fixture.fallback(7, &["refs"], input).await;
    }
}

#[tokio::test]
async fn wrong_canonical_digest_and_foreign_table_cannot_use_projection() {
    let fixture = TestProjection::new(JSON).await;
    assert!(!fixture.projection.put(&fixture.record, 7, b"{}").await);
    assert!(fixture.projection.put(&fixture.record, 7, JSON).await);
    let mut record = fixture.record.clone();
    record.digest[0] ^= 1;
    assert!(matches!(
        fixture.projection.select(record, 7, &["refs"]).await,
        Err(crowdb_access_iceberg::file::FileIoError::Invalid(
            crowdb_access_iceberg::error::ValidationError::Record
        ))
    ));
    let mut record = fixture.record.clone();
    let mut table = record.location.table();
    table.table = TableId::random();
    record.location = table.file("metadata/test.json").unwrap();
    assert!(matches!(
        fixture.projection.select(record, 7, &["refs"]).await.unwrap(),
        MetadataRead::Canonical(_)
    ));
}

#[tokio::test]
async fn unavailable_projection_storage_does_not_block_canonical_reads() {
    let mut fixture = TestProjection::new(JSON).await;
    fixture.projection = crowdb_access_iceberg::metadata_projection::ProjectionStore::new(
        Arc::new(TestUnavailableProjectionStore),
        fixture.blocks.clone(),
    );
    assert!(!fixture.projection.put(&fixture.record, 7, JSON).await);
    fixture.fallback(7, &["refs"], JSON).await;
    fixture.blocks.corrupt_reads.store(true, Ordering::SeqCst);
    let MetadataRead::Canonical(mut reader) = fixture
        .projection
        .select(fixture.record.clone(), 7, &["refs"])
        .await
        .unwrap()
    else {
        panic!("expected fallback");
    };
    assert!(reader.next().await.is_err());
}

#[tokio::test]
async fn oversized_stored_pages_and_invalid_key_dimensions_are_rejected() {
    let fixture = TestProjection::new(JSON).await;
    assert!(fixture.projection.put(&fixture.record, 7, JSON).await);
    let original = fixture.store.values.load_full();
    for key in original.keys() {
        let mut values = (*original).clone();
        values.get_mut(key).unwrap().bytes = vec![0; PROJECTION_PAGE_BYTES + 1];
        fixture.store.values.store(Arc::new(values));
        fixture.fallback(7, &["refs", "unknown"], JSON).await;
    }
    let root = original.keys().find(|key| key.ends_with(&[0; 4])).unwrap();
    let IcebergKey::Catalog {
        catalog,
        scope,
        suffix,
    } = IcebergKey::decode(root).unwrap()
    else {
        panic!("catalog key");
    };
    for (index, value) in [(57, 0), (59, 65), (61, 64), (61, 2)] {
        let mut invalid = suffix.clone();
        invalid[index] = value;
        assert!(IcebergKey::Catalog {
            catalog,
            scope,
            suffix: invalid
        }
        .encode()
        .is_err());
    }
    for version in [1, 2] {
        let mut receipt = suffix.clone();
        receipt[57] = version;
        receipt[61] = 1;
        assert_eq!(
            IcebergKey::Catalog {
                catalog,
                scope,
                suffix: receipt
            }
            .encode()
            .is_ok(),
            version == 2
        );
    }
}
