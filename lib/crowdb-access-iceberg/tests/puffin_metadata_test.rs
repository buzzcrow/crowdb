#[path = "common/file_blocks.rs"]
mod blocks;
#[path = "common/puffin.rs"]
mod fixtures;

use crowdb_access_iceberg::file::{read_puffin_metadata, FormatHint, PuffinMetadata, PuffinMetadataError};
use std::sync::Arc;

#[tokio::test]
async fn canonical_puffin_footers_bind_exact_deletion_vector_descriptors() {
    let store = Arc::new(blocks::TestBlocks::default());
    let referenced = fixtures::referenced();
    let plain = serde_json::to_vec(&fixtures::metadata(&referenced.to_string())).unwrap();
    for compressed in [false, true] {
        let bytes = if compressed {
            fixtures::compressed(&plain, true)
        } else {
            plain.clone()
        };
        let mut record = fixtures::record(store.clone(), &bytes, compressed).await;
        record.hint = Some(FormatHint { offset: 4, length: 1 });
        let footer = read_puffin_metadata(store.clone(), &record, 1024, 1024)
            .await
            .unwrap();
        assert_eq!(footer.properties["created-by"], "test");
        let span = FormatHint {
            offset: 4,
            length: 20,
        };
        assert_eq!(
            footer
                .deletion_vector_at(&referenced, span, 2)
                .unwrap()
                .snapshot_id,
            -1
        );
        assert!(footer.deletion_vector_at(&referenced, span, 3).is_err());
        assert!(footer
            .deletion_vector_at(&fixtures::referenced(), span, 2)
            .is_err());
        assert!(footer
            .deletion_vector_at(&referenced, FormatHint { offset: 5, ..span }, 2)
            .is_err());
        assert!(footer
            .deletion_vector_at(&referenced, FormatHint { length: 19, ..span }, 2)
            .is_err());
    }
}

#[tokio::test]
async fn puffin_metadata_rejects_escaped_overlapping_and_incoherent_blob_descriptors() {
    let store = Arc::new(blocks::TestBlocks::default());
    let original = fixtures::metadata(&fixtures::referenced().to_string());
    for (field, value) in [
        ("offset", serde_json::json!(3)),
        ("length", serde_json::json!(21)),
        ("offset", serde_json::json!(u64::MAX)),
        ("snapshot-id", serde_json::json!(1)),
        ("sequence-number", serde_json::json!(0)),
        ("compression-codec", serde_json::json!("lz4")),
        ("type", serde_json::json!("")),
        ("fields", serde_json::json!([-1])),
    ] {
        let mut invalid = original.clone();
        invalid["blobs"][0][field] = value;
        let record = fixtures::record(store.clone(), &serde_json::to_vec(&invalid).unwrap(), false).await;
        assert!(
            read_puffin_metadata(store.clone(), &record, 1024, 1024)
                .await
                .is_err(),
            "accepted invalid {field}"
        );
    }
    for value in ["", "-1", "+2", " 2", "2 ", "9223372036854775808"] {
        let mut invalid = original.clone();
        invalid["blobs"][0]["properties"]["cardinality"] = serde_json::json!(value);
        let record = fixtures::record(store.clone(), &serde_json::to_vec(&invalid).unwrap(), false).await;
        assert!(read_puffin_metadata(store.clone(), &record, 1024, 1024)
            .await
            .is_err());
    }
    let mut overlapping = original.clone();
    overlapping["blobs"]
        .as_array_mut()
        .unwrap()
        .push(original["blobs"][0].clone());
    let record = fixtures::record(store.clone(), &serde_json::to_vec(&overlapping).unwrap(), false).await;
    assert!(read_puffin_metadata(store, &record, 1024, 1024).await.is_err());
}

#[tokio::test]
async fn puffin_lz4_requires_one_complete_sized_checksum_verified_frame() {
    let store = Arc::new(blocks::TestBlocks::default());
    let plain = serde_json::to_vec(&fixtures::metadata(&fixtures::referenced().to_string())).unwrap();
    let valid = fixtures::compressed(&plain, true);
    let mut checksum = valid.clone();
    *checksum.last_mut().unwrap() ^= 1;
    for bytes in [
        fixtures::compressed(&plain, false),
        valid[..valid.len() - 1].to_vec(),
        [valid.clone(), vec![0]].concat(),
        [valid.clone(), valid.clone()].concat(),
        checksum,
    ] {
        let record = fixtures::record(store.clone(), &bytes, true).await;
        assert!(read_puffin_metadata(store.clone(), &record, 2048, 1024)
            .await
            .is_err());
    }
    let record = fixtures::record(store.clone(), &valid, true).await;
    assert!(matches!(
        read_puffin_metadata(store.clone(), &record, valid.len() - 1, 1024).await,
        Err(PuffinMetadataError::Bounds)
    ));
    assert!(matches!(
        read_puffin_metadata(store.clone(), &record, 1024, plain.len() - 1).await,
        Err(PuffinMetadataError::Bounds)
    ));
    assert!(matches!(
        read_puffin_metadata(store, &record, 0, 1024).await,
        Err(PuffinMetadataError::Bounds)
    ));
}

#[test]
fn puffin_json_bounds_collections_and_rejects_duplicate_properties() {
    assert!(
        serde_json::from_str::<PuffinMetadata>(r#"{"blobs":[],"properties":{"key":"one","key":"two"}}"#)
            .is_err()
    );
    let original = fixtures::metadata(&fixtures::referenced().to_string());
    let mut too_many = original.clone();
    too_many["blobs"] = serde_json::json!(vec![original["blobs"][0].clone(); 4097]);
    assert!(serde_json::from_value::<PuffinMetadata>(too_many).is_err());
    let mut fields = original.clone();
    fields["blobs"][0]["fields"] = serde_json::json!(vec![1; 4097]);
    assert!(serde_json::from_value::<PuffinMetadata>(fields).is_err());
    let mut properties = original;
    properties["properties"]["key"] = serde_json::json!("x".repeat(4097));
    assert!(serde_json::from_value::<PuffinMetadata>(properties).is_err());
    assert!(serde_json::from_str::<PuffinMetadata>(r#"{"blobs":[]}"#).is_ok());
}
