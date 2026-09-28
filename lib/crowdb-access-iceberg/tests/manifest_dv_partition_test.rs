#[path = "common/file_blocks.rs"]
mod blocks;
#[path = "common/manifest_dv.rs"]
mod fixture;
#[allow(dead_code)]
#[path = "common/deletion_vector.rs"]
mod fixtures;
use crowdb_access_iceberg::manifest::{
    ManifestContext, ManifestVersion, PartitionValue, SnapshotDvValidator,
};
use serde_json::json;
use std::sync::Arc;

fn context(kind: &str, spec: i32, transform: &str) -> ManifestContext {
    ManifestContext::parse(ManifestVersion::V3,0,spec,
        &serde_json::to_vec(&json!({"type":"struct","schema-id":0,"fields":[{"id":3,"name":"v","required":false,"type":kind}]})).unwrap(),
        &serde_json::to_vec(&json!([{"source-id":3,"field-id":1000,"name":"p","transform":transform}])).unwrap()).unwrap()
}

#[tokio::test]
async fn vector_partition_equality_normalizes_nan_and_historical_numeric_promotions() {
    let store = Arc::new(blocks::TestBlocks::default());
    let blob = fixtures::blob(&[(0, fixtures::array(0, &[1]))]);
    for (left_type, left, right_type, right, equal) in [
        (
            "double",
            PartitionValue::Double(0x7ff0_0000_0000_0001),
            "double",
            PartitionValue::Double(0xfff8_0000_0000_0042),
            true,
        ),
        (
            "double",
            PartitionValue::Double((-0_f64).to_bits()),
            "double",
            PartitionValue::Double(0_f64.to_bits()),
            false,
        ),
        (
            "float",
            PartitionValue::Float(1_f32.to_bits()),
            "double",
            PartitionValue::Double(1_f64.to_bits()),
            true,
        ),
        (
            "int",
            PartitionValue::Int(1),
            "long",
            PartitionValue::Long(1),
            true,
        ),
        (
            "int",
            PartitionValue::Int(1),
            "long",
            PartitionValue::Long(2),
            false,
        ),
        (
            "decimal(4,2)",
            PartitionValue::Bytes(vec![0, 100]),
            "decimal(8,2)",
            PartitionValue::Bytes(vec![0, 0, 0, 100]),
            true,
        ),
        (
            "decimal(4,2)",
            PartitionValue::Bytes(vec![0, 100]),
            "decimal(8,3)",
            PartitionValue::Bytes(vec![0, 0, 0, 100]),
            false,
        ),
        ("long", PartitionValue::Null, "long", PartitionValue::Null, true),
        (
            "long",
            PartitionValue::Null,
            "long",
            PartitionValue::Long(1),
            false,
        ),
    ] {
        let mut pair = fixture::pair(store.clone(), &blob, 1, 2).await;
        pair.vector_context = context(left_type, 0, "identity");
        pair.data_context = context(right_type, 0, "identity");
        pair.vector_entry.file.partition = Some(vec![(1000, left)]);
        pair.data_entry.file.partition = Some(vec![(1000, right)]);
        let mut checker = SnapshotDvValidator::new(store.clone(), pair.scope, 1, fixture::limits()).unwrap();
        assert_eq!(
            checker
                .check(pair.scope, pair.vector(), pair.data())
                .await
                .is_ok(),
            equal,
            "{left_type}/{right_type}"
        );
        assert_eq!(checker.finish().is_ok(), equal);
    }
}

#[tokio::test]
async fn vector_partition_specs_ids_transforms_and_unknown_results_must_match() {
    let store = Arc::new(blocks::TestBlocks::default());
    let blob = fixtures::blob(&[(0, fixtures::array(0, &[1]))]);
    for fault in 0..6 {
        let mut pair = fixture::pair(store.clone(), &blob, 1, 2).await;
        pair.vector_context = context("long", 0, "future");
        pair.data_context = pair.vector_context.clone();
        pair.vector_entry.file.partition = Some(vec![(1000, PartitionValue::Opaque(vec![1, 2]))]);
        pair.data_entry.file.partition = Some(vec![(1000, PartitionValue::Opaque(vec![1, 2]))]);
        match fault {
            0 => {}
            1 => pair.data_context = context("long", 1, "future"),
            2 => pair.data_context = context("long", 0, "another"),
            3 => pair.data_entry.file.partition.as_mut().unwrap()[0].0 = 1001,
            4 => pair.data_entry.file.partition = Some(vec![]),
            _ => pair.data_entry.file.partition.as_mut().unwrap()[0].1 = PartitionValue::Opaque(vec![1, 3]),
        }
        let mut checker = SnapshotDvValidator::new(store.clone(), pair.scope, 1, fixture::limits()).unwrap();
        assert_eq!(
            checker
                .check(pair.scope, pair.vector(), pair.data())
                .await
                .is_ok(),
            fault == 0
        );
        assert_eq!(checker.finish().is_ok(), fault == 0);
    }
}
