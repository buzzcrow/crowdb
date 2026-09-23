#[path = "common/manifest_entry.rs"]
mod fixture;

use crowdb_access_iceberg::file::{AvroContainerError, AvroDatumLimits, AvroSchema};
use crowdb_access_iceberg::manifest::{
    ManifestContent, ManifestEntryProjection, ManifestEntryState, ManifestVersion,
};
use fixture::{table, TestManifestEntry};
use serde_json::{json, Value};

fn limits() -> AvroDatumLimits {
    AvroDatumLimits {
        depth: 64,
        values: 100_000,
        value_bytes: 2 * 1024 * 1024,
    }
}

fn state() -> ManifestEntryState {
    ManifestEntryState::new(
        ManifestVersion::V3,
        table(),
        ManifestContent::Data,
        99,
        9,
        Some(100),
    )
    .unwrap()
}

#[test]
fn optional_metrics_preserve_empty_maps_and_do_not_treat_nested_counts_as_row_counts() {
    for version in [ManifestVersion::V1, ManifestVersion::V2, ManifestVersion::V3] {
        let mut fixture = TestManifestEntry::new(version);
        fixture.file.extend([
            (108, "long-map", json!([])),
            (109, "long-map", json!([[3, 100], [4, 10]])),
            (110, "long-map", json!([[3, 30]])),
            (137, "long-map", json!([[3, 20]])),
            (125, "bytes-map", json!([[3, "a"], [4, ""]])),
            (128, "bytes-map", json!(null)),
        ]);
        fixture.file.reverse();
        let schema = fixture.schema();
        let projection = ManifestEntryProjection::new(&schema, version, table()).unwrap();
        let bytes = fixture.bytes();
        let mut state =
            ManifestEntryState::new(version, table(), ManifestContent::Data, 99, 9, Some(100)).unwrap();
        let entry = projection
            .records(&bytes, 1, limits(), &mut state)
            .unwrap()
            .next_entry()
            .unwrap()
            .unwrap();
        assert!(entry.file.metrics.column_sizes.unwrap().is_empty());
        assert_eq!(entry.file.metrics.value_counts.unwrap()[&3], 100);
        assert_eq!(entry.file.metrics.lower_bounds.unwrap()[&4], b"");
        assert!(entry.file.metrics.upper_bounds.is_none());
        assert_eq!(state.next_row_id(), Some(110));
    }
}

#[test]
fn malformed_metrics_poison_cursor_without_consuming_row_ids() {
    for metric in [
        json!([[3, -1]]),
        json!([[0, 1]]),
        json!([[3, 1], [3, 1]]),
        json!([[3, 11]]),
    ] {
        let mut fixture = TestManifestEntry::new(ManifestVersion::V3);
        fixture
            .file
            .extend([(109, "long-map", json!([[3, 10]])), (110, "long-map", metric)]);
        fixture.set(103, json!(10));
        let schema = fixture.schema();
        let projection = ManifestEntryProjection::new(&schema, ManifestVersion::V3, table()).unwrap();
        let bytes = fixture.bytes();
        let mut state = state();
        let mut records = projection.records(&bytes, 1, limits(), &mut state).unwrap();
        assert!(records.next_entry().is_err());
        assert!(matches!(
            records.next_entry(),
            Err(crowdb_access_iceberg::manifest::ManifestEntryError::Avro(
                AvroContainerError::Failed
            ))
        ));
        assert_eq!(state.next_row_id(), Some(100));
    }
}

#[test]
fn metrics_enforce_shared_item_and_byte_budgets_and_count_overflow() {
    for fields in [
        vec![
            (
                109,
                "long-map",
                Value::Array((1..=2049).map(|id| json!([id, 1])).collect()),
            ),
            (
                110,
                "long-map",
                Value::Array((1..=2048).map(|id| json!([id, 0])).collect()),
            ),
        ],
        vec![
            (125, "bytes-map", json!([[3, "a".repeat(600_000)]])),
            (128, "bytes-map", json!([[3, "z".repeat(600_000)]])),
        ],
        vec![
            (109, "long-map", json!([[3, i64::MAX]])),
            (110, "long-map", json!([[3, i64::MAX]])),
            (137, "long-map", json!([[3, 1]])),
        ],
    ] {
        let mut fixture = TestManifestEntry::new(ManifestVersion::V3);
        fixture.file.extend(fields);
        let schema = fixture.schema();
        let projection = ManifestEntryProjection::new(&schema, ManifestVersion::V3, table()).unwrap();
        let bytes = fixture.bytes();
        let mut state = state();
        assert!(projection
            .records(&bytes, 1, limits(), &mut state)
            .unwrap()
            .next_entry()
            .is_err());
        assert_eq!(state.next_row_id(), Some(100));
    }
}

#[test]
fn metric_schema_requires_logical_map_integer_key_and_exact_nested_ids() {
    let mut fixture = TestManifestEntry::new(ManifestVersion::V3);
    fixture.file.push((109, "long-map", json!([])));
    for change in 0..5 {
        let mut schema: Value = serde_json::from_slice(&fixture.schema_bytes()).unwrap();
        let fields = schema["fields"].as_array_mut().unwrap();
        let file = fields.iter_mut().find(|field| field["field-id"] == 2).unwrap();
        let metric = file["type"][1]["fields"]
            .as_array_mut()
            .unwrap()
            .iter_mut()
            .find(|field| field["field-id"] == 109)
            .unwrap();
        let map = &mut metric["type"][1];
        match change {
            0 => {
                map.as_object_mut().unwrap().remove("logicalType");
            }
            1 => map["items"]["fields"][0]["field-id"] = json!(120),
            2 => map["items"]["fields"][0]["type"] = json!("string"),
            3 => map["items"]["fields"][1]["type"] = json!(["null", "long"]),
            _ => map["items"]["fields"][1]["type"] = json!("bytes"),
        }
        let schema = AvroSchema::parse(&serde_json::to_vec(&schema).unwrap()).unwrap();
        assert!(ManifestEntryProjection::new(&schema, ManifestVersion::V3, table()).is_err());
    }
}
