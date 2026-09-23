#[path = "common/table_metadata.rs"]
#[allow(dead_code)]
mod fixture;

use crowdb_access_iceberg::{
    commit::{CommitRequest, CommitRequestLimits},
    namespace::NamespaceIdentifier,
    table::TableMetadataError,
};
use serde_json::{json, Value};

fn limits() -> CommitRequestLimits {
    CommitRequestLimits {
        json: fixture::limits(),
        requirements: 100,
        updates: 100,
    }
}

fn decode(updates: Value) -> Result<CommitRequest, TableMetadataError> {
    let mut request = json!({"requirements":[]});
    request["updates"] = updates;
    CommitRequest::decode(&serde_json::to_vec(&request).unwrap(), limits())
}

#[test]
fn every_pinned_table_update_variant_decodes_without_accepting_view_updates() {
    let values = json!([
        {"action":"assign-uuid","uuid":"12345678-1234-1234-1234-123456789abc"},
        {"action":"upgrade-format-version","format-version":3},
        {"action":"add-schema","schema":{}},
        {"action":"set-current-schema","schema-id":-1},
        {"action":"add-spec","spec":{}},
        {"action":"set-default-spec","spec-id":-1},
        {"action":"add-sort-order","sort-order":{}},
        {"action":"set-default-sort-order","sort-order-id":-1},
        {"action":"add-snapshot","snapshot":{}},
        {"action":"set-snapshot-ref","ref-name":"main","type":"branch","snapshot-id":10},
        {"action":"remove-snapshots","snapshot-ids":[10]},
        {"action":"remove-snapshot-ref","ref-name":"tag"},
        {"action":"set-location","location":"s3://bucket/table"},
        {"action":"set-properties","updates":{"a":"b"}},
        {"action":"remove-properties","removals":["a"]},
        {"action":"set-statistics","statistics":{}},
        {"action":"remove-statistics","snapshot-id":10},
        {"action":"set-partition-statistics","partition-statistics":{}},
        {"action":"remove-partition-statistics","snapshot-id":10},
        {"action":"remove-partition-specs","spec-ids":[1]},
        {"action":"remove-schemas","schema-ids":[1]},
        {"action":"add-encryption-key","encryption-key":{}},
        {"action":"remove-encryption-key","key-id":"key"}
    ]);
    assert_eq!(decode(values).unwrap().updates.len(), 23);
    assert!(decode(json!([{"action":"add-view-version","view-version":{}}])).is_err());
    assert!(decode(json!([{"action":"unknown"}])).is_err());
    assert!(decode(json!([{"action":"add-snapshot","snapshot":null}])).is_err());
    assert!(decode(json!([{"action":"set-properties","updates":{"a":1}}])).is_err());
}

#[test]
fn duplicate_keys_nested_work_and_request_counts_fail_before_evaluation() {
    for bytes in [
        br#"{"requirements":[],"updates":[],"updates":[]}"#.as_slice(),
        br#"{"requirements":[],"updates":[{"action":"add-schema","schema":{"fields":[],"fields":[]}}]}"#
            .as_slice(),
        br#"{"requirements":[{"type":"assert-ref-snapshot-id","ref":"main"}],"updates":[]}"#.as_slice(),
    ] {
        assert!(CommitRequest::decode(bytes, limits()).is_err());
    }
    let bytes = serde_json::to_vec(&json!({"requirements":[],"updates":[{"action":"remove-properties","removals":[]},{"action":"remove-properties","removals":[]}]})).unwrap();
    assert!(matches!(
        CommitRequest::decode(
            &bytes,
            CommitRequestLimits {
                updates: 1,
                ..limits()
            }
        ),
        Err(TableMetadataError::Bounds)
    ));
    let mut budget = limits();
    budget.json.values = 2;
    assert!(matches!(
        CommitRequest::decode(&bytes, budget),
        Err(TableMetadataError::Bounds)
    ));
}

#[test]
fn optional_body_identifier_must_match_the_route_when_present() {
    let namespace = NamespaceIdentifier::new(vec!["analytics".into()]).unwrap();
    let bytes = serde_json::to_vec(
        &json!({"identifier":{"namespace":["analytics"],"name":"events"},"requirements":[],"updates":[]}),
    )
    .unwrap();
    let request = CommitRequest::decode(&bytes, limits()).unwrap();
    assert!(request.check_identifier(&namespace, "events").is_ok());
    assert!(request.check_identifier(&namespace, "other").is_err());
    assert!(decode(json!([]))
        .unwrap()
        .check_identifier(&namespace, "events")
        .is_ok());
}

#[test]
fn opaque_nested_payload_values_keep_their_original_numeric_spelling() {
    let bytes = br#"{"requirements":[],"updates":[{"action":"add-schema","schema": { "future": 123456789012345678901234567890, "fields": [] }}]}"#;
    let request = CommitRequest::decode(bytes, limits()).unwrap();
    let crowdb_access_iceberg::commit::TableUpdate::AddSchema { schema, .. } = &request.updates[0] else {
        panic!("expected schema update");
    };
    assert_eq!(
        schema.canonical(),
        Some(r#"{ "future": 123456789012345678901234567890, "fields": [] }"#)
    );
    assert!(schema.fields()["fields"].as_array().unwrap().is_empty());
}
