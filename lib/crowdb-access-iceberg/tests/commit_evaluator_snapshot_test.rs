#[path = "common/table_metadata.rs"]
#[allow(dead_code)]
mod fixture;

use crowdb_access_iceberg::{
    commit::{
        evaluate_metadata_updates, CommitRequest, CommitRequestLimits, EvaluationLimits, RequirementLimits,
    },
    key::FileId,
    table::TableMetadataDocument,
};
use serde_json::{json, Value};

fn evaluate(metadata: &Value, updates: Value) -> Option<Value> {
    let bytes = serde_json::to_vec(metadata).unwrap();
    let version = u8::try_from(metadata["format-version"].as_u64().unwrap()).unwrap();
    let mut head = fixture::head(
        &bytes,
        version,
        Some(uuid::Uuid::parse_str(metadata["table-uuid"].as_str().unwrap()).unwrap()),
    );
    let document = TableMetadataDocument::parse(bytes, &head, fixture::limits()).unwrap();
    let mut request = json!({"requirements":[]});
    request["updates"] = updates;
    let request = CommitRequest::decode(
        &serde_json::to_vec(&request).unwrap(),
        CommitRequestLimits {
            json: fixture::limits(),
            requirements: 100,
            updates: 100,
        },
    )
    .unwrap();
    head.generation += 1;
    head.metadata_file = FileId::random();
    head.metadata_location = fixture::table().file("metadata/next.json").unwrap();
    let result = evaluate_metadata_updates(
        &document,
        &request,
        head,
        1100,
        EvaluationLimits {
            metadata: fixture::limits(),
            requirements: RequirementLimits {
                count: 100,
                text_bytes: 4096,
            },
            updates: 100,
            work_bytes: 8 * 1024 * 1024,
        },
    )
    .ok()?;
    Some(serde_json::from_slice(result.document.canonical()).unwrap())
}

fn reference(id: i64) -> Value {
    json!({"action":"set-snapshot-ref","ref-name":"main","type":"branch","snapshot-id":id})
}

#[test]
fn snapshots_refs_and_transaction_logs_are_ordered() {
    for version in 1..=3 {
        let metadata = fixture::metadata(version);
        let mut first = fixture::snapshot(10, i64::from(version != 1));
        let mut second = fixture::snapshot(20, if version == 1 { 0 } else { 2 });
        second["parent-snapshot-id"] = json!(10);
        if version == 3 {
            first["first-row-id"] = json!(0);
            first["added-rows"] = json!(2);
            second["first-row-id"] = json!(2);
            second["added-rows"] = json!(3);
        }
        let result = evaluate(
            &metadata,
            json!([
                {"action":"add-snapshot","snapshot":first}, reference(10),
                {"action":"add-snapshot","snapshot":second}, reference(20),
                {"action":"set-snapshot-ref","ref-name":"release","type":"tag","snapshot-id":10}
            ]),
        )
        .unwrap();
        assert_eq!(result["current-snapshot-id"], 20);
        assert_eq!(result["refs"]["release"]["snapshot-id"], 10);
        assert_eq!(
            result["snapshot-log"],
            json!([{"timestamp-ms":1000,"snapshot-id":20}])
        );
        if version == 3 {
            assert_eq!(result["next-row-id"], 5);
        }
        assert!(evaluate(
            &metadata,
            json!([reference(10), {"action":"add-snapshot","snapshot":first}])
        )
        .is_none());
    }
}

#[test]
fn snapshot_removal_cleans_refs_statistics_and_history_gaps() {
    let mut metadata = fixture::metadata(2);
    metadata["snapshots"] = json!([
        fixture::snapshot(10, 1),
        fixture::snapshot(20, 2),
        fixture::snapshot(30, 3)
    ]);
    metadata["last-sequence-number"] = json!(3);
    metadata["current-snapshot-id"] = json!(30);
    metadata["refs"] =
        json!({"main":{"type":"branch","snapshot-id":30},"tag":{"type":"tag","snapshot-id":20}});
    metadata["snapshot-log"] = json!([
        {"timestamp-ms":1000,"snapshot-id":10},{"timestamp-ms":1000,"snapshot-id":20},{"timestamp-ms":1000,"snapshot-id":30}
    ]);
    let result = evaluate(
        &metadata,
        json!([{"action":"remove-snapshots","snapshot-ids":[20]}]),
    )
    .unwrap();
    assert_eq!(
        result["snapshot-log"],
        json!([{"timestamp-ms":1000,"snapshot-id":30}])
    );
    assert!(result["refs"].get("tag").is_none());
    assert_eq!(result["last-sequence-number"], 3);
    let result = evaluate(
        &metadata,
        json!([{"action":"remove-snapshots","snapshot-ids":[30]}]),
    )
    .unwrap();
    assert_eq!(result["current-snapshot-id"], -1);
    assert!(result["refs"].get("main").is_none());
}

#[test]
fn intermediate_snapshot_removal_does_not_reuse_allocated_rows_or_sequences() {
    let metadata = fixture::metadata(3);
    let mut snapshot = fixture::snapshot(10, 1);
    snapshot["first-row-id"] = json!(0);
    snapshot["added-rows"] = json!(4);
    let result = evaluate(
        &metadata,
        json!([{"action":"add-snapshot","snapshot":snapshot},
        {"action":"remove-snapshots","snapshot-ids":[10]}]),
    )
    .unwrap();
    assert_eq!(result["last-sequence-number"], 1);
    assert_eq!(result["next-row-id"], 4);
    let mut reused = snapshot.clone();
    reused["snapshot-id"] = json!(20);
    assert!(evaluate(
        &metadata,
        json!([{"action":"add-snapshot","snapshot":snapshot},
        {"action":"remove-snapshots","snapshot-ids":[10]}, {"action":"add-snapshot","snapshot":reused}])
    )
    .is_none());
}

#[test]
fn auxiliary_replacement_uses_nested_snapshot_and_duplicate_key_add_is_noop() {
    let metadata = fixture::metadata(3);
    let statistics = json!({"snapshot-id":10,"statistics-path":fixture::table().file("metadata/stats.puffin").unwrap().to_string(),
        "file-size-in-bytes":40,"file-footer-size-in-bytes":20,"blob-metadata":[]});
    let partition = json!({"snapshot-id":10,"statistics-path":fixture::table().file("metadata/partition.parquet").unwrap().to_string(),
        "file-size-in-bytes":40});
    let result = evaluate(
        &metadata,
        json!([
            {"action":"set-statistics","snapshot-id":999,"statistics":statistics},
            {"action":"set-partition-statistics","partition-statistics":partition},
            {"action":"add-encryption-key","encryption-key":{"key-id":"key","encrypted-key-metadata":"AQ=="}},
            {"action":"add-encryption-key","encryption-key":{"key-id":"key","encrypted-key-metadata":"Ag=="}}
        ]),
    )
    .unwrap();
    assert_eq!(result["statistics"][0]["snapshot-id"], 10);
    assert_eq!(result["partition-statistics"][0]["snapshot-id"], 10);
    assert_eq!(result["encryption-keys"][0]["encrypted-key-metadata"], "AQ==");
    let result = evaluate(&result, json!([
        {"action":"remove-statistics","snapshot-id":10},{"action":"remove-partition-statistics","snapshot-id":10},
        {"action":"remove-encryption-key","key-id":"key"}
    ])).unwrap();
    for name in ["statistics", "partition-statistics", "encryption-keys"] {
        assert_eq!(result[name], json!([]));
    }
}
