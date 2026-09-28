#[path = "common/table_metadata.rs"]
#[allow(dead_code)]
mod fixture;
#[path = "common/staged_commit_fixture.rs"]
mod sdk;

use base64::Engine;
use crowdb_access_iceberg::{
    commit::{
        evaluate_table_create_commit, CommitRequest, CommitRequestLimits, EvaluationLimits, RequirementLimits,
    },
    key::OperationId,
    table::TableHead,
};
use serde_json::{json, Value};

fn target() -> TableHead {
    let mut target = fixture::head(&[], 2, Some(uuid::Uuid::new_v4()));
    target.pending_operation = Some(OperationId::random());
    target
}

fn request(target: &TableHead) -> Value {
    json!({"requirements":[{"type":"assert-create"}],"updates":[
        {"action":"assign-uuid","uuid":target.table_uuid.unwrap().to_string()},
        {"action":"upgrade-format-version","format-version":2},
        {"action":"add-schema","schema":{"type":"struct","schema-id":77,"fields":[
            {"id":91,"name":"id","type":"long","required":true}]}},
        {"action":"set-current-schema","schema-id":-1},
        {"action":"add-spec","spec":{"spec-id":71,"fields":[]}},
        {"action":"set-default-spec","spec-id":-1},
        {"action":"add-sort-order","sort-order":{"order-id":0,"fields":[]}},
        {"action":"set-default-sort-order","sort-order-id":-1},
        {"action":"set-location","location":fixture::table().to_string()}
    ]})
}

fn decode(value: &Value) -> CommitRequest {
    CommitRequest::decode(
        &serde_json::to_vec(value).unwrap(),
        CommitRequestLimits {
            json: fixture::limits(),
            requirements: 100,
            updates: 100,
        },
    )
    .unwrap()
}

fn limits() -> EvaluationLimits {
    EvaluationLimits {
        metadata: fixture::limits(),
        requirements: RequirementLimits {
            count: 100,
            text_bytes: 4096,
        },
        updates: 100,
        work_bytes: 8 * 1024 * 1024,
    }
}

#[test]
fn initial_commit_preserves_staged_field_ids_and_has_no_draft_metadata_log() {
    let target = target();
    let request = decode(&request(&target));
    let result = evaluate_table_create_commit(&request, target, 1000, limits()).unwrap();
    let root = result.document.fields();
    assert_eq!(root["schemas"][0]["fields"][0]["id"], 91);
    assert_eq!(root["last-column-id"], 91);
    assert_eq!(root["current-schema-id"], 0);
    assert_eq!(root["metadata-log"], json!([]));
    assert_eq!(result.head.generation, 1);
    assert_eq!(root["properties"], json!({}));
}

#[test]
fn incomplete_initialization_cannot_be_repaired_from_the_draft() {
    let target = target();
    let original = request(&target);
    for index in 0..9 {
        if index == 1 {
            continue;
        }
        let mut request = original.clone();
        request["updates"].as_array_mut().unwrap().remove(index);
        assert!(
            evaluate_table_create_commit(&decode(&request), target.clone(), 1000, limits()).is_err(),
            "removed {index}"
        );
    }
}

#[test]
fn initial_commit_rejects_other_requirements_and_foreign_table_identity() {
    let target = target();
    for requirements in [
        json!([]),
        json!([{"type":"assert-current-schema-id","current-schema-id":0}]),
        json!([{"type":"assert-create"},{"type":"assert-default-spec-id","default-spec-id":0}]),
    ] {
        let mut request = request(&target);
        request["requirements"] = requirements;
        assert!(evaluate_table_create_commit(&decode(&request), target.clone(), 1000, limits()).is_err());
    }
    let mut request = request(&target);
    request["updates"][0]["uuid"] = json!(uuid::Uuid::new_v4().to_string());
    assert!(evaluate_table_create_commit(&decode(&request), target.clone(), 1000, limits()).is_err());
    let mut changed = target;
    changed.generation = 2;
    assert!(evaluate_table_create_commit(&decode(&request), changed, 1000, limits()).is_err());
}

#[test]
fn initial_commit_enforces_independent_limits() {
    let target = target();
    let request = decode(&request(&target));
    let mut limited = limits();
    limited.updates = 8;
    assert!(evaluate_table_create_commit(&request, target.clone(), 1000, limited).is_err());
    limited = limits();
    limited.work_bytes = 10;
    assert!(evaluate_table_create_commit(&request, target, 1000, limited).is_err());
}

#[test]
fn initial_auxiliary_metadata_is_validated_before_acceptance() {
    for (encoded, accepted) in [("AQ==", true), ("!", false)] {
        let target = target();
        let mut request = request(&target);
        request["updates"][1]["format-version"] = json!(3);
        request["updates"].as_array_mut().unwrap().push(json!({
            "action":"add-encryption-key",
            "encryption-key":{"key-id":"key","encrypted-key-metadata":encoded}
        }));
        let result = evaluate_table_create_commit(&decode(&request), target, 1000, limits());
        assert_eq!(result.is_ok(), accepted);
    }
}

#[test]
fn staged_transaction_updates_match_pinned_java_empty_builder() {
    for (version, request, expected) in [
        (1, sdk::REQUEST_V1, sdk::OUTPUT_V1),
        (2, sdk::REQUEST_V2, sdk::OUTPUT_V2),
        (3, sdk::REQUEST_V3, sdk::OUTPUT_V3),
    ] {
        let decode_base64 = |text| base64::engine::general_purpose::STANDARD.decode(text).unwrap();
        let expected: Value = serde_json::from_slice(&decode_base64(expected)).unwrap();
        let request: Value = serde_json::from_slice(&decode_base64(request)).unwrap();
        let mut target = target();
        target.table_uuid = Some(uuid::Uuid::parse_str(expected["table-uuid"].as_str().unwrap()).unwrap());
        let result = evaluate_table_create_commit(
            &decode(&request),
            target,
            expected["last-updated-ms"].as_i64().unwrap(),
            limits(),
        )
        .unwrap();
        assert_eq!(
            Value::Object(result.document.fields().clone()),
            expected,
            "version {version}"
        );
        assert_eq!(result.head.generation, 1);
        assert_eq!(result.head.format_version, version);
    }
}
