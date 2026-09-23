#[path = "common/table_metadata.rs"]
#[allow(dead_code)]
mod fixture;

use crowdb_access_iceberg::commit::{
    validate_requirements, RequirementError, RequirementLimits, TableRequirement,
};
use serde_json::json;

fn limits() -> RequirementLimits {
    RequirementLimits {
        count: 100,
        text_bytes: 1024,
    }
}

#[test]
fn complete_requirement_union_checks_one_selected_generation() {
    let document = fixture::parse(&fixture::metadata(3)).unwrap();
    let requirements: Vec<TableRequirement> = serde_json::from_value(json!([
        {"type":"assert-table-uuid","uuid":"12345678-1234-1234-1234-123456789abc"},
        {"type":"assert-ref-snapshot-id","ref":"main","snapshot-id":null},
        {"type":"assert-last-assigned-field-id","last-assigned-field-id":1},
        {"type":"assert-current-schema-id","current-schema-id":0},
        {"type":"assert-last-assigned-partition-id","last-assigned-partition-id":999},
        {"type":"assert-default-spec-id","default-spec-id":0},
        {"type":"assert-default-sort-order-id","default-sort-order-id":0}
    ]))
    .unwrap();
    assert!(validate_requirements(&requirements, Some(&document), limits()).is_ok());
    assert_eq!(
        validate_requirements(&requirements, None, limits()),
        Err(RequirementError::Failed(0))
    );
    let create = [TableRequirement::AssertCreate];
    assert!(validate_requirements(&create, None, limits()).is_ok());
    assert_eq!(
        validate_requirements(&create, Some(&document), limits()),
        Err(RequirementError::Failed(0))
    );
    assert_eq!(
        validate_requirements(
            &requirements,
            Some(&document),
            RequirementLimits { count: 1, ..limits() }
        ),
        Err(RequirementError::Bounds)
    );
}

#[test]
fn missing_nullable_snapshot_id_and_unknown_tags_are_not_silent_noops() {
    for invalid in [
        json!({"type":"assert-ref-snapshot-id","ref":"main"}),
        json!({"type":"assert-new-unknown-rule"}),
        json!({"type":"assert-current-schema-id","current-schema-id":2_147_483_648_u64}),
    ] {
        assert!(serde_json::from_value::<TableRequirement>(invalid).is_err());
    }
    let invalid = [TableRequirement::AssertCurrentSchemaId {
        current_schema_id: -1,
    }];
    assert_eq!(
        validate_requirements(&invalid, None, limits()),
        Err(RequirementError::Invalid(0))
    );
}

#[test]
fn implicit_legacy_main_and_reference_absence_are_distinct() {
    let mut value = fixture::metadata(1);
    value.as_object_mut().unwrap().remove("refs");
    value["current-snapshot-id"] = json!(10);
    value["snapshots"] = json!([fixture::snapshot(10, 0)]);
    let document = fixture::parse(&value).unwrap();
    let present = [TableRequirement::AssertRefSnapshotId {
        reference: "main".into(),
        snapshot_id: Some(10),
    }];
    let absent = [TableRequirement::AssertRefSnapshotId {
        reference: "main".into(),
        snapshot_id: None,
    }];
    assert!(validate_requirements(&present, Some(&document), limits()).is_ok());
    assert_eq!(
        validate_requirements(&absent, Some(&document), limits()),
        Err(RequirementError::Failed(0))
    );
    assert_eq!(
        validate_requirements(
            &present,
            Some(&document),
            RequirementLimits {
                text_bytes: 1,
                ..limits()
            }
        ),
        Err(RequirementError::Bounds)
    );
}
