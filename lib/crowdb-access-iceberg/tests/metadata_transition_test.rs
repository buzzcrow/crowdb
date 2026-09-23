#[path = "common/table_metadata.rs"]
#[allow(dead_code)]
mod fixture;

use crowdb_access_iceberg::{
    commit::{validate_metadata_transition, TransitionLimits},
    key::FileId,
    table::{TableMetadataDocument, TableMetadataError},
};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

fn check(
    prior: &Value,
    candidate: &Value,
    upgrades: &[u8],
    entries: usize,
) -> Result<(), TableMetadataError> {
    let bytes = serde_json::to_vec(prior).unwrap();
    let mut head = fixture::head(
        &bytes,
        u8::try_from(prior["format-version"].as_u64().unwrap()).unwrap(),
        Some(uuid::Uuid::parse_str(prior["table-uuid"].as_str().unwrap()).unwrap()),
    );
    let prior = TableMetadataDocument::parse(bytes, &head, fixture::limits())?;
    let bytes = serde_json::to_vec(candidate).unwrap();
    head.generation += 1;
    head.metadata_file = FileId::random();
    head.metadata_location = fixture::table().file("metadata/next.json").unwrap();
    head.metadata_digest = Sha256::digest(&bytes).into();
    head.format_version = u8::try_from(candidate["format-version"].as_u64().unwrap()).unwrap();
    let candidate = TableMetadataDocument::parse(bytes, &head, fixture::limits())?;
    validate_metadata_transition(
        &prior,
        &candidate,
        upgrades,
        TransitionLimits {
            entries,
            upgrade_steps: 10,
        },
    )
}

#[test]
fn upgrades_require_explicit_adjacent_steps_and_preserve_high_water_marks() {
    let prior = fixture::metadata(1);
    let candidate = fixture::metadata(3);
    assert!(check(&prior, &candidate, &[2, 3], 1000).is_ok());
    assert!(check(&prior, &candidate, &[3], 1000).is_err());
    assert!(check(&prior, &candidate, &[], 1000).is_err());
    assert!(check(&candidate, &prior, &[2, 1], 1000).is_err());
    let mut prior = fixture::metadata(3);
    prior["last-column-id"] = json!(10);
    assert!(check(&prior, &candidate, &[], 1000).is_err());
    prior["last-column-id"] = json!(1);
    prior["next-row-id"] = json!(100);
    assert!(check(&prior, &candidate, &[], 1000).is_err());
}

#[test]
fn historical_missing_lineage_is_readable_but_new_snapshots_require_it() {
    let mut prior = fixture::metadata(3);
    prior["last-sequence-number"] = json!(1);
    prior["snapshots"] = json!([fixture::snapshot(10, 1)]);
    assert!(check(&prior, &prior, &[], 1000).is_ok());
    let mut candidate = prior.clone();
    candidate["last-sequence-number"] = json!(2);
    candidate["snapshots"]
        .as_array_mut()
        .unwrap()
        .push(fixture::snapshot(20, 2));
    assert!(check(&prior, &candidate, &[], 1000).is_err());
    candidate["snapshots"][1]["first-row-id"] = json!(0);
    candidate["snapshots"][1]["added-rows"] = json!(5);
    candidate["next-row-id"] = json!(5);
    assert!(check(&prior, &candidate, &[], 1000).is_ok());
    assert!(check(&prior, &candidate, &[], 1).is_err());
    candidate["snapshots"][0]["timestamp-ms"] = json!(1001);
    assert!(check(&prior, &candidate, &[], 1000).is_err());
}

#[test]
fn expiration_and_intermediate_allocations_do_not_reset_counters() {
    let mut prior = fixture::metadata(3);
    prior["last-sequence-number"] = json!(10);
    prior["next-row-id"] = json!(100);
    let mut candidate = prior.clone();
    candidate["last-sequence-number"] = json!(12);
    candidate["next-row-id"] = json!(120);
    let mut snapshot = fixture::snapshot(20, 12);
    snapshot["first-row-id"] = json!(110);
    snapshot["added-rows"] = json!(10);
    candidate["snapshots"] = json!([snapshot]);
    assert!(check(&prior, &candidate, &[], 1000).is_ok());
    candidate["snapshots"][0]["first-row-id"] = json!(90);
    assert!(check(&prior, &candidate, &[], 1000).is_err());
    candidate["snapshots"] = json!([]);
    assert!(check(&prior, &candidate, &[], 1000).is_ok());
}

#[test]
fn transition_binds_generation_name_fences_and_immutable_file_identity() {
    let value = fixture::metadata(2);
    let bytes = serde_json::to_vec(&value).unwrap();
    let prior_head = fixture::head(
        &bytes,
        2,
        Some(uuid::Uuid::parse_str(value["table-uuid"].as_str().unwrap()).unwrap()),
    );
    let prior = TableMetadataDocument::parse(bytes.clone(), &prior_head, fixture::limits()).unwrap();
    for case in 0..5 {
        let mut head = prior_head.clone();
        head.generation = 2;
        head.metadata_file = FileId::random();
        head.metadata_location = fixture::table().file("metadata/candidate.json").unwrap();
        match case {
            0 => head.generation = 3,
            1 => head.name_epoch += 1,
            2 => head.namespace = crowdb_access_iceberg::key::NamespaceId::random(),
            3 => head.metadata_file = prior_head.metadata_file,
            _ => head.metadata_location = prior_head.metadata_location.clone(),
        }
        let candidate = TableMetadataDocument::parse(bytes.clone(), &head, fixture::limits()).unwrap();
        assert!(matches!(
            validate_metadata_transition(
                &prior,
                &candidate,
                &[],
                TransitionLimits {
                    entries: 1000,
                    upgrade_steps: 10
                }
            ),
            Err(TableMetadataError::Binding)
        ));
    }
}

#[test]
fn legacy_partition_counters_and_retained_summaries_cannot_be_reset() {
    let mut prior = fixture::metadata(1);
    prior.as_object_mut().unwrap().remove("last-partition-id");
    prior["partition-specs"][0]["fields"] =
        json!([{"field-id":1000,"source-id":1,"name":"id","transform":"identity"}]);
    let mut candidate = fixture::metadata(2);
    assert!(check(&prior, &candidate, &[2], 1000).is_err());
    candidate["last-partition-id"] = json!(1000);
    candidate["partition-specs"] = prior["partition-specs"].clone();
    assert!(check(&prior, &candidate, &[2], 1000).is_ok());
    let mut prior = fixture::metadata(2);
    prior["last-sequence-number"] = json!(1);
    prior["snapshots"] = json!([fixture::snapshot(10, 1)]);
    let mut candidate = prior.clone();
    candidate["snapshots"][0]["summary"]["operation"] = json!("delete");
    assert!(check(&prior, &candidate, &[], 1000).is_err());
}

#[test]
fn retained_definition_ids_cannot_change_their_meaning() {
    let mut prior = fixture::metadata(3);
    prior["partition-specs"][0]["fields"] =
        json!([{"field-id":1000,"source-id":1,"name":"id","transform":"identity"}]);
    prior["last-partition-id"] = json!(1000);
    prior["sort-orders"] = json!([{"order-id":1,"fields":[{
        "source-id":1,"transform":"identity","direction":"asc","null-order":"nulls-first"
    }]}]);
    prior["default-sort-order-id"] = json!(1);
    for collection in ["schemas", "partition-specs", "sort-orders"] {
        let mut candidate = prior.clone();
        match collection {
            "schemas" => candidate[collection][0]["fields"][0]["name"] = json!("renamed"),
            "partition-specs" => candidate[collection][0]["fields"][0]["transform"] = json!("bucket[8]"),
            _ => candidate[collection][0]["fields"][0]["direction"] = json!("desc"),
        }
        assert!(matches!(
            check(&prior, &candidate, &[], 1000),
            Err(TableMetadataError::Field(field)) if field == collection
        ));
    }
    let mut candidate = prior.clone();
    candidate["schemas"][0]["fields"][0]["write-default"] = json!(7);
    assert!(check(&prior, &candidate, &[], 1000).is_err());
    candidate["schemas"][0]["schema-id"] = json!(1);
    candidate["current-schema-id"] = json!(1);
    assert!(check(&prior, &candidate, &[], 1000).is_ok());
}

#[test]
fn legacy_definitions_normalize_without_requiring_removed_history() {
    let mut prior = fixture::metadata(1);
    prior.as_object_mut().unwrap().remove("schemas");
    prior.as_object_mut().unwrap().remove("partition-specs");
    prior["partition-spec"] = json!([{"source-id":1,"name":"id","transform":"identity"}]);
    prior["last-partition-id"] = json!(1000);
    let mut candidate = fixture::metadata(2);
    candidate["schemas"][0]["identifier-field-ids"] = json!([]);
    candidate["partition-specs"][0]["fields"] =
        json!([{"field-id":1000,"source-id":1,"name":"id","transform":"identity"}]);
    candidate["last-partition-id"] = json!(1000);
    assert!(check(&prior, &candidate, &[2], 1000).is_ok());
    candidate["partition-specs"][0] = json!({"spec-id":1,"fields":[]});
    candidate["default-spec-id"] = json!(1);
    assert!(check(&prior, &candidate, &[2], 1000).is_ok());
    candidate["schemas"][0]["fields"][0]["name"] = json!("different");
    assert!(check(&prior, &candidate, &[2], 1000).is_err());
}
