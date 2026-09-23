#[path = "common/table_metadata.rs"]
#[allow(dead_code)]
mod fixture;

use serde_json::{json, Value};

fn populated() -> Value {
    let mut value = fixture::metadata(3);
    let mut first = fixture::snapshot(10, 1);
    first["first-row-id"] = json!(0);
    first["added-rows"] = json!(10);
    let mut second = fixture::snapshot(20, 2);
    second["first-row-id"] = json!(10);
    second["added-rows"] = json!(5);
    second["parent-snapshot-id"] = json!(10);
    value["snapshots"] = json!([second, first]);
    value["last-sequence-number"] = json!(2);
    value["next-row-id"] = json!(15);
    value["current-snapshot-id"] = json!(20);
    value["refs"] = json!({"main":{"snapshot-id":20,"type":"branch"},"old":{"snapshot-id":10,"type":"tag"}});
    value
}

#[test]
fn snapshots_validate_unsorted_history_and_retained_parent_links() {
    let document = fixture::parse(&populated()).unwrap();
    assert_eq!(document.current_snapshot(), Some(20));
    let selected = document.snapshots()[&20]
        .manifest_selection(crowdb_access_iceberg::manifest::ManifestVersion::V3)
        .unwrap();
    assert_eq!(selected.parent_snapshot_id, Some(10));
    assert_eq!(selected.first_row_id, Some(10));
    assert_eq!(selected.added_rows, Some(5));
    let mut value = populated();
    value["snapshots"][1]["parent-snapshot-id"] = json!(1);
    assert!(fixture::parse(&value).is_ok());
}

#[test]
fn duplicate_sequences_cycles_and_invalid_row_ranges_are_rejected() {
    for case in 0..9 {
        let mut value = populated();
        match case {
            0 => value["snapshots"][1]["snapshot-id"] = json!(20),
            1 => value["snapshots"][1]["sequence-number"] = json!(2),
            2 => value["snapshots"][1]["parent-snapshot-id"] = json!(20),
            3 => value["snapshots"][0]["first-row-id"] = json!(9),
            4 => value["snapshots"][0]["added-rows"] = json!(6),
            5 => value["snapshots"][0]["first-row-id"] = json!(i64::MAX),
            6 => value["snapshots"][0]
                .as_object_mut()
                .unwrap()
                .remove("added-rows")
                .map(|_| ())
                .unwrap(),
            7 => value["snapshots"][0]["sequence-number"] = json!(3),
            _ => value["snapshots"][0]["schema-id"] = json!(1),
        }
        assert!(fixture::parse(&value).is_err(), "case {case}");
    }
    let mut value = populated();
    for snapshot in value["snapshots"].as_array_mut().unwrap() {
        snapshot["sequence-number"] = json!(0);
        snapshot.as_object_mut().unwrap().remove("first-row-id");
        snapshot.as_object_mut().unwrap().remove("added-rows");
    }
    value["snapshots"][1]["parent-snapshot-id"] = json!(20);
    assert!(fixture::parse(&value).is_err());
}

#[test]
fn references_validate_main_retention_and_missing_targets() {
    for (field, invalid) in [
        ("snapshot-id", json!(30)),
        ("type", json!("tag")),
        ("min-snapshots-to-keep", json!(0)),
        ("max-snapshot-age-ms", json!(-1)),
        ("min-snapshots-to-keep", json!(2_147_483_648_u64)),
    ] {
        let mut value = populated();
        value["refs"]["main"][field] = invalid;
        assert!(fixture::parse(&value).is_err(), "{field}");
    }
    let mut value = populated();
    value["refs"]["old"]["min-snapshots-to-keep"] = json!(1);
    assert!(fixture::parse(&value).is_err());
    let mut value = populated();
    value["refs"] = Value::Null;
    assert!(fixture::parse(&value).is_ok());
    let mut value = populated();
    value["refs"].as_object_mut().unwrap().remove("main");
    assert!(fixture::parse(&value).is_err());
}

#[test]
fn logs_follow_sdk_clock_skew_tolerance_without_overflow() {
    for delta in [60_000_i64, 60_001, i64::MAX] {
        let mut value = populated();
        value["snapshot-log"] =
            json!([{"snapshot-id":10,"timestamp-ms":delta},{"snapshot-id":20,"timestamp-ms":0}]);
        assert_eq!(fixture::parse(&value).is_ok(), delta == 60_000);
    }
    let mut value = populated();
    value["metadata-log"] = json!([{"metadata-file":fixture::table().file("metadata/older.json").unwrap().to_string(),"timestamp-ms":61001}]);
    assert!(fixture::parse(&value).is_err());
    let mut value = populated();
    value["snapshot-log"] = json!([{"snapshot-id":30,"timestamp-ms":0}]);
    assert!(fixture::parse(&value).is_err());
}

#[test]
fn statistics_and_encryption_metadata_are_structural_not_file_proofs() {
    let mut value = populated();
    value["statistics"] = json!([{"snapshot-id":20,"statistics-path":fixture::table().file("stats/stats.puffin").unwrap().to_string(),
        "file-size-in-bytes":100,"file-footer-size-in-bytes":30,"key-metadata":"AQI=",
        "blob-metadata":[{"type":"theta-sketch","snapshot-id":20,"sequence-number":2,"fields":[1],"properties":{"a":"b"}}]}]);
    value["encryption-keys"] =
        json!([{"key-id":"key-a","encrypted-key-metadata":"AQI=","encrypted-by-id":"kms-external"}]);
    value["snapshots"][0]["key-id"] = json!("key-a");
    assert!(fixture::parse(&value).is_ok());
    value["encryption-keys"][0]["encrypted-by-id"] = json!(null);
    assert!(fixture::parse(&value).is_ok());
    for case in 0..4 {
        let mut invalid = value.clone();
        match case {
            0 => invalid["statistics"][0]["file-footer-size-in-bytes"] = json!(101),
            1 => invalid["statistics"][0]["blob-metadata"][0]["fields"] = json!(["bad"]),
            2 => invalid["encryption-keys"][0]["encrypted-key-metadata"] = json!("!"),
            _ => {
                let key = invalid["encryption-keys"][0].clone();
                invalid["encryption-keys"].as_array_mut().unwrap().push(key);
            }
        }
        assert!(fixture::parse(&invalid).is_err());
    }
}
