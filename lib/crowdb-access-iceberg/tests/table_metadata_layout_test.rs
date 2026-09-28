#[path = "common/table_metadata.rs"]
#[allow(dead_code)]
mod fixture;

use serde_json::json;

#[test]
fn current_layout_binds_sources_but_history_can_retain_dropped_sources() {
    let mut table = fixture::metadata(3);
    table["last-partition-id"] = json!(1000);
    table["partition-specs"] = json!([
        {"spec-id":0,"fields":[]},
        {"spec-id":1,"fields":[{"field-id":1000,"source-id":50,"name":"old","transform":"identity"}]}
    ]);
    table["sort-orders"] = json!([
        {"order-id":0,"fields":[]},
        {"order-id":1,"fields":[{"source-id":50,"transform":"identity","direction":"asc","null-order":"nulls-first"}]}
    ]);
    assert!(fixture::parse(&table).is_ok());
    table["default-spec-id"] = json!(1);
    assert!(fixture::parse(&table).is_err());
    table["partition-specs"][1]["fields"][0]["source-id"] = json!(1);
    assert!(fixture::parse(&table).is_ok());
    table["default-sort-order-id"] = json!(1);
    assert!(fixture::parse(&table).is_err());
    table["sort-orders"][1]["fields"][0]["source-id"] = json!(1);
    assert!(fixture::parse(&table).is_ok());
}

#[test]
fn layout_rejects_invalid_transforms_ordering_and_partition_identity() {
    for case in 0..6 {
        let mut table = fixture::metadata(3);
        table["last-partition-id"] = json!(1000);
        table["partition-specs"][0]["fields"] =
            json!([{"field-id":1000,"source-id":1,"name":"part","transform":"bucket[8]"}]);
        match case {
            0 => table["partition-specs"][0]["fields"][0]["transform"] = json!("bucket[0]"),
            1 => table["partition-specs"][0]["fields"][0]["transform"] = json!("hour"),
            2 => table["last-partition-id"] = json!(999),
            3 => table["partition-specs"][0]["fields"][0]["source-id"] = json!(0),
            4 => table["sort-orders"][0]["fields"] = json!([{}]),
            _ => {
                let field = table["partition-specs"][0]["fields"][0].clone();
                table["partition-specs"][0]["fields"]
                    .as_array_mut()
                    .unwrap()
                    .push(field);
            }
        }
        assert!(fixture::parse(&table).is_err(), "case {case}");
    }
}

#[test]
fn name_mapping_preserves_historical_ids_and_rejects_ambiguity() {
    let mut table = fixture::metadata(3);
    for (mapping, accepted) in [
        (
            r#"[{"field-id":20,"names":["old","renamed"]},{"names":[]}]"#,
            true,
        ),
        (
            r#"[{"field-id":20,"names":["a"]},{"field-id":20,"names":["b"]}]"#,
            false,
        ),
        (
            r#"[{"field-id":1,"names":["a"]},{"field-id":2,"names":["a"]}]"#,
            false,
        ),
        (r#"[{"field-id":1},{"field-id":2,"names":["a","a"]}]"#, true),
        (r#"[{"names":[],"names":[]}]"#, false),
    ] {
        table["properties"]["schema.name-mapping.default"] = json!(mapping);
        assert_eq!(fixture::parse(&table).is_ok(), accepted);
    }
}
