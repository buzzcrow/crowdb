#[path = "common/table_metadata.rs"]
#[allow(dead_code)]
mod fixture;

use serde_json::{json, Value};

fn accepted(kind: Value, default: Value) -> bool {
    let mut table = fixture::metadata(3);
    table["last-column-id"] = json!(10);
    table["schemas"][0]["fields"][0]["required"] = json!(false);
    table["schemas"][0]["fields"][0]["type"] = kind;
    table["schemas"][0]["fields"][0]["initial-default"] = default.clone();
    table["schemas"][0]["fields"][0]["write-default"] = default;
    fixture::parse(&table).is_ok()
}

#[test]
fn primitive_defaults_check_type_range_and_canonical_encodings() {
    for (kind, good, bad) in [
        ("boolean", json!(true), json!("true")),
        ("int", json!(2_147_483_647), json!(2_147_483_648_u64)),
        ("long", json!(i64::MAX), json!(u64::MAX)),
        ("float", json!(1.5), json!(1e100)),
        ("double", json!(1e100), json!("NaN")),
        ("decimal(4,2)", json!("12.34"), json!("123.45")),
        ("decimal(4,2)", json!("1234e-2"), json!("12.3")),
        ("string", json!("雪"), json!(5)),
        (
            "uuid",
            json!("12345678-1234-1234-1234-123456789abc"),
            json!("1234"),
        ),
        ("fixed[2]", json!("aB00"), json!("ab")),
        ("binary", json!(""), json!("GG")),
        ("date", json!("2024-02-29"), json!("2023-02-29")),
        ("time", json!("12:34:56.123456"), json!("24:00:00")),
        (
            "timestamp",
            json!("2024-01-01T01:02:03.123456"),
            json!("2024-01-01T01:02:03+00:00"),
        ),
        (
            "timestamptz",
            json!("2024-01-01T01:02:03+00:00"),
            json!("2024-01-01T01:02:03+01:00"),
        ),
        (
            "timestamp_ns",
            json!("2024-01-01T01:02:03.123456789"),
            json!("9999-01-01T00:00:00"),
        ),
        ("variant", Value::Null, json!({})),
        ("unknown", Value::Null, json!(1)),
    ] {
        assert!(accepted(json!(kind), good), "valid {kind}");
        assert!(!accepted(json!(kind), bad), "invalid {kind}");
    }
}

#[test]
fn nested_defaults_validate_elements_and_struct_field_defaults() {
    let list = json!({"type":"list","element-id":2,"element":"long","element-required":true});
    assert!(accepted(list.clone(), json!([1, 2])));
    assert!(!accepted(list.clone(), json!([1, null])));
    assert!(!accepted(list, json!(["1"])));
    let map =
        json!({"type":"map","key-id":2,"key":"string","value-id":3,"value":"long","value-required":false});
    assert!(accepted(map.clone(), json!({"keys":["a"],"values":[null]})));
    assert!(!accepted(map.clone(), json!({"keys":[null],"values":[1]})));
    assert!(!accepted(map, json!({"keys":["a"],"values":[]})));
    let structure = json!({"type":"struct","fields":[{"id":2,"name":"inner","type":"long","required":false,"initial-default":1}]});
    assert!(accepted(structure.clone(), json!({})));
    assert!(!accepted(structure, json!({"2":1})));
}

#[test]
fn map_keys_are_compared_as_typed_values_not_json_spellings() {
    for (kind, keys) in [
        ("binary", json!(["ab", "AB"])),
        ("decimal(4,2)", json!(["12.34", "1234e-2"])),
        (
            "timestamp",
            json!(["2024-01-01T01:02:03", "2024-01-01T01:02:03.000000"]),
        ),
        ("double", json!([1, 1.0])),
    ] {
        let map =
            json!({"type":"map","key-id":2,"key":kind,"value-id":3,"value":"long","value-required":true});
        assert!(!accepted(map, json!({"keys":keys,"values":[1,2]})), "{kind}");
    }
    assert!(!accepted(json!("time"), json!("23:59:60")));
}
