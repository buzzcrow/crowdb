#[path = "common/manifest_entry.rs"]
mod fixture;
use crowdb_access_iceberg::file::AvroDatumLimits;
use crowdb_access_iceberg::manifest::{
    ManifestContent, ManifestContext, ManifestEntryProjection, ManifestEntryState, ManifestVersion,
};
use fixture::{table, TestManifestEntry};
use serde_json::json;

fn object(fields: &[(&str, Vec<u8>)], width: usize, reverse: bool) -> Vec<u8> {
    let width_tag = u8::try_from(width - 1).unwrap();
    let mut bytes = vec![1 | 16 | (width_tag << 6)];
    uint(fields.len(), width, &mut bytes);
    let mut offset = 0;
    for (name, _) in fields {
        uint(offset, width, &mut bytes);
        offset += name.len();
    }
    uint(offset, width, &mut bytes);
    for (name, _) in fields {
        bytes.extend(name.as_bytes());
    }
    bytes.push(2 | (width_tag << 2) | (width_tag << 4) | 64);
    uint(fields.len(), 4, &mut bytes);
    for index in 0..fields.len() {
        uint(index, width, &mut bytes);
    }
    let mut positions = vec![0; fields.len()];
    let mut payload = Vec::new();
    for position in 0..fields.len() {
        let index = if reverse {
            fields.len() - position - 1
        } else {
            position
        };
        positions[index] = payload.len();
        payload.extend_from_slice(&fields[index].1);
    }
    for offset in positions {
        uint(offset, width, &mut bytes);
    }
    uint(payload.len(), width, &mut bytes);
    bytes.extend(payload);
    bytes
}

fn uint(value: usize, width: usize, bytes: &mut Vec<u8>) {
    bytes.extend(&u32::try_from(value).unwrap().to_le_bytes()[..width]);
}

fn primitive(kind: u8, value: &[u8]) -> Vec<u8> {
    let mut bytes = vec![kind << 2];
    bytes.extend(value);
    bytes
}

fn valid(lower: Option<Vec<u8>>, upper: Option<Vec<u8>>) -> bool {
    let mut fixture = TestManifestEntry::new(ManifestVersion::V3);
    fixture.set(103, json!(10));
    for (id, value) in [(125, lower), (128, upper)] {
        if let Some(value) = value {
            fixture.file.push((id, "bytes-map", json!([[3, value]])));
        }
    }
    let context=ManifestContext::parse(ManifestVersion::V3,0,0,
        br#"{"type":"struct","schema-id":0,"fields":[{"id":3,"name":"v","required":false,"type":"variant"}]}"#,b"[]").unwrap();
    let schema = fixture.schema();
    let projection =
        ManifestEntryProjection::with_context(&schema, ManifestVersion::V3, table(), &context).unwrap();
    let bytes = fixture.bytes();
    let mut state = ManifestEntryState::new(
        ManifestVersion::V3,
        table(),
        ManifestContent::Data,
        99,
        9,
        Some(100),
    )
    .unwrap();
    let mut records = projection
        .records(
            &bytes,
            1,
            AvroDatumLimits {
                depth: 64,
                values: 10000,
                value_bytes: 1024 * 1024,
            },
            &mut state,
        )
        .unwrap();
    let valid = records.next_entry().is_ok();
    if !valid {
        assert!(records.next_entry().is_err());
    }
    assert_eq!(state.next_row_id(), Some(if valid { 110 } else { 100 }));
    valid
}

fn pair(lower: Vec<u8>, upper: Vec<u8>) -> bool {
    valid(
        Some(object(&[("$", lower)], 1, false)),
        Some(object(&[("$", upper)], 1, false)),
    )
}

#[test]
fn variant_bounds_decode_all_primitive_types_and_logical_encoding_equivalences() {
    let mut values = vec![
        primitive(2, &[]),
        primitive(1, &[]),
        primitive(3, &[255]),
        primitive(4, &(-10_i16).to_le_bytes()),
        primitive(5, &1_i32.to_le_bytes()),
        primitive(6, &i64::MAX.to_le_bytes()),
        primitive(7, &f64::INFINITY.to_le_bytes()),
        primitive(11, &(-1_i32).to_le_bytes()),
        primitive(12, &i64::MIN.to_le_bytes()),
        primitive(13, &i64::MAX.to_le_bytes()),
        primitive(14, &f32::NEG_INFINITY.to_le_bytes()),
        primitive(17, &86_399_999_999_i64.to_le_bytes()),
        primitive(18, &1_i64.to_le_bytes()),
        primitive(19, &2_i64.to_le_bytes()),
        primitive(20, &[255; 16]),
        vec![1],
        vec![9, b'h', b'i'],
    ];
    for (kind, width, value) in [
        (8, 4, 999_999_999_i128),
        (9, 8, -999_999_999_999_999_999),
        (10, 16, 10_i128.pow(38) - 1),
    ] {
        let mut bytes = vec![38];
        bytes.extend(&value.to_le_bytes()[..width]);
        values.push(primitive(kind, &bytes));
    }
    for kind in [15, 16] {
        values.push(primitive(kind, &[2, 0, 0, 0, b'h', b'i']));
    }
    for value in values {
        assert!(pair(value.clone(), value.clone()), "{value:?}");
    }
    assert!(pair(primitive(2, &[]), primitive(1, &[])));
    assert!(!pair(primitive(1, &[]), primitive(2, &[])));
    assert!(pair(primitive(3, &[1]), primitive(6, &2_i64.to_le_bytes())));
    assert!(pair(
        vec![9, b'h', b'i'],
        primitive(16, &[2, 0, 0, 0, b'h', b'i'])
    ));
    assert!(pair(
        primitive(12, &1_i64.to_le_bytes()),
        primitive(18, &1000_i64.to_le_bytes())
    ));
    assert!(!pair(
        primitive(12, &1_i64.to_le_bytes()),
        primitive(19, &1000_i64.to_le_bytes())
    ));
}

#[test]
fn variant_bounds_order_decimals_floats_strings_and_reject_mixed_types() {
    let decimal = |value: i128, scale: u8| {
        let mut bytes = vec![scale];
        bytes.extend(value.to_le_bytes());
        primitive(10, &bytes)
    };
    assert!(pair(decimal(10_i128.pow(38) - 1, 38), primitive(3, &[1])));
    assert!(!pair(primitive(3, &[1]), decimal(10_i128.pow(38) - 1, 38)));
    assert!(pair(decimal(-100, 2), decimal(-99, 2)));
    assert!(!pair(decimal(-99, 2), decimal(-100, 2)));
    assert!(pair(decimal(0, 38), decimal(0, 0)));
    assert!(pair(
        primitive(14, &(-0_f32).to_le_bytes()),
        primitive(14, &0_f32.to_le_bytes())
    ));
    assert!(!pair(
        primitive(14, &0_f32.to_le_bytes()),
        primitive(14, &(-0_f32).to_le_bytes())
    ));
    assert!(!pair(
        primitive(14, &1_f32.to_le_bytes()),
        primitive(7, &1_f64.to_le_bytes())
    ));
    assert!(!pair(primitive(6, &1_i64.to_le_bytes()), vec![5, b'1']));
    for value in [
        primitive(7, &f64::NAN.to_le_bytes()),
        primitive(14, &f32::NAN.to_le_bytes()),
        primitive(17, &86_400_000_000_i64.to_le_bytes()),
        decimal(1, 39),
        decimal(10_i128.pow(38), 0),
        primitive(16, &[1, 0, 0, 0, 255]),
        primitive(0, &[]),
        vec![3, 0, 0],
        vec![2, 0, 0],
        primitive(63, &[]),
    ] {
        assert!(!valid(Some(object(&[("$", value)], 1, false)), None));
    }
}

#[test]
fn variant_objects_accept_all_offset_widths_reordered_values_and_optional_paths() {
    let fields = [
        ("$['a']", primitive(3, &[0])),
        ("$['b']", primitive(6, &42_i64.to_le_bytes())),
    ];
    for width in 1..=4 {
        for reverse in [false, true] {
            let lower = object(&fields, width, reverse);
            let upper = object(&fields, width, !reverse);
            assert!(valid(Some(lower), Some(upper)));
        }
    }
    assert!(valid(
        Some(object(&fields, 1, false)),
        Some(object(
            &[("$['b']", primitive(6, &43_i64.to_le_bytes()))],
            1,
            false
        ))
    ));
    assert!(valid(None, Some(object(&fields, 1, false))));
    assert!(valid(Some(object(&[], 1, false)), Some(object(&[], 1, false))));
    let mut bytes = object(&fields, 1, false);
    bytes[0] |= 32;
    let object_start = 1 + 1 + 3 + 12;
    bytes[object_start] |= 128;
    assert!(valid(Some(bytes), None));
}

#[test]
fn variant_paths_require_normalized_jsonpath_with_bounded_depth() {
    for path in [
        "$",
        "$['']",
        "$['user.name']",
        "$['位置']['纬度']",
        "$['a'][0]",
        "$[42]",
        r"$['a\'b']",
        r"$['\n']",
        r"$['\u000b']",
        r"$['\u001f']",
        r"$['\\']",
    ] {
        assert!(
            valid(Some(object(&[(path, primitive(3, &[1]))], 2, false)), None),
            "{path}"
        );
    }
    let deep = format!("${}", "['a']".repeat(33));
    for path in [
        "",
        "a",
        "$.a",
        "$..a",
        "$[*]",
        "$[-1]",
        "$[01]",
        "$['x']garbage",
        "$[9007199254740992]",
        "$[\"a\"]",
        r"$['\u0061']",
        r"$['\u000a']",
        r"$['\u000B']",
        r"$['\q']",
        "$['\n']",
        deep.as_str(),
    ] {
        assert!(
            !valid(Some(object(&[(path, primitive(3, &[1]))], 2, false)), None),
            "{path}"
        );
    }
}

#[test]
fn malformed_variant_dictionaries_offsets_truncation_and_limits_fail_atomically() {
    let original = object(
        &[("$['a']", primitive(3, &[1])), ("$['b']", primitive(3, &[2]))],
        1,
        false,
    );
    for length in 0..original.len() {
        assert!(!valid(Some(original[..length].to_vec()), None), "prefix {length}");
    }
    for (position, value) in [
        (0, 2),
        (2, 1),
        (3, 0),
        (4, 250),
        (5, 255),
        (17, 0),
        (22, 9),
        (23, 0),
        (24, 1),
        (25, 0),
        (26, 250),
    ] {
        let mut corrupt = original.clone();
        corrupt[position] = value;
        assert!(!valid(Some(corrupt), None), "byte {position}");
    }
    let mut trailing = original.clone();
    trailing.push(0);
    assert!(!valid(Some(trailing), None));
    assert!(!valid(
        Some(object(
            &[("$['a']", primitive(3, &[1])), ("$['a']", primitive(3, &[2]))],
            1,
            false
        )),
        None
    ));
    assert!(!valid(Some(vec![65, 1, 16]), None));
    for position in 0..original.len() {
        for byte in [0, 127, 255] {
            let mut changed = original.clone();
            changed[position] = byte;
            let _valid = valid(Some(changed), None);
        }
    }
}
