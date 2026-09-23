#[path = "common/manifest_list.rs"]
mod fixture;

use crowdb_access_iceberg::file::AvroDatumLimits;
use crowdb_access_iceberg::manifest::{ManifestContext, ManifestListProjection, ManifestVersion};
use fixture::{table, TestManifestList};
use serde_json::json;

fn summary_fixture() -> TestManifestList {
    let mut fixture = TestManifestList::new();
    fixture.summary_schema = Some(json!(["null", {"type":"array","element-id":508,"items":{
    "type":"record","name":"Summary","fields":[
        {"name":"upper","field-id":511,"type":["null","bytes"]},
        {"name":"nulls","field-id":509,"type":"boolean"},
        {"name":"nans","field-id":518,"type":["null","boolean"]},
        {"name":"lower","field-id":510,"type":["null","bytes"]}
    ]}}]));
    fixture.summary_bytes = vec![2, 2, 2, 8];
    fixture.summary_bytes.extend(4_i32.to_le_bytes());
    fixture.summary_bytes.extend([1, 2, 0, 2, 8]);
    fixture.summary_bytes.extend(1_i32.to_le_bytes());
    fixture.summary_bytes.push(0);
    fixture
}

fn context(kind: &str) -> ManifestContext {
    ManifestContext::parse(ManifestVersion::V3, 0, 0,
        &serde_json::to_vec(&json!({"type":"struct","schema-id":0,"fields":[{"id":1,"name":"v","required":false,"type":kind}]})).unwrap(),
        br#"[{"source-id":1,"field-id":1000,"name":"p","transform":"identity"}]"#).unwrap()
}

fn decode(
    fixture: &TestManifestList,
    version: ManifestVersion,
) -> crowdb_access_iceberg::manifest::ManifestListEntry {
    let schema = fixture.schema();
    let projection = ManifestListProjection::new(&schema, version, table()).unwrap();
    projection
        .records(&fixture.bytes(), 1, limits())
        .unwrap()
        .next_entry()
        .unwrap()
        .unwrap()
}

fn limits() -> AvroDatumLimits {
    AvroDatumLimits {
        depth: 64,
        values: 10000,
        value_bytes: 1024 * 1024,
    }
}

#[test]
fn summaries_decode_by_id_in_all_versions_and_preserve_missing_null_and_empty() {
    for version in [ManifestVersion::V1, ManifestVersion::V2, ManifestVersion::V3] {
        let mut fixture = summary_fixture();
        for sized in [false, true] {
            if sized {
                fixture.summary_bytes[1] = 1;
                fixture.summary_bytes.insert(2, 30);
            }
            let entry = decode(&fixture, version);
            entry.validate_partition_summaries(&context("int")).unwrap();
            let summary = &entry.partitions.unwrap()[0];
            assert!(summary.contains_null);
            assert_eq!(summary.contains_nan, Some(false));
            assert_eq!(summary.lower_bound, Some(1_i32.to_le_bytes().to_vec()));
            assert_eq!(summary.upper_bound, Some(4_i32.to_le_bytes().to_vec()));
        }
        fixture.summary_bytes = vec![0];
        assert!(decode(&fixture, version).partitions.is_none());
        fixture.summary_bytes = vec![2, 0];
        assert_eq!(decode(&fixture, version).partitions, Some(vec![]));
        fixture.summary_schema = None;
        fixture.summary_bytes.clear();
        assert!(decode(&fixture, version).partitions.is_none());
    }
}

#[test]
fn summaries_reject_schema_framing_and_resource_errors_and_poison_cursor() {
    for fault in 0..4 {
        let mut fixture = summary_fixture();
        match fault {
            0 => fixture.summary_schema.as_mut().unwrap()[1]["element-id"] = json!(999),
            1 => fixture.summary_schema.as_mut().unwrap()[1]["items"]["fields"][1]["field-id"] = json!(511),
            2 => fixture.summary_schema.as_mut().unwrap()[1]["items"]["fields"][1]["type"] = json!("long"),
            _ => {
                fixture.summary_schema.as_mut().unwrap()[1]["items"]["fields"]
                    .as_array_mut()
                    .unwrap()
                    .remove(1);
            }
        }
        assert!(ManifestListProjection::new(&fixture.schema(), ManifestVersion::V3, table()).is_err());
    }
    for fault in 0..4 {
        let mut fixture = summary_fixture();
        match fault {
            0 => {
                fixture.summary_bytes[1] = 1;
                fixture.summary_bytes.insert(2, 24);
            }
            1 => fixture.summary_bytes[8] = 2,
            2 => {
                fixture.summary_bytes = vec![2, 130, 4];
                fixture.summary_bytes.extend([0, 0, 0, 0].repeat(257));
                fixture.summary_bytes.push(0);
            }
            _ => {
                fixture.summary_bytes.pop();
            }
        }
        let schema = fixture.schema();
        let projection = ManifestListProjection::new(&schema, ManifestVersion::V3, table()).unwrap();
        let bytes = fixture.bytes();
        let mut records = projection.records(&bytes, 1, limits()).unwrap();
        assert!(records.next_entry().is_err(), "fault {fault}");
        assert!(records.next_entry().is_err());
    }
}

#[test]
fn summaries_bind_spec_order_types_bounds_and_signed_zero() {
    let mut fixture = summary_fixture();
    fixture.set(502, json!(1));
    assert!(decode(&fixture, ManifestVersion::V3)
        .validate_partition_summaries(&context("int"))
        .is_err());
    fixture.set(502, json!(0));
    for fault in 0..6 {
        let mut entry = decode(&fixture, ManifestVersion::V3);
        let summary = &mut entry.partitions.as_mut().unwrap()[0];
        match fault {
            0 => summary.lower_bound = Some(5_i32.to_le_bytes().to_vec()),
            1 => summary.upper_bound = Some(vec![1]),
            2 => summary.contains_nan = Some(true),
            3 => entry.partition_spec_id = 1,
            4 => entry.partitions.as_mut().unwrap().clear(),
            _ => summary.lower_bound = Some(vec![0; 1024 * 1024 + 1]),
        }
        assert!(entry.validate_partition_summaries(&context("int")).is_err());
    }
    let mut entry = decode(&fixture, ManifestVersion::V3);
    let summary = &mut entry.partitions.as_mut().unwrap()[0];
    summary.lower_bound = Some(0_f32.to_le_bytes().to_vec());
    summary.upper_bound = Some((-0_f32).to_le_bytes().to_vec());
    assert!(entry.validate_partition_summaries(&context("float")).is_err());
    let summary = &mut entry.partitions.as_mut().unwrap()[0];
    std::mem::swap(&mut summary.lower_bound, &mut summary.upper_bound);
    entry.validate_partition_summaries(&context("float")).unwrap();
    entry.partitions.as_mut().unwrap()[0].upper_bound = Some(f32::NAN.to_le_bytes().to_vec());
    assert!(entry.validate_partition_summaries(&context("float")).is_err());
}
