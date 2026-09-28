#[path = "common/manifest_list.rs"]
mod fixture;

use crowdb_access_iceberg::file::{AvroContainerError, AvroDatumLimits};
use crowdb_access_iceberg::manifest::{
    ManifestContent, ManifestListError, ManifestListProjection, ManifestVersion,
};
use fixture::{table, TestManifestList};
use serde_json::json;

fn limits() -> AvroDatumLimits {
    AvroDatumLimits {
        depth: 64,
        values: 1000,
        value_bytes: 1024,
    }
}

#[test]
fn list_versions_resolve_defaults_and_preserve_counts_and_row_ids_by_field_identity() {
    for version in [ManifestVersion::V1, ManifestVersion::V2, ManifestVersion::V3] {
        let mut fixture = TestManifestList::new();
        fixture.fields.reverse();
        let schema = fixture.schema();
        let projection = ManifestListProjection::new(&schema, version, table()).unwrap();
        let bytes = fixture.bytes();
        let mut records = projection.records(&bytes, 1, limits()).unwrap();
        let entry = records.next_entry().unwrap().unwrap();
        assert_eq!(entry.location, table().file("metadata/manifest.avro").unwrap());
        assert_eq!(
            (entry.length, entry.partition_spec_id, entry.added_snapshot_id),
            (42, 0, 99)
        );
        assert_eq!(entry.content, ManifestContent::Data);
        assert_eq!(
            (entry.sequence, entry.min_sequence),
            if version == ManifestVersion::V1 {
                (0, 0)
            } else {
                (8, 6)
            }
        );
        assert_eq!(entry.file_counts, [Some(1), Some(2), Some(3)]);
        assert_eq!(entry.row_counts, [Some(10), Some(20), Some(30)]);
        assert_eq!(
            entry.first_row_id,
            if version == ManifestVersion::V3 {
                Some(100)
            } else {
                None
            }
        );
        assert!(records.next_entry().unwrap().is_none());
    }
}

#[test]
fn omitted_v1_counts_remain_unknown_and_newer_versions_require_them() {
    let mut fixture = TestManifestList::new();
    fixture.fields.truncate(4);
    let schema = fixture.schema();
    let projection = ManifestListProjection::new(&schema, ManifestVersion::V1, table()).unwrap();
    let bytes = fixture.bytes();
    let entry = projection
        .records(&bytes, 1, limits())
        .unwrap()
        .next_entry()
        .unwrap()
        .unwrap();
    assert_eq!(entry.file_counts, [None; 3]);
    assert_eq!(entry.row_counts, [None; 3]);
    assert_eq!(entry.first_row_id, None);
    assert!(ManifestListProjection::new(&schema, ManifestVersion::V2, table()).is_err());
    fixture = TestManifestList::new();
    fixture.fields.pop();
    let schema = fixture.schema();
    let projection = ManifestListProjection::new(&schema, ManifestVersion::V3, table()).unwrap();
    let bytes = fixture.bytes();
    assert_eq!(
        projection
            .records(&bytes, 1, limits())
            .unwrap()
            .next_entry()
            .unwrap()
            .unwrap()
            .first_row_id,
        None
    );
}

#[test]
fn invalid_numeric_fields_null_requirements_and_cross_table_paths_poison_the_list_cursor() {
    for (id, value) in [
        (501, json!(0)),
        (501, json!(-1)),
        (502, json!(-1)),
        (517, json!(2)),
        (515, json!(-1)),
        (516, json!(9)),
        (504, json!(-1)),
        (505, json!(null)),
        (512, json!(-1)),
        (513, json!(null)),
        (520, json!(-1)),
        (503, json!(null)),
        (500, json!("s3://other/file")),
    ] {
        let mut fixture = TestManifestList::new();
        fixture.set(id, value);
        let schema = fixture.schema();
        let projection = ManifestListProjection::new(&schema, ManifestVersion::V3, table()).unwrap();
        let bytes = fixture.bytes();
        let mut records = projection.records(&bytes, 1, limits()).unwrap();
        assert!(
            matches!(records.next_entry(), Err(ManifestListError::Field)),
            "field {id}"
        );
        assert!(matches!(
            records.next_entry(),
            Err(ManifestListError::Avro(AvroContainerError::Failed))
        ));
    }
    let fixture = TestManifestList::new();
    let schema = fixture.schema();
    let mut foreign = table();
    foreign.table = crowdb_access_iceberg::key::TableId::from_bytes(&[3; 16]).unwrap();
    let projection = ManifestListProjection::new(&schema, ManifestVersion::V3, foreign).unwrap();
    assert!(projection
        .records(&fixture.bytes(), 1, limits())
        .unwrap()
        .next_entry()
        .is_err());
}

#[test]
fn delete_lists_forbid_row_ids_and_empty_lists_still_check_writer_types() {
    let mut fixture = TestManifestList::new();
    fixture.set(517, json!(1));
    let schema = fixture.schema();
    let projection = ManifestListProjection::new(&schema, ManifestVersion::V3, table()).unwrap();
    assert!(projection
        .records(&fixture.bytes(), 1, limits())
        .unwrap()
        .next_entry()
        .is_err());
    fixture.set(520, json!(null));
    let bytes = fixture.bytes();
    let entry = projection
        .records(&bytes, 1, limits())
        .unwrap()
        .next_entry()
        .unwrap()
        .unwrap();
    assert_eq!(entry.content, ManifestContent::Deletes);
    assert_eq!(entry.first_row_id, None);
    assert!(projection
        .records(&[], 0, limits())
        .unwrap()
        .next_entry()
        .unwrap()
        .is_none());
    for id in [500, 501, 502, 503, 517, 515, 516, 504, 512, 520] {
        let mut fixture = TestManifestList::new();
        let field = fixture.fields.iter_mut().find(|field| field.0 == id).unwrap();
        field.1 = if field.1 == "long" { "int" } else { "long" };
        assert!(ManifestListProjection::new(&fixture.schema(), ManifestVersion::V3, table()).is_err());
    }
}
