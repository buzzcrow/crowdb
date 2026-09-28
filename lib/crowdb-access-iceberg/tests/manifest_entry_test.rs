#[path = "common/manifest_entry.rs"]
mod fixture;

use crowdb_access_iceberg::file::{AvroContainerError, AvroDatumLimits, ContentFormat, FormatHint};
use crowdb_access_iceberg::manifest::{
    ManifestContent, ManifestEntryError, ManifestEntryProjection, ManifestEntryState,
    ManifestInheritanceError, ManifestVersion,
};
use fixture::{table, TestManifestEntry};
use serde_json::json;

fn limits() -> AvroDatumLimits {
    AvroDatumLimits {
        depth: 64,
        values: 1000,
        value_bytes: 1024,
    }
}

fn state(version: ManifestVersion) -> ManifestEntryState {
    ManifestEntryState::new(version, table(), ManifestContent::Data, 50, 9, Some(100)).unwrap()
}

#[test]
fn typed_entries_resolve_writer_versions_and_preserve_inheritance_across_blocks() {
    for version in [ManifestVersion::V1, ManifestVersion::V2, ManifestVersion::V3] {
        let mut fixture = TestManifestEntry::new(version);
        fixture.root.reverse();
        fixture.file.reverse();
        let schema = fixture.schema();
        let projection = ManifestEntryProjection::new(&schema, version, table()).unwrap();
        let mut state = state(version);
        for first in [100, 110] {
            let bytes = fixture.bytes();
            let mut records = projection.records(&bytes, 1, limits(), &mut state).unwrap();
            let entry = records.next_entry().unwrap().unwrap();
            assert_eq!(entry.inherited.first_row_id, Some(first));
            assert_eq!(
                entry.inherited.data_sequence,
                if version == ManifestVersion::V1 { 0 } else { 9 }
            );
            assert_eq!(entry.inherited.snapshot_id, 99);
            assert_eq!(entry.file.format, ContentFormat::Parquet);
            assert_eq!(entry.file.length, 42);
            assert_eq!(entry.file.location, table().file("data/file.parquet").unwrap());
            assert!(records.next_entry().unwrap().is_none());
        }
        assert_eq!(state.next_row_id(), Some(120));
    }
}

#[test]
fn missing_existing_sequences_and_row_overflow_do_not_advance_inheritance() {
    let mut fixture = TestManifestEntry::new(ManifestVersion::V3);
    fixture.set(0, json!(0));
    let schema = fixture.schema();
    let projection = ManifestEntryProjection::new(&schema, ManifestVersion::V3, table()).unwrap();
    let mut state = state(ManifestVersion::V3);
    assert!(matches!(
        projection
            .records(&fixture.bytes(), 1, limits(), &mut state)
            .unwrap()
            .next_entry(),
        Err(ManifestEntryError::Inheritance(ManifestInheritanceError::Missing))
    ));
    assert_eq!(state.next_row_id(), Some(100));
    fixture.set(3, json!(6));
    fixture.set(4, json!(7));
    fixture.set(142, json!(200));
    let entry = projection
        .records(&fixture.bytes(), 1, limits(), &mut state)
        .unwrap()
        .next_entry()
        .unwrap()
        .unwrap();
    assert_eq!(
        (
            entry.inherited.data_sequence,
            entry.inherited.file_sequence,
            entry.inherited.first_row_id
        ),
        (6, 7, Some(200))
    );
    assert_eq!(state.next_row_id(), Some(100));
    fixture.set(142, json!(null));
    fixture.set(103, json!(i64::MAX));
    assert!(matches!(
        projection
            .records(&fixture.bytes(), 1, limits(), &mut state)
            .unwrap()
            .next_entry(),
        Err(ManifestEntryError::Inheritance(
            ManifestInheritanceError::Overflow
        ))
    ));
    assert_eq!(state.next_row_id(), Some(100));
}

#[test]
fn scalar_errors_poison_the_cursor_before_advancing_any_row_ids() {
    for (id, value) in [
        (0, json!(3)),
        (134, json!(3)),
        (103, json!(-1)),
        (104, json!(0)),
        (100, json!("s3://other/path")),
        (101, json!("json")),
        (140, json!(-1)),
        (142, json!(-1)),
        (144, json!(4)),
        (145, json!(20)),
        (100, json!(null)),
        (143, json!(table().file("data/other").unwrap().to_string())),
        (3, json!(-1)),
    ] {
        let mut fixture = TestManifestEntry::new(ManifestVersion::V3);
        fixture.set(id, value);
        let schema = fixture.schema();
        let projection = ManifestEntryProjection::new(&schema, ManifestVersion::V3, table()).unwrap();
        let mut state = state(ManifestVersion::V3);
        let bytes = fixture.bytes();
        {
            let mut records = projection.records(&bytes, 1, limits(), &mut state).unwrap();
            assert!(records.next_entry().is_err(), "field {id}");
            assert!(matches!(
                records.next_entry(),
                Err(ManifestEntryError::Avro(AvroContainerError::Failed))
            ));
        }
        assert_eq!(state.next_row_id(), Some(100));
    }
}

#[test]
fn deletion_vectors_bind_their_descriptor_and_position_deletes_ignore_sort_order() {
    let mut fixture = TestManifestEntry::new(ManifestVersion::V3);
    fixture.set(134, json!(1));
    fixture.set(101, json!("puffin"));
    fixture.set(140, json!(-99));
    fixture.set(
        143,
        json!(table().file("data/referenced.parquet").unwrap().to_string()),
    );
    fixture.set(144, json!(4));
    fixture.set(145, json!(20));
    let schema = fixture.schema();
    let projection = ManifestEntryProjection::new(&schema, ManifestVersion::V3, table()).unwrap();
    let mut state = ManifestEntryState::new(
        ManifestVersion::V3,
        table(),
        ManifestContent::Deletes,
        50,
        9,
        None,
    )
    .unwrap();
    let entry = projection
        .records(&fixture.bytes(), 1, limits(), &mut state)
        .unwrap()
        .next_entry()
        .unwrap()
        .unwrap();
    assert_eq!(
        entry.file.deletion_vector,
        Some(FormatHint {
            offset: 4,
            length: 20
        })
    );
    assert_eq!(entry.file.sort_order_id, None);
    assert_eq!(entry.inherited.first_row_id, None);
    for (id, value) in [
        (143, json!(null)),
        (144, json!(null)),
        (144, json!(-1)),
        (145, json!(19)),
        (145, json!(i64::MAX)),
        (142, json!(1)),
        (134, json!(2)),
    ] {
        let previous = fixture.file.iter().find(|field| field.0 == id).unwrap().2.clone();
        fixture.set(id, value);
        assert!(projection
            .records(&fixture.bytes(), 1, limits(), &mut state)
            .unwrap()
            .next_entry()
            .is_err());
        fixture.set(id, previous);
    }
}

#[test]
fn schema_context_null_record_and_trailing_bytes_fail_without_advancing_state() {
    let mut fixture = TestManifestEntry::new(ManifestVersion::V3);
    let schema = fixture.schema();
    let projection = ManifestEntryProjection::new(&schema, ManifestVersion::V3, table()).unwrap();
    assert!(projection
        .records(&[], 0, limits(), &mut state(ManifestVersion::V2))
        .is_err());
    let mut state = state(ManifestVersion::V3);
    fixture.null_file = true;
    assert!(projection
        .records(&fixture.bytes(), 1, limits(), &mut state)
        .unwrap()
        .next_entry()
        .is_err());
    fixture.null_file = false;
    let mut bytes = fixture.bytes();
    bytes.push(0);
    assert!(projection
        .records(&bytes, 1, limits(), &mut state)
        .unwrap()
        .next_entry()
        .is_err());
    assert_eq!(state.next_row_id(), Some(100));
    fixture.file.iter_mut().find(|field| field.0 == 103).unwrap().1 = "int";
    assert!(ManifestEntryProjection::new(&fixture.schema(), ManifestVersion::V3, table()).is_err());
    fixture.file.retain(|field| field.0 != 100);
    assert!(ManifestEntryProjection::new(&fixture.schema(), ManifestVersion::V3, table()).is_err());
    let mut v1 = TestManifestEntry::new(ManifestVersion::V1);
    v1.root.retain(|field| field.0 != 1);
    assert!(ManifestEntryProjection::new(&v1.schema(), ManifestVersion::V1, table()).is_err());
}

#[test]
fn equality_ids_require_a_bounded_unique_list_and_matching_element_id() {
    let mut fixture = TestManifestEntry::new(ManifestVersion::V2);
    fixture.set(134, json!(2));
    fixture.set(135, json!([3, 8]));
    let schema = fixture.schema();
    let projection = ManifestEntryProjection::new(&schema, ManifestVersion::V2, table()).unwrap();
    let mut state = ManifestEntryState::new(
        ManifestVersion::V2,
        table(),
        ManifestContent::Deletes,
        50,
        9,
        None,
    )
    .unwrap();
    let entry = projection
        .records(&fixture.bytes(), 1, limits(), &mut state)
        .unwrap()
        .next_entry()
        .unwrap()
        .unwrap();
    assert_eq!(entry.file.equality_ids, Some(vec![3, 8]));

    for ids in [json!(null), json!([]), json!([3, 3]), json!([0]), json!([-1])] {
        fixture.set(135, ids);
        assert!(projection
            .records(&fixture.bytes(), 1, limits(), &mut state)
            .unwrap()
            .next_entry()
            .is_err());
    }
    fixture.set(135, json!([3, 8]));
    fixture.set(134, json!(0));
    assert!(projection
        .records(&fixture.bytes(), 1, limits(), &mut state)
        .unwrap()
        .next_entry()
        .is_err());

    let schema = String::from_utf8(fixture.schema_bytes()).unwrap();
    for replacement in ["\"element-id\":137", "\"element-id\":-1", "\"other-id\":136"] {
        let schema = schema.replace("\"element-id\":136", replacement);
        let schema = crowdb_access_iceberg::file::AvroSchema::parse(schema.as_bytes()).unwrap();
        assert!(ManifestEntryProjection::new(&schema, ManifestVersion::V2, table()).is_err());
    }
}

#[test]
fn equality_ids_accept_sized_avro_blocks_and_reject_excess_work() {
    let mut fixture = TestManifestEntry::new(ManifestVersion::V2);
    fixture.set(134, json!(2));
    fixture.set(135, json!([3, 8]));
    let schema = fixture.schema();
    let projection = ManifestEntryProjection::new(&schema, ManifestVersion::V2, table()).unwrap();
    let mut state = ManifestEntryState::new(
        ManifestVersion::V2,
        table(),
        ManifestContent::Deletes,
        50,
        9,
        None,
    )
    .unwrap();
    let mut bytes = fixture.bytes();
    bytes.truncate(bytes.len() - 5);
    bytes.extend_from_slice(&[2, 3, 4, 6, 16, 0]);
    let entry = projection
        .records(&bytes, 1, limits(), &mut state)
        .unwrap()
        .next_entry()
        .unwrap()
        .unwrap();
    assert_eq!(entry.file.equality_ids, Some(vec![3, 8]));
    assert_eq!(state.next_row_id(), None);

    let block_length = bytes.len() - 4;
    bytes[block_length] = 6;
    assert!(projection
        .records(&bytes, 1, limits(), &mut state)
        .unwrap()
        .next_entry()
        .is_err());

    fixture.set(135, json!(vec![1; 4097]));
    let generous = AvroDatumLimits {
        depth: 64,
        values: 20_000,
        value_bytes: 8 * 1024 * 1024,
    };
    assert!(matches!(
        projection
            .records(&fixture.bytes(), 1, generous, &mut state)
            .unwrap()
            .next_entry(),
        Err(ManifestEntryError::Avro(AvroContainerError::Bounds))
    ));
}
