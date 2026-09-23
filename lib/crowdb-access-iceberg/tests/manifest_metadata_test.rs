use std::collections::BTreeMap;

use crowdb_access_iceberg::file::TableLocation;
use crowdb_access_iceberg::key::{CatalogId, TableId};
use crowdb_access_iceberg::manifest::{
    ManifestContent, ManifestEntryState, ManifestListEntry, ManifestMetadata, ManifestVersion,
};

fn table() -> TableLocation {
    TableLocation {
        catalog: CatalogId::from_bytes(&[1; 16]).unwrap(),
        table: TableId::from_bytes(&[2; 16]).unwrap(),
    }
}

fn metadata(version: ManifestVersion) -> BTreeMap<String, Vec<u8>> {
    let mut values = BTreeMap::from([
        (
            "schema".to_owned(),
            br#"{"type":"struct","schema-id":7,"fields":[]}"#.to_vec(),
        ),
        ("partition-spec".to_owned(), b"[]".to_vec()),
    ]);
    if version != ManifestVersion::V1 {
        values.insert("schema-id".to_owned(), b"7".to_vec());
        values.insert("partition-spec-id".to_owned(), b"4".to_vec());
        values.insert(
            "format-version".to_owned(),
            if version == ManifestVersion::V2 {
                b"2"
            } else {
                b"3"
            }
            .to_vec(),
        );
        values.insert("content".to_owned(), b"deletes".to_vec());
    }
    values
}

#[test]
fn manifest_writer_metadata_selects_its_own_version_and_content() {
    for version in [ManifestVersion::V1, ManifestVersion::V2, ManifestVersion::V3] {
        let values = metadata(version);
        let parsed = ManifestMetadata::parse(&values).unwrap();
        assert_eq!(parsed.version, version);
        assert_eq!(
            parsed.content,
            if version == ManifestVersion::V1 {
                ManifestContent::Data
            } else {
                ManifestContent::Deletes
            }
        );
        assert_eq!(parsed.schema_json, values["schema"]);
        assert_eq!(parsed.partition_spec_json, values["partition-spec"]);
        assert_eq!(
            parsed.schema_id,
            if version == ManifestVersion::V1 {
                None
            } else {
                Some(7)
            }
        );
        assert_eq!(
            parsed.partition_spec_id,
            if version == ManifestVersion::V1 {
                None
            } else {
                Some(4)
            }
        );
    }
}

#[test]
fn missing_invalid_and_mismatched_manifest_metadata_fails_closed() {
    for (key, value) in [
        ("schema", br#"{"type":"record","fields":[]}"#.to_vec()),
        ("schema", br#"{"type":"struct","fields":[]}"#.to_vec()),
        ("schema", b"{".to_vec()),
        ("partition-spec", b"{}".to_vec()),
        ("schema-id", b"8".to_vec()),
        ("schema-id", b"-1".to_vec()),
        ("partition-spec-id", b"invalid".to_vec()),
        ("format-version", b"4".to_vec()),
        ("content", b"positions".to_vec()),
    ] {
        let mut values = metadata(ManifestVersion::V3);
        values.insert(key.to_owned(), value);
        assert!(ManifestMetadata::parse(&values).is_err(), "{key}");
    }
    for key in [
        "schema",
        "partition-spec",
        "schema-id",
        "partition-spec-id",
        "content",
    ] {
        let mut values = metadata(ManifestVersion::V2);
        values.remove(key);
        assert!(ManifestMetadata::parse(&values).is_err(), "{key}");
    }
    let mut values = metadata(ManifestVersion::V1);
    values.insert("content".to_owned(), b"deletes".to_vec());
    assert!(ManifestMetadata::parse(&values).is_err());
    values = metadata(ManifestVersion::V1);
    values.insert("schema".to_owned(), vec![b' '; 1024 * 1024 + 1]);
    assert!(ManifestMetadata::parse(&values).is_err());
}

#[test]
fn manifest_list_binding_checks_content_spec_and_table_before_inheritance() {
    let values = metadata(ManifestVersion::V3);
    let header = ManifestMetadata::parse(&values).unwrap();
    let mut list = ManifestListEntry {
        location: table().file("metadata/manifest.avro").unwrap(),
        length: 42,
        partition_spec_id: 4,
        added_snapshot_id: 99,
        content: ManifestContent::Deletes,
        sequence: 8,
        min_sequence: 6,
        file_counts: [Some(0); 3],
        row_counts: [Some(0); 3],
        first_row_id: None,
    };
    let state = ManifestEntryState::from_list(header, &list, table()).unwrap();
    assert_eq!(state.next_row_id(), None);
    list.partition_spec_id = 5;
    assert!(ManifestEntryState::from_list(header, &list, table()).is_err());
    list.partition_spec_id = 4;
    list.content = ManifestContent::Data;
    assert!(ManifestEntryState::from_list(header, &list, table()).is_err());
    list.content = ManifestContent::Deletes;
    let mut foreign = table();
    foreign.table = crowdb_access_iceberg::key::TableId::from_bytes(&[3; 16]).unwrap();
    assert!(ManifestEntryState::from_list(header, &list, foreign).is_err());

    let values = metadata(ManifestVersion::V1);
    let header = ManifestMetadata::parse(&values).unwrap();
    list.content = ManifestContent::Data;
    assert!(ManifestEntryState::from_list(header, &list, table()).is_ok());
}
