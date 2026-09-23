#[path = "common/manifest_list.rs"]
mod fixture;

use crowdb_access_iceberg::file::AvroDatumLimits;
use crowdb_access_iceberg::manifest::{ManifestContent, ManifestListProjection, ManifestVersion};
use fixture::{table, TestManifestList};
use serde_json::json;

#[test]
fn upgraded_tables_read_old_fields_by_id_without_guessing_writer_version() {
    for table_version in [ManifestVersion::V2, ManifestVersion::V3] {
        for old_layout in [true, false] {
            let mut fixture = TestManifestList::new();
            if old_layout {
                fixture.fields.truncate(4);
            } else {
                fixture.fields.retain(|(id, _, _)| !matches!(id, 504 | 512 | 520));
                fixture.set(505, json!(null));
                fixture.set(513, json!(null));
                fixture.set(517, json!(null));
                fixture.set(515, json!(null));
                fixture.set(516, json!(null));
            }
            fixture.fields.reverse();
            let schema = fixture.schema();
            let projection = ManifestListProjection::for_read(&schema, table_version, table()).unwrap();
            let bytes = fixture.bytes();
            let mut reader = projection
                .records(
                    &bytes,
                    1,
                    AvroDatumLimits {
                        depth: 64,
                        values: 1000,
                        value_bytes: 1024,
                    },
                )
                .unwrap();
            let entry = reader.next_entry().unwrap().unwrap();
            assert_eq!(entry.content, ManifestContent::Data);
            assert_eq!((entry.sequence, entry.min_sequence), (0, 0));
            assert_eq!(entry.file_counts[0..2], [None, None]);
            assert_eq!(entry.row_counts[0..2], [None, None]);
            assert_eq!(entry.first_row_id, None);
            assert!(reader.next_entry().unwrap().is_none());
            assert!(ManifestListProjection::new(&schema, table_version, table()).is_err());
        }
    }
}

#[test]
fn compatibility_never_defaults_common_required_fields_or_bad_present_values() {
    for (id, value) in [
        (503, json!(null)),
        (515, json!(-1)),
        (517, json!(2)),
        (504, json!(-1)),
    ] {
        let mut fixture = TestManifestList::new();
        fixture.set(id, value);
        let schema = fixture.schema();
        let projection = ManifestListProjection::for_read(&schema, ManifestVersion::V3, table()).unwrap();
        let bytes = fixture.bytes();
        let mut reader = projection
            .records(
                &bytes,
                1,
                AvroDatumLimits {
                    depth: 64,
                    values: 1000,
                    value_bytes: 1024,
                },
            )
            .unwrap();
        assert!(reader.next_entry().is_err());
        assert!(reader.next_entry().is_err());
    }
    let mut fixture = TestManifestList::new();
    fixture.fields.retain(|(id, _, _)| *id != 503);
    assert!(ManifestListProjection::for_read(&fixture.schema(), ManifestVersion::V3, table()).is_err());
}
