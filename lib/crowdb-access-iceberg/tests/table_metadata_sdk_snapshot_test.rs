#[path = "common/table_metadata.rs"]
#[allow(dead_code)]
mod fixture;
#[path = "common/metadata_snapshot_fixture.rs"]
mod snapshots;

use crowdb_access_iceberg::table::TableMetadataDocument;
use serde_json::Value;

#[test]
fn pinned_sdk_nonempty_metadata_preserves_snapshots_refs_and_row_lineage() {
    for bytes in snapshots::files() {
        let value: Value = serde_json::from_slice(&bytes).unwrap();
        let version = u8::try_from(value["format-version"].as_u64().unwrap()).unwrap();
        let head = fixture::head(
            &bytes,
            version,
            Some(uuid::Uuid::parse_str(value["table-uuid"].as_str().unwrap()).unwrap()),
        );
        let document = TableMetadataDocument::parse(bytes.clone(), &head, fixture::limits()).unwrap();
        assert_eq!(document.canonical(), bytes);
        assert_eq!(document.current_snapshot(), Some(20));
        assert_eq!(document.snapshots().len(), 2);
        assert_eq!(document.fields()["refs"]["release"]["snapshot-id"], 10);
        assert_eq!(document.fields()["refs"]["main"]["snapshot-id"], 20);
        let first = &document.snapshots()[&10];
        let second = &document.snapshots()[&20];
        assert_eq!(first.parent_snapshot_id, None);
        assert_eq!(second.parent_snapshot_id, Some(10));
        assert_eq!(first.sequence, i64::from(version != 1));
        assert_eq!(second.sequence, 2 * i64::from(version != 1));
        if version == 3 {
            assert_eq!(first.first_row_id, Some(0));
            assert_eq!(first.added_rows, Some(2));
            assert_eq!(second.first_row_id, Some(2));
            assert_eq!(second.added_rows, Some(3));
            assert_eq!(document.fields()["next-row-id"], 5);
        } else {
            assert_eq!(first.first_row_id, None);
            assert_eq!(second.added_rows, None);
        }
        let context = document.manifest_context(0, 0, &[], 1000).unwrap();
        assert_eq!(context.schema_id(), 0);
        assert!(context.partitions().is_empty());
    }
}
