#[path = "common/manifest_entry.rs"]
#[allow(dead_code)]
mod fixture;

use crowdb_access_iceberg::file::{AvroDatumLimits, ContentFormat, FormatHint};
use crowdb_access_iceberg::manifest::{
    EntryStatus, FileContentKind, ManifestContent, ManifestEntryProjection, ManifestEntryState,
    ManifestScalarEntry, ManifestVersion, SnapshotIdentityError as Error, SnapshotIdentityIndex,
    SnapshotIdentityLimits,
};

fn index() -> SnapshotIdentityIndex {
    SnapshotIdentityIndex::new(
        fixture::table(),
        SnapshotIdentityLimits {
            keys: 100,
            key_bytes: 8192,
        },
    )
    .unwrap()
}

fn entry(path: &str) -> ManifestScalarEntry {
    let fixture = fixture::TestManifestEntry::new(ManifestVersion::V3);
    let schema = fixture.schema();
    let projection = ManifestEntryProjection::new(&schema, ManifestVersion::V3, fixture::table()).unwrap();
    let mut state = ManifestEntryState::new(
        ManifestVersion::V3,
        fixture::table(),
        ManifestContent::Data,
        99,
        9,
        Some(100),
    )
    .unwrap();
    let mut entry = projection
        .records(
            &fixture.bytes(),
            1,
            AvroDatumLimits {
                depth: 64,
                values: 1000,
                value_bytes: 1024,
            },
            &mut state,
        )
        .unwrap()
        .next_entry()
        .unwrap()
        .unwrap();
    entry.file.location = fixture::table().file(path).unwrap();
    entry
}

fn vector(path: &str, target: &str, offset: u64) -> ManifestScalarEntry {
    let mut entry = entry(path);
    entry.entry.content = FileContentKind::PositionDeletes;
    entry.entry.first_row_id = None;
    entry.inherited.first_row_id = None;
    entry.file.format = ContentFormat::Puffin;
    entry.file.length = 1000;
    entry.file.deletion_vector = Some(FormatHint { offset, length: 100 });
    entry.file.referenced_data_file = Some(fixture::table().file(target).unwrap());
    entry
}

#[test]
fn duplicate_live_paths_and_manifests_fail_but_deleted_entries_are_not_live() {
    let mut index = index();
    let mut entry = entry("data/a");
    entry.entry.status = EntryStatus::Deleted;
    index.observe_entry(&entry).unwrap();
    index.observe_entry(&entry).unwrap();
    entry.entry.status = EntryStatus::Added;
    index.observe_entry(&entry).unwrap();
    entry.entry.status = EntryStatus::Existing;
    assert!(matches!(index.observe_entry(&entry), Err(Error::Duplicate)));
    assert!(matches!(index.observe_entry(&entry), Err(Error::Failed)));
    let mut index = self::index();
    let path = fixture::table().file("metadata/a").unwrap();
    index.observe_manifest(&path).unwrap();
    assert!(matches!(index.observe_manifest(&path), Err(Error::Duplicate)));
}

#[test]
fn puffin_files_may_hold_distinct_vectors_in_any_order() {
    let mut index = index();
    for (target, offset) in [("data/c", 300), ("data/a", 100), ("data/b", 200)] {
        index
            .observe_entry(&vector("deletes/shared", target, offset))
            .unwrap();
    }
    index
        .observe_entry(&vector("deletes/other", "data/d", 100))
        .unwrap();
}

#[test]
fn repeated_targets_overlapping_spans_and_conflicting_physical_sizes_fail() {
    for invalid in 0..6 {
        let mut index = index();
        index
            .observe_entry(&vector("deletes/shared", "data/a", 100))
            .unwrap();
        let mut candidate = vector("deletes/shared", "data/b", 200);
        match invalid {
            0 => candidate.file.referenced_data_file = Some(fixture::table().file("data/a").unwrap()),
            1 => candidate.file.deletion_vector.as_mut().unwrap().offset = 150,
            2 => candidate.file.deletion_vector.as_mut().unwrap().offset = 50,
            3 => candidate.file.length += 1,
            4 => candidate.file.deletion_vector.as_mut().unwrap().offset = 950,
            _ => candidate.file.deletion_vector.as_mut().unwrap().offset = u64::MAX,
        }
        assert!(index.observe_entry(&candidate).is_err());
        assert!(matches!(
            index.observe_entry(&entry("data/fresh")),
            Err(Error::Failed)
        ));
    }
}

#[test]
fn identity_node_and_key_byte_caps_are_independent_and_never_evict() {
    for limits in [
        SnapshotIdentityLimits {
            keys: 1,
            key_bytes: 100,
        },
        SnapshotIdentityLimits {
            keys: 100,
            key_bytes: 6,
        },
    ] {
        let mut index = SnapshotIdentityIndex::new(fixture::table(), limits).unwrap();
        index.observe_entry(&entry("data/a")).unwrap();
        assert!(matches!(
            index.observe_entry(&entry("data/b")),
            Err(Error::Bounds)
        ));
        assert!(matches!(
            index.observe_entry(&entry("data/a")),
            Err(Error::Failed)
        ));
    }
    for limits in [
        SnapshotIdentityLimits {
            keys: 0,
            key_bytes: 1,
        },
        SnapshotIdentityLimits {
            keys: usize::MAX,
            key_bytes: 1,
        },
        SnapshotIdentityLimits {
            keys: 1,
            key_bytes: 0,
        },
        SnapshotIdentityLimits {
            keys: 1,
            key_bytes: usize::MAX,
        },
    ] {
        assert!(SnapshotIdentityIndex::new(fixture::table(), limits).is_err());
    }
}

#[test]
fn foreign_tables_cannot_alias_relative_keys() {
    let mut candidate = entry("data/a");
    let mut table = fixture::table();
    table.table = crowdb_access_iceberg::key::TableId::random();
    candidate.file.location = table.file("data/a").unwrap();
    assert!(matches!(index().observe_entry(&candidate), Err(Error::Binding)));
    assert!(matches!(
        index().observe_manifest(&candidate.file.location),
        Err(Error::Binding)
    ));
}
