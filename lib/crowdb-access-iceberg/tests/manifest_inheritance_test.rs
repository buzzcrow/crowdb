use crowdb_access_iceberg::manifest::{
    EntryStatus, FileContentKind, ManifestContent, ManifestEntry, ManifestInheritance,
    ManifestInheritanceError, ManifestVersion,
};

fn entry(status: EntryStatus) -> ManifestEntry {
    ManifestEntry {
        status,
        content: FileContentKind::Data,
        snapshot_id: None,
        data_sequence: None,
        file_sequence: None,
        first_row_id: None,
        record_count: 10,
    }
}

#[test]
fn version_one_sequences_default_to_zero_even_in_an_upgraded_snapshot() {
    let mut resolver =
        ManifestInheritance::new(ManifestVersion::V1, ManifestContent::Data, 50, 9, Some(100)).unwrap();
    let mut entry = entry(EntryStatus::Existing);
    assert_eq!(resolver.resolve(entry), Err(ManifestInheritanceError::Missing));
    assert_eq!(resolver.next_row_id(), Some(100));
    entry.snapshot_id = Some(20);
    let result = resolver.resolve(entry).unwrap();
    assert_eq!(result.snapshot_id, 20);
    assert_eq!(result.data_sequence, 0);
    assert_eq!(result.file_sequence, 0);
    assert_eq!(result.first_row_id, Some(100));
    assert_eq!(resolver.next_row_id(), Some(110));
}

#[test]
fn sequence_inheritance_is_only_for_added_files_and_preserves_explicit_age() {
    for version in [ManifestVersion::V2, ManifestVersion::V3] {
        let mut resolver = ManifestInheritance::new(version, ManifestContent::Data, 50, 9, None).unwrap();
        let added = resolver.resolve(entry(EntryStatus::Added)).unwrap();
        assert_eq!(added.snapshot_id, 50);
        assert_eq!(added.data_sequence, 9);
        assert_eq!(added.file_sequence, 9);
        for status in [EntryStatus::Existing, EntryStatus::Deleted] {
            let mut value = entry(status);
            assert_eq!(resolver.resolve(value), Err(ManifestInheritanceError::Missing));
            value.data_sequence = Some(3);
            assert_eq!(resolver.resolve(value), Err(ManifestInheritanceError::Missing));
            value.file_sequence = Some(5);
            let result = resolver.resolve(value).unwrap();
            assert_eq!(result.data_sequence, 3);
            assert_eq!(result.file_sequence, 5);
        }
        let mut value = entry(EntryStatus::Added);
        value.data_sequence = Some(1);
        assert_eq!(resolver.resolve(value).unwrap().data_sequence, 1);
    }
}

#[test]
fn row_ids_advance_only_for_unassigned_data_files_in_manifest_order() {
    let mut resolver =
        ManifestInheritance::new(ManifestVersion::V3, ManifestContent::Data, 50, 9, Some(1000)).unwrap();
    let mut existing = entry(EntryStatus::Existing);
    existing.data_sequence = Some(2);
    existing.file_sequence = Some(2);
    existing.first_row_id = Some(100);
    assert_eq!(resolver.resolve(existing).unwrap().first_row_id, Some(100));
    assert_eq!(resolver.next_row_id(), Some(1000));
    for status in [EntryStatus::Added, EntryStatus::Existing, EntryStatus::Deleted] {
        let expected = resolver.next_row_id();
        let mut value = existing;
        value.status = status;
        value.first_row_id = None;
        assert_eq!(resolver.resolve(value).unwrap().first_row_id, expected);
    }
    assert_eq!(resolver.next_row_id(), Some(1030));
    let mut legacy =
        ManifestInheritance::new(ManifestVersion::V2, ManifestContent::Data, 1, 1, None).unwrap();
    assert_eq!(
        legacy.resolve(entry(EntryStatus::Added)).unwrap().first_row_id,
        None
    );
}

#[test]
fn delete_manifests_cannot_mix_data_or_inherit_row_ids() {
    assert!(ManifestInheritance::new(ManifestVersion::V1, ManifestContent::Deletes, 1, 0, None).is_err());
    assert!(ManifestInheritance::new(ManifestVersion::V3, ManifestContent::Deletes, 1, 1, Some(0)).is_err());
    let mut resolver =
        ManifestInheritance::new(ManifestVersion::V3, ManifestContent::Deletes, 1, 1, None).unwrap();
    assert_eq!(
        resolver.resolve(entry(EntryStatus::Added)),
        Err(ManifestInheritanceError::Content)
    );
    for content in [FileContentKind::PositionDeletes, FileContentKind::EqualityDeletes] {
        let mut value = entry(EntryStatus::Added);
        value.content = content;
        assert_eq!(resolver.resolve(value).unwrap().first_row_id, None);
        value.first_row_id = Some(0);
        assert_eq!(resolver.resolve(value), Err(ManifestInheritanceError::Content));
    }
}

#[test]
fn invalid_entry_or_row_id_overflow_never_advances_the_cursor() {
    let mut resolver = ManifestInheritance::new(
        ManifestVersion::V3,
        ManifestContent::Data,
        1,
        1,
        Some(i64::MAX - 1),
    )
    .unwrap();
    assert_eq!(
        resolver.resolve(entry(EntryStatus::Added)),
        Err(ManifestInheritanceError::Overflow)
    );
    assert_eq!(resolver.next_row_id(), Some(i64::MAX - 1));
    let mut value = entry(EntryStatus::Added);
    value.record_count = -1;
    assert_eq!(resolver.resolve(value), Err(ManifestInheritanceError::Number));
    value.record_count = 1;
    value.data_sequence = Some(-1);
    assert_eq!(resolver.resolve(value), Err(ManifestInheritanceError::Number));
    value.data_sequence = None;
    assert!(resolver.resolve(value).is_ok());
    assert_eq!(resolver.next_row_id(), Some(i64::MAX));
}
