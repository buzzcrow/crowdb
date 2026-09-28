#[path = "common/file_blocks.rs"]
mod blocks;
#[path = "common/parquet_metadata.rs"]
#[allow(dead_code)]
mod fixture;

use std::sync::atomic::Ordering;

use crowdb_access_iceberg::file::{ContentFormat, FileKind, FileRecord, FormatHint};
use crowdb_access_iceberg::key::TableId;
use crowdb_access_iceberg::manifest::{
    read_selected_parquet_metadata, EntryStatus, FileContentKind, InheritedEntry, ManifestEntry,
    ManifestFileFields, ManifestMetrics, ManifestScalarEntry, SelectedParquetError as Error,
};
use fixture::{footer, limits, stored, structure};

fn entry(record: &FileRecord, content: FileContentKind) -> ManifestScalarEntry {
    ManifestScalarEntry {
        entry: ManifestEntry {
            status: EntryStatus::Added,
            content,
            snapshot_id: None,
            data_sequence: None,
            file_sequence: None,
            first_row_id: None,
            record_count: 10,
        },
        file: ManifestFileFields {
            location: record.location.clone(),
            format: record.format,
            length: record.length,
            sort_order_id: None,
            referenced_data_file: None,
            deletion_vector: None,
            equality_ids: (content == FileContentKind::EqualityDeletes).then_some(vec![3]),
            metrics: ManifestMetrics::default(),
            partition: Some(vec![]),
        },
        inherited: InheritedEntry {
            snapshot_id: 99,
            data_sequence: 1,
            file_sequence: 1,
            first_row_id: None,
        },
    }
}

#[tokio::test]
async fn selected_content_not_extension_binds_the_immutable_upload() {
    for (content, kind) in [
        (FileContentKind::Data, FileKind::Data),
        (FileContentKind::PositionDeletes, FileKind::PositionDelete),
        (FileContentKind::EqualityDeletes, FileKind::EqualityDelete),
    ] {
        let (store, mut record) = stored(&structure(&footer()), 64).await;
        let entry = entry(&record, content);
        let table = record.location.table();
        let original = record.clone();
        let metadata = read_selected_parquet_metadata(store.clone(), &record, &entry, table, limits())
            .await
            .unwrap();
        assert_eq!(metadata.rows, 10);
        assert_eq!(record, original);
        record.kind = kind;
        assert!(
            read_selected_parquet_metadata(store.clone(), &record, &entry, table, limits())
                .await
                .is_ok()
        );
        record.kind = FileKind::Statistics;
        assert!(matches!(
            read_selected_parquet_metadata(store, &record, &entry, table, limits()).await,
            Err(Error::Binding)
        ));
    }
}

#[tokio::test]
async fn descriptor_mismatches_fail_before_reading_canonical_bytes() {
    for invalid in 0..9 {
        let (store, mut record) = stored(&structure(&footer()), 64).await;
        let mut entry = entry(&record, FileContentKind::Data);
        let mut table = record.location.table();
        match invalid {
            0 => table.table = TableId::random(),
            1 => entry.file.location = table.file("data/other.parquet").unwrap(),
            2 => entry.file.length += 1,
            3 => entry.file.format = ContentFormat::Orc,
            4 => record.format = ContentFormat::Orc,
            5 => entry.entry.status = EntryStatus::Deleted,
            6 => entry.entry.record_count = -1,
            7 => entry.file.deletion_vector = Some(FormatHint { offset: 1, length: 1 }),
            _ => record.kind = FileKind::EqualityDelete,
        }
        let reads = store.reads.load(Ordering::SeqCst);
        assert!(matches!(
            read_selected_parquet_metadata(store.clone(), &record, &entry, table, limits()).await,
            Err(Error::Binding)
        ));
        assert_eq!(store.reads.load(Ordering::SeqCst), reads);
    }
}

#[tokio::test]
async fn footer_rows_are_checked_not_trusted_from_the_manifest_or_cached_hint() {
    for rows in [0, 9, 10, 11, i64::MAX] {
        let (store, mut record) = stored(&structure(&footer()), 64).await;
        record.hint = Some(FormatHint { offset: 0, length: 1 });
        let mut entry = entry(&record, FileContentKind::Data);
        entry.entry.record_count = rows;
        let result =
            read_selected_parquet_metadata(store, &record, &entry, record.location.table(), limits()).await;
        if rows == 10 {
            assert_eq!(result.unwrap().rows, 10);
        } else {
            assert!(matches!(result, Err(Error::Rows)));
        }
    }
}
