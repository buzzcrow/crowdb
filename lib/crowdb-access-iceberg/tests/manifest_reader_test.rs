#[path = "common/file_blocks.rs"]
mod blocks;
#[path = "common/manifest_entry.rs"]
mod fixture;
#[path = "common/manifest_stream.rs"]
mod stream;
use crowdb_access_iceberg::file::{AvroDatumLimits, AvroLimits, FileRecord};
use crowdb_access_iceberg::manifest::{ManifestListEntry, ManifestReader, ManifestVersion};
use std::sync::{atomic::Ordering, Arc};

async fn open(
    store: Arc<blocks::TestBlocks>,
    record: FileRecord,
    mut list: ManifestListEntry,
    version: ManifestVersion,
) -> Result<ManifestReader, crowdb_access_iceberg::manifest::ManifestEntryError> {
    if version == ManifestVersion::V1 {
        list.min_sequence = 0;
    }
    ManifestReader::open(
        store,
        record,
        list,
        stream::context(version),
        AvroLimits {
            header_bytes: 8192,
            metadata_entries: 8,
            block_bytes: 4096,
            records_per_block: 8,
        },
        AvroDatumLimits {
            depth: 64,
            values: 1000,
            value_bytes: 1024,
        },
        4096,
    )
    .await
}

#[tokio::test]
async fn bound_reader_streams_versions_codecs_and_verifies_totals_at_eof() {
    for version in [ManifestVersion::V1, ManifestVersion::V2, ManifestVersion::V3] {
        for deflate in [false, true] {
            let (store, record) = stream::stored(version, deflate, false).await;
            let mut reader = open(store, record.clone(), stream::list(&record), version)
                .await
                .unwrap();
            for expected in [100, 110] {
                assert!(!reader.is_complete());
                let entry = reader.next_entry().await.unwrap().unwrap();
                assert_eq!(entry.inherited.first_row_id, Some(expected));
                assert_eq!(entry.file.partition, Some(vec![]));
            }
            assert!(reader.next_entry().await.unwrap().is_none());
            assert!(reader.is_complete());
            assert_eq!(reader.next_row_id(), Some(120));
            assert!(reader.next_entry().await.unwrap().is_none());
        }
    }
}

#[tokio::test]
async fn wrong_file_identity_length_kind_or_history_fails_before_entries() {
    let version = ManifestVersion::V3;
    let (store, record) = stream::stored(version, false, false).await;
    for change in 0..4 {
        let mut list = stream::list(&record);
        let mut candidate = record.clone();
        match change {
            0 => list.length += 1,
            1 => list.location = fixture::table().file("metadata/other.avro").unwrap(),
            2 => candidate.kind = crowdb_access_iceberg::file::FileKind::ManifestList,
            _ => list.partition_spec_id = 1,
        }
        assert!(open(store.clone(), candidate, list, version).await.is_err());
    }
}

#[tokio::test]
async fn later_semantic_errors_and_list_count_overruns_preserve_last_good_inheritance() {
    let version = ManifestVersion::V3;
    for corrupt in [false, true] {
        let (store, record) = stream::stored(version, true, corrupt).await;
        let mut list = stream::list(&record);
        if !corrupt {
            list.file_counts[0] = Some(1);
        }
        let mut reader = open(store, record, list, version).await.unwrap();
        assert!(reader.next_entry().await.unwrap().is_some());
        assert!(reader.next_entry().await.is_err());
        assert_eq!(reader.next_row_id(), Some(110));
        assert!(reader.next_entry().await.is_err());
        assert!(!reader.is_complete());
    }
}

#[tokio::test]
async fn missing_entries_are_detected_only_at_eof_and_never_mark_complete() {
    let version = ManifestVersion::V3;
    let (store, record) = stream::stored(version, false, false).await;
    let mut list = stream::list(&record);
    list.file_counts[0] = Some(3);
    let mut reader = open(store, record, list, version).await.unwrap();
    assert!(reader.next_entry().await.unwrap().is_some());
    assert!(reader.next_entry().await.unwrap().is_some());
    assert!(reader.next_entry().await.is_err());
    assert!(!reader.is_complete());
}

#[tokio::test]
async fn cancellation_poisoning_requires_a_fresh_reader_and_replays_from_canonical_bytes() {
    let version = ManifestVersion::V3;
    let (store, record) = stream::stored(version, false, false).await;
    let mut reader = open(store.clone(), record.clone(), stream::list(&record), version)
        .await
        .unwrap();
    store.pause_reads.store(true, Ordering::SeqCst);
    tokio::select! {
        ()=store.read_entered.notified()=>{},
        result=reader.next_entry()=>panic!("read unexpectedly completed: {result:?}"),
    }
    assert!(reader.next_entry().await.is_err());
    assert_eq!(reader.next_row_id(), Some(100));
    store.pause_reads.store(false, Ordering::SeqCst);
    let mut fresh = open(store, record.clone(), stream::list(&record), version)
        .await
        .unwrap();
    assert_eq!(
        fresh.next_entry().await.unwrap().unwrap().inherited.first_row_id,
        Some(100)
    );
}

#[tokio::test]
async fn multiple_entries_in_one_block_advance_exactly_once_and_check_live_minimum() {
    let version = ManifestVersion::V3;
    let (store, record) = stream::stored_with_count(version, true, false, 3).await;
    let mut list = stream::list(&record);
    list.file_counts[0] = Some(6);
    list.row_counts[0] = Some(60);
    let mut reader = open(store.clone(), record.clone(), list, version).await.unwrap();
    for expected in [100, 110, 120, 130, 140, 150] {
        assert_eq!(
            reader.next_entry().await.unwrap().unwrap().inherited.first_row_id,
            Some(expected)
        );
    }
    assert!(reader.next_entry().await.unwrap().is_none());
    let mut list = stream::list(&record);
    list.file_counts[0] = Some(6);
    list.row_counts[0] = Some(60);
    list.min_sequence = 8;
    let mut reader = open(store, record, list, version).await.unwrap();
    for _ in 0..6 {
        assert!(reader.next_entry().await.unwrap().is_some());
    }
    assert!(reader.next_entry().await.is_err());
}
