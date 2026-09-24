#[path = "common/file_blocks.rs"]
mod blocks;
#[path = "common/manifest_entry.rs"]
#[allow(dead_code)]
mod fixture;
#[path = "common/manifest_stream.rs"]
#[allow(dead_code)]
mod stream;

use async_trait::async_trait;
use crowdb_access_iceberg::{
    file::{FileLocation, FileRecord},
    manifest::{
        ManifestContext, ManifestVersion, SnapshotIdentityLimits, SnapshotManifestError,
        SnapshotManifestLimits, SnapshotManifestReader, SnapshotManifestSource,
    },
};
use std::sync::Arc;

#[tokio::test]
async fn legacy_enumeration_completes_all_canonical_manifests_and_checks_work_limits() {
    use crowdb_access_iceberg::file::{FileContent, FileIdentity, FileReader, FileTreeWriter};
    let (store, mut record) = stream::stored(ManifestVersion::V1, false, false).await;
    let mut reader = FileReader::new(store.clone(), record.clone(), None, 8192).unwrap();
    let mut bytes = Vec::new();
    while let Some(chunk) = reader.next().await.unwrap() {
        bytes.extend(chunk);
    }
    let needle = b"data/file.parquet";
    let offset = bytes
        .windows(needle.len())
        .enumerate()
        .filter(|(_, value)| *value == needle)
        .nth(1)
        .unwrap()
        .0;
    bytes[offset..offset + needle.len()].copy_from_slice(b"data/next.parquet");
    record.file = crowdb_access_iceberg::key::FileId::random();
    let mut writer = FileTreeWriter::new(
        store.clone(),
        FileIdentity {
            table: fixture::table(),
            file: record.file,
        },
        64,
    )
    .unwrap();
    writer.push(&bytes).await.unwrap();
    let tree = writer.finish().await.unwrap();
    record.length = tree.length;
    record.digest = tree.digest;
    record.content = FileContent::Chunks { root: tree.root };
    let source = Arc::new(TestSource(record.clone()));
    for bounded in [false, true] {
        let mut limits = limits();
        if bounded {
            limits.entries = 1;
        }
        let mut reader = SnapshotManifestReader::open_legacy(
            store.clone(),
            source.clone(),
            fixture::table(),
            99,
            vec![record.location.clone()],
            limits,
        )
        .unwrap();
        assert!(reader.next_entry().await.unwrap().is_some());
        if bounded {
            assert!(reader.next_entry().await.is_err());
        } else {
            assert!(reader.next_entry().await.unwrap().is_some());
            assert!(reader.next_entry().await.unwrap().is_none());
            assert_eq!(
                (
                    reader.finish().unwrap().manifests,
                    reader.finish().unwrap().entries
                ),
                (1, 2)
            );
        }
    }
}

struct TestSource(FileRecord);

#[async_trait]
impl SnapshotManifestSource for TestSource {
    async fn resolve(
        &self,
        location: &FileLocation,
    ) -> Result<(FileRecord, ManifestContext), SnapshotManifestError> {
        if *location != self.0.location {
            return Err(SnapshotManifestError::Unavailable);
        }
        Ok((self.0.clone(), stream::context(ManifestVersion::V1)))
    }
}

fn limits() -> SnapshotManifestLimits {
    SnapshotManifestLimits {
        framing: crowdb_access_iceberg::file::AvroLimits {
            header_bytes: 8192,
            metadata_entries: 10,
            block_bytes: 8192,
            records_per_block: 10,
        },
        datum: crowdb_access_iceberg::file::AvroDatumLimits {
            depth: 64,
            values: 2000,
            value_bytes: 2048,
        },
        decoded_bytes: 8192,
        manifests: 10,
        entries: 10,
        manifest_bytes: 100_000,
        identity: SnapshotIdentityLimits {
            keys: 100,
            key_bytes: 10000,
        },
    }
}

#[tokio::test]
async fn legacy_enumeration_checks_sequences_duplicates_and_poisoning_without_a_list_file() {
    let (store, record) = stream::stored(ManifestVersion::V1, false, false).await;
    let source = Arc::new(TestSource(record.clone()));
    let mut reader = SnapshotManifestReader::open_legacy(
        store.clone(),
        source.clone(),
        fixture::table(),
        99,
        vec![record.location.clone()],
        limits(),
    )
    .unwrap();
    let entry = reader.next_entry().await.unwrap().unwrap();
    assert_eq!(entry.inherited.data_sequence, 0);
    assert_eq!(entry.inherited.file_sequence, 0);
    assert!(reader.finish().is_err());
    assert!(reader.next_entry().await.is_err());
    assert!(reader.next_entry().await.is_err());
    assert!(reader.finish().is_err());
    let mut empty =
        SnapshotManifestReader::open_legacy(store, source, fixture::table(), 99, vec![], limits()).unwrap();
    assert!(empty.next_entry().await.unwrap().is_none());
    assert_eq!(empty.finish().unwrap().entries, 0);
}

#[tokio::test]
async fn legacy_snapshots_reject_newer_writer_headers_even_with_a_legacy_context() {
    let (store, record) = stream::stored(ManifestVersion::V2, false, false).await;
    let source = Arc::new(TestSource(record.clone()));
    let mut reader = SnapshotManifestReader::open_legacy(
        store,
        source,
        fixture::table(),
        99,
        vec![record.location],
        limits(),
    )
    .unwrap();
    assert!(reader.next_entry().await.is_err());
    assert!(reader.finish().is_err());
}
