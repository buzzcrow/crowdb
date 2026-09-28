use std::sync::Arc;

use crowdb_access_iceberg::{
    file::{
        AvroDatumLimits, AvroLimits, ContentFormat, FileContent, FileIdentity, FileKind, FileRecord,
        FileTreeWriter, TableLocation,
    },
    gc::{avro_links, metadata_links, AvroMarkCursor, AvroMarkLimits, ReachableKind},
    key::{CatalogId, FileId, TableId},
    table::TableMetadataLimits,
};
use serde_json::json;

#[path = "common/file_blocks.rs"]
mod blocks;

fn table() -> TableLocation {
    TableLocation {
        catalog: CatalogId::random(),
        table: TableId::random(),
    }
}

fn metadata_limits() -> TableMetadataLimits {
    TableMetadataLimits {
        bytes: 65536,
        values: 1000,
        depth: 16,
        string_bytes: 4096,
        collection_entries: 100,
    }
}

#[test]
fn metadata_reachability_keeps_retained_snapshots_and_auxiliary_files_for_all_versions() {
    let table = table();
    let owner = table.file("metadata/current.json").unwrap();
    let location = |path: &str| table.file(path).unwrap().to_string();
    for version in [1, 2, 3] {
        let mut metadata = json!({
            "format-version": version,
            "metadata-log": [{"metadata-file": location("metadata/old.json")}],
            "snapshots": [{"manifest-list": location("metadata/retained.avro")},
                {"manifest-list": location("metadata/tagged.avro")}],
            "statistics": [{"statistics-path": location("metadata/stat.puffin")}],
            "partition-statistics": [{"statistics-path": location("metadata/part.parquet")}]
        });
        let links =
            metadata_links(&serde_json::to_vec(&metadata).unwrap(), &owner, metadata_limits()).unwrap();
        assert_eq!(links.len(), 5);
        assert_eq!(links[0].kind, ReachableKind::File);
        assert_eq!(links[1].kind, ReachableKind::ManifestList);
        assert_eq!(links[2].kind, ReachableKind::ManifestList);
        metadata["snapshots"] = json!([{"manifests": [location("metadata/manifest.avro")]}]);
        let legacy = metadata_links(&serde_json::to_vec(&metadata).unwrap(), &owner, metadata_limits());
        if version == 1 {
            assert_eq!(legacy.unwrap()[1].kind, ReachableKind::Manifest);
        } else {
            assert!(legacy.is_err());
        }
    }
}

#[test]
fn metadata_reachability_fails_closed_on_foreign_paths_and_bounded_collections() {
    let owner = table().file("metadata/current.json").unwrap();
    let metadata = json!({"format-version": 3, "statistics": [{"statistics-path": table().file("stat.puffin").unwrap().to_string()}]});
    assert!(metadata_links(&serde_json::to_vec(&metadata).unwrap(), &owner, metadata_limits()).is_err());
    let metadata = json!({"format-version": 3, "snapshots": [
        {"manifest-list": owner.table().file("first.avro").unwrap().to_string()},
        {"manifest-list": owner.table().file("second.avro").unwrap().to_string()}
    ]});
    let mut limits = metadata_limits();
    limits.collection_entries = 1;
    assert!(metadata_links(&serde_json::to_vec(&metadata).unwrap(), &owner, limits).is_err());
    assert!(metadata_links(
        br#"{"format-version":3,"snapshots":null}"#,
        &owner,
        metadata_limits()
    )
    .is_err());
}

fn long(bytes: &mut Vec<u8>, value: i64) {
    let mut encoded = u64::from_ne_bytes(((value << 1) ^ (value >> 63)).to_ne_bytes());
    while encoded >= 128 {
        bytes.push(u8::try_from(encoded & 127).unwrap() | 128);
        encoded >>= 7;
    }
    bytes.push(u8::try_from(encoded).unwrap());
}

fn sized(bytes: &mut Vec<u8>, value: &[u8]) {
    long(bytes, i64::try_from(value.len()).unwrap());
    bytes.extend_from_slice(value);
}

async fn manifest(store: Arc<blocks::TestBlocks>, table: TableLocation) -> FileRecord {
    let schema = json!({"type":"record","name":"entry","fields":[
        {"name":"status","field-id":0,"type":"int"},
        {"name":"data_file","field-id":2,"type":{"type":"record","name":"file","fields":[
            {"name":"file_path","field-id":100,"type":"string"},
            {"name":"referenced_data_file","field-id":143,"type":["null","string"]}
        ]}}
    ]});
    let mut bytes = b"Obj\x01".to_vec();
    long(&mut bytes, 1);
    sized(&mut bytes, b"avro.schema");
    sized(&mut bytes, &serde_json::to_vec(&schema).unwrap());
    long(&mut bytes, 0);
    bytes.extend([42; 16]);
    let mut records = Vec::new();
    for status in [0, 1, 2] {
        long(&mut records, status);
        sized(
            &mut records,
            table
                .file(&format!("data/{status}.puffin"))
                .unwrap()
                .to_string()
                .as_bytes(),
        );
        long(&mut records, 1);
        sized(
            &mut records,
            table
                .file(&format!("data/{status}.parquet"))
                .unwrap()
                .to_string()
                .as_bytes(),
        );
    }
    long(&mut bytes, 3);
    sized(&mut bytes, &records);
    bytes.extend([42; 16]);
    let owner = FileIdentity {
        table,
        file: FileId::random(),
    };
    let mut writer = FileTreeWriter::new(store, owner, 256).unwrap();
    writer.push(&bytes).await.unwrap();
    let tree = writer.finish().await.unwrap();
    FileRecord {
        file: owner.file,
        location: table.file("metadata/manifest.avro").unwrap(),
        kind: FileKind::Manifest,
        format: ContentFormat::Avro,
        length: tree.length,
        digest: tree.digest,
        content: FileContent::Chunks { root: tree.root },
        hint: None,
    }
}

#[tokio::test]
async fn manifest_pages_preserve_live_dv_references_and_skip_deleted_entries() {
    let store = Arc::new(blocks::TestBlocks::default());
    let file = manifest(store.clone(), table()).await;
    let limits = AvroMarkLimits {
        framing: AvroLimits {
            header_bytes: 4096,
            metadata_entries: 8,
            block_bytes: 4096,
            records_per_block: 100,
        },
        datum: AvroDatumLimits {
            depth: 16,
            values: 100,
            value_bytes: 4096,
        },
        decoded_bytes: 4096,
        page_items: 2,
    };
    let mut cursor = AvroMarkCursor::default();
    let mut links = Vec::new();
    let mut complete = false;
    for _ in 0..5 {
        let page = avro_links(store.clone(), &file, ReachableKind::Manifest, &cursor, limits)
            .await
            .unwrap();
        assert!(page.links.len() <= 2);
        links.extend(page.links);
        cursor = page.next;
        complete = page.complete;
        if complete {
            break;
        }
    }
    assert!(complete);
    assert_eq!(links.len(), 4);
    for (index, path) in [
        "data/0.puffin",
        "data/0.parquet",
        "data/1.puffin",
        "data/1.parquet",
    ]
    .iter()
    .enumerate()
    {
        assert_eq!(links[index].location, file.location.table().file(path).unwrap());
        assert_eq!(links[index].kind, ReachableKind::File);
    }
    let mut invalid = limits;
    invalid.page_items = 1;
    assert!(
        avro_links(store, &file, ReachableKind::Manifest, &cursor, invalid)
            .await
            .is_err()
    );
}
