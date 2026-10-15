#[path = "common/file_blocks.rs"]
mod blocks;
#[path = "common/store.rs"]
mod common;
#[path = "common/file.rs"]
mod fixture;

use std::{collections::BTreeMap, sync::Arc};

use crowdb_access_iceberg::{
    file::{
        ByteRange, DirectFileLocations, FileIdentity, FileReader, FileRepository, FileTreeWriter,
        TableLocation,
    },
    key::{CatalogId, FileId, NamespaceId, TableId},
};

fn table() -> TableLocation {
    TableLocation {
        storage_namespace: Some(NamespaceId::random()),
        catalog: CatalogId::random(),
        table: TableId::random(),
    }
}

#[tokio::test]
async fn identical_replacement_at_the_same_root_extent_cannot_revive_a_reclaimed_locator() {
    let blocks = Arc::new(blocks::TestBlocks::default());
    let table = table();
    let signer = DirectFileLocations::new(1, BTreeMap::from([(1, [1; 32])])).unwrap();
    let bytes = b"identical parquet bytes";
    let mut files = Vec::new();
    for _ in 0..2 {
        let file = FileId::random();
        let mut writer = FileTreeWriter::new(blocks.clone(), FileIdentity { table, file }, 1024).unwrap();
        writer.push(bytes).await.unwrap();
        let tree = writer.finish().await.unwrap();
        let root = tree.root.unwrap();
        assert!(root.height > 0);
        files.push((
            signer
                .parquet(table, "part", file, tree.length, tree.digest, root.clone())
                .unwrap(),
            root,
        ));
    }
    assert_ne!(files[0].0.location, files[1].0.location);
    let mut reused = (*blocks.values.load_full()).clone();
    reused.insert(files[0].1.chunk.low, reused[&files[1].1.chunk.low].clone());
    blocks.values.store(Arc::new(reused));
    let old = signer.resolve(&files[0].0.location).unwrap().unwrap();
    let mut reader =
        FileReader::new(blocks.clone(), old, Some(ByteRange { start: 0, end: 3 }), 1024).unwrap();
    assert!(reader.next().await.is_err());
    let mut reader = FileReader::new(blocks, files[1].0.clone(), None, 1024).unwrap();
    assert_eq!(reader.next().await.unwrap().unwrap(), bytes);
}

#[tokio::test]
async fn injection_replay_and_logical_delete_do_not_add_direct_read_metadata_queries() {
    use std::sync::atomic::Ordering;
    let mut fixture = fixture::TestFile::new(common::TestStore::default()).await;
    fixture.table.storage_namespace = Some(NamespaceId::random());
    let owner = FileIdentity {
        table: fixture.table,
        file: FileId::random(),
    };
    let blocks = Arc::new(blocks::TestBlocks::default());
    let mut writer = FileTreeWriter::new(blocks.clone(), owner, 1024).unwrap();
    writer.push(b"retained parquet bytes").await.unwrap();
    let tree = writer.finish().await.unwrap();
    let signer = DirectFileLocations::new(1, BTreeMap::from([(1, [1; 32])])).unwrap();
    let repository = FileRepository::new(fixture.store.clone());
    repository
        .publish(fixture.context, &fixture.record("metadata/v1.json", b"{}"))
        .await
        .unwrap();
    fixture.store.file_record_reply_loss.store(true, Ordering::SeqCst);
    assert!(repository
        .finalize_native_parquet(fixture.context, owner, "part", tree.clone(), &signer)
        .await
        .is_err());
    let published = repository
        .finalize_native_parquet(fixture.context, owner, "part", tree, &signer)
        .await
        .unwrap();
    repository
        .mark_deleted(fixture.context, &published, 100)
        .await
        .unwrap();
    assert!(repository
        .load(fixture.context, &published.location)
        .await
        .unwrap()
        .is_none());
    let before = fixture.store.reads.load(Ordering::SeqCst);
    let record = signer.resolve(&published.location).unwrap().unwrap();
    let mut reader = FileReader::new(blocks.clone(), record, None, 1024).unwrap();
    assert_eq!(reader.next().await.unwrap().unwrap(), b"retained parquet bytes");
    assert_eq!(fixture.store.reads.load(Ordering::SeqCst), before);
    blocks.values.store(Arc::new(BTreeMap::new()));
    let mut reclaimed = FileReader::new(
        blocks,
        signer.resolve(&published.location).unwrap().unwrap(),
        None,
        1024,
    )
    .unwrap();
    assert!(reclaimed.next().await.is_err());
}

#[tokio::test]
async fn direct_urls_read_multi_extent_ranges_without_any_file_repository() {
    let blocks = Arc::new(blocks::TestBlocks::default());
    let table = table();
    let file = FileId::random();
    let owner = FileIdentity { table, file };
    let mut writer = FileTreeWriter::new(blocks.clone(), owner, 3).unwrap();
    writer.push(b"abcdefghij").await.unwrap();
    let tree = writer.finish().await.unwrap();
    let signer = DirectFileLocations::new(1, BTreeMap::from([(1, [1; 32])])).unwrap();
    let record = signer
        .parquet(
            table,
            "part.0001",
            file,
            tree.length,
            tree.digest,
            tree.root.unwrap(),
        )
        .unwrap();
    let location = record.location.to_string().parse().unwrap();
    let resolved = signer.resolve(&location).unwrap().unwrap();
    assert_eq!(resolved, record);
    assert!(location.object_key().len() <= 1024);
    let mut reader = FileReader::new(blocks, resolved, Some(ByteRange { start: 2, end: 9 }), 3).unwrap();
    let mut bytes = Vec::new();
    while let Some(chunk) = reader.next().await.unwrap() {
        bytes.extend_from_slice(&chunk);
    }
    assert_eq!(bytes, b"cdefghi");
}

#[tokio::test]
async fn locator_rotation_preserves_old_urls_and_tampering_never_resolves() {
    let blocks = Arc::new(blocks::TestBlocks::default());
    let table = table();
    let file = FileId::random();
    let mut writer = FileTreeWriter::new(blocks, FileIdentity { table, file }, 1024).unwrap();
    writer.push(b"retained bytes").await.unwrap();
    let tree = writer.finish().await.unwrap();
    let signer = DirectFileLocations::new(1, BTreeMap::from([(1, [1; 32])])).unwrap();
    let record = signer
        .parquet(table, "part", file, tree.length, tree.digest, tree.root.unwrap())
        .unwrap();
    let rotated = DirectFileLocations::new(2, BTreeMap::from([(1, [1; 32]), (2, [2; 32])])).unwrap();
    assert_eq!(rotated.resolve(&record.location).unwrap(), Some(record.clone()));
    let mut foreign = table;
    foreign.storage_namespace = Some(NamespaceId::random());
    assert!(rotated
        .resolve(&foreign.file(record.location.relative_key()).unwrap())
        .is_err());
    let changed_name = record.location.relative_key().replacen("part.", "other.", 1);
    assert!(rotated.resolve(&table.file(&changed_name).unwrap()).is_err());
    let uri = record.location.to_string();
    let marker = uri.find("part.").unwrap() + 5;
    let mut changed = uri.into_bytes();
    changed[marker + 20] = if changed[marker + 20] == b'A' { b'B' } else { b'A' };
    assert!(rotated
        .resolve(&String::from_utf8(changed).unwrap().parse().unwrap())
        .is_err());
    assert!(rotated
        .resolve(&table.file("metadata/v1.json").unwrap())
        .unwrap()
        .is_none());
}
