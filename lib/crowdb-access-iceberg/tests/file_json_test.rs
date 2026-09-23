#[path = "common/file_blocks.rs"]
mod blocks;

use std::sync::{atomic::Ordering, Arc};
use std::time::Duration;

use blocks::TestBlocks;
use crowdb_access_iceberg::file::{
    ContentFormat, FileContent, FileIdentity, FileKind, FileRecord, FileTreeWriter, JsonSealError,
    JsonSealer, TableLocation,
};
use crowdb_access_iceberg::key::{CatalogId, FileId, TableId};

async fn record(store: Arc<TestBlocks>, bytes: &[u8], block_bytes: usize) -> FileRecord {
    let owner = FileIdentity {
        table: TableLocation {
            catalog: CatalogId::random(),
            table: TableId::random(),
        },
        file: FileId::random(),
    };
    let mut writer = FileTreeWriter::new(store, owner, block_bytes).unwrap();
    writer.push(bytes).await.unwrap();
    let tree = writer.finish().await.unwrap();
    FileRecord {
        file: owner.file,
        location: owner.table.file("metadata/one.json").unwrap(),
        kind: FileKind::Metadata,
        format: ContentFormat::Json,
        length: tree.length,
        digest: tree.digest,
        content: FileContent::Chunks { root: tree.root },
        hint: None,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn json_sealing_streams_large_strings_and_split_utf8_without_changing_canonical_bytes() {
    let store = Arc::new(TestBlocks::default());
    let validator = JsonSealer::new(store.clone(), 2, 1024 * 1024, 64).unwrap();
    for bytes in [
        br#"{"n":1e+2,"a":[null,true,false],"s":"{\"x\"}"}"#.to_vec(),
        "{\"字段\":\"冰😀\"}".as_bytes().to_vec(),
        format!("{{\"large\":\"{}\"}}", "x".repeat(100_000)).into_bytes(),
    ] {
        let size = if bytes.len() < 100 { 7 } else { 2048 };
        let record = record(store.clone(), &bytes, size).await;
        assert_eq!(validator.validate(record.clone()).await.unwrap(), record);
        assert_eq!(validator.active(), 0);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn json_sealing_rejects_syntax_utf8_nesting_and_size_violations() {
    let store = Arc::new(TestBlocks::default());
    let validator = JsonSealer::new(store.clone(), 1, 1000, 4).unwrap();
    for bytes in [
        b"[]".to_vec(),
        b"null".to_vec(),
        b"{}{}".to_vec(),
        b"{\"n\":1e}".to_vec(),
        b"{\"bad\":\"\xff\"}".to_vec(),
        b"{\"bad\":\"\xf0\x9f\"}".to_vec(),
        b"{\"deep\":[[[[]]]]}".to_vec(),
        b"{\"missing\":true,".to_vec(),
    ] {
        let record = record(store.clone(), &bytes, 7).await;
        assert!(validator.validate(record).await.is_err());
        assert_eq!(validator.active(), 0);
    }
    let record = record(store.clone(), &vec![b' '; 1001], 100).await;
    assert!(matches!(
        validator.validate(record).await,
        Err(JsonSealError::Bounds)
    ));
    assert!(JsonSealer::new(store.clone(), 0, 1000, 4).is_err());
    assert!(JsonSealer::new(store, 1, 1000, 129).is_err());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancelled_json_sealing_keeps_worker_admission_until_bounded_reader_exits() {
    let store = Arc::new(TestBlocks::default());
    let record = record(store.clone(), b"{\"value\":123}", 4).await;
    let validator = Arc::new(JsonSealer::new(store.clone(), 1, 1000, 8).unwrap());
    store.pause_reads.store(true, Ordering::SeqCst);
    let worker = {
        let validator = validator.clone();
        let record = record.clone();
        tokio::spawn(async move { validator.validate(record).await })
    };
    tokio::time::timeout(Duration::from_secs(1), store.read_entered.notified())
        .await
        .unwrap();
    assert_eq!(validator.active(), 1);
    assert!(matches!(
        validator.validate(record.clone()).await,
        Err(JsonSealError::Busy)
    ));
    worker.abort();
    assert!(worker.await.unwrap_err().is_cancelled());
    assert_eq!(validator.active(), 1);
    store.pause_reads.store(false, Ordering::SeqCst);
    store.read_release.notify_one();
    tokio::time::timeout(Duration::from_secs(1), async {
        while validator.active() != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(validator.validate(record.clone()).await.unwrap(), record);
}
