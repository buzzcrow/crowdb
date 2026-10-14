#[path = "common/store.rs"]
mod common;
#[path = "common/file.rs"]
mod fixture;

use std::sync::atomic::Ordering;

use crowdb_access_iceberg::{
    file::{
        canonical_file_key, FileDigest, FileGrant, FileGrantIssuer, FileIdentity, FileListRequest,
        FileListTokens, FileOperation, FileOperations, FileRepository, TableLocation,
    },
    key::{NamespaceId, OperationId, StorageKey, StorageTablePath, TableStorageScope},
    record::StorageRecord,
};

async fn native_fixture() -> fixture::TestFile {
    let mut fixture = fixture::TestFile::new(common::TestStore::default()).await;
    fixture.table = StorageTablePath {
        catalog: fixture.context.catalog,
        storage_namespace: NamespaceId::random(),
        table: fixture.table.table,
    }
    .into();
    fixture
}

fn grant(fixture: &fixture::TestFile) -> FileGrant {
    FileGrant {
        storage_namespace: fixture.table.storage_namespace,
        context: fixture.context,
        table: fixture.table.table,
        principal: [1; 32],
        nonce: OperationId::random(),
        issued_ms: 100,
        expires_ms: 10_000,
        operations: FileOperations::new(&[FileOperation::Get, FileOperation::ListObjects]).unwrap(),
        max_request_bytes: 1024,
        max_file_bytes: 1024,
    }
}

#[tokio::test]
async fn native_publication_has_one_filename_record_and_conflicts_preserve_bytes() {
    let fixture = native_fixture().await;
    let repository = FileRepository::new(fixture.store.clone());
    let candidate = fixture.record("metadata/v1.json", b"{}");
    assert_eq!(
        repository.publish(fixture.context, &candidate).await.unwrap(),
        candidate
    );
    let equal = fixture.record("metadata/v1.json", b"{}");
    assert_eq!(
        repository.publish(fixture.context, &equal).await.unwrap(),
        candidate
    );
    assert!(repository
        .publish(fixture.context, &fixture.record("metadata/v1.json", b"[]"))
        .await
        .is_err());
    assert_eq!(
        repository
            .load(fixture.context, &candidate.location)
            .await
            .unwrap(),
        Some(candidate.clone())
    );
    let key = canonical_file_key(&candidate.location);
    let snapshot = fixture.store.values.load_full();
    let files: Vec<_> = snapshot.keys().filter(|key| matches!(StorageKey::decode(key), Ok(StorageKey::NativeTable(key)) if key.scope == TableStorageScope::File)).collect();
    assert_eq!(files, [&key.encode().unwrap()]);
    assert_eq!(
        StorageRecord::decode_storage(&key, &snapshot[&key.encode().unwrap()].bytes).unwrap(),
        StorageRecord::File(Box::new(candidate.clone()))
    );
    let mut foreign = candidate.location.table();
    foreign.storage_namespace = Some(NamespaceId::random());
    let foreign = canonical_file_key(&foreign.file(candidate.location.relative_key()).unwrap());
    assert!(StorageRecord::decode_storage(&foreign, &snapshot[&key.encode().unwrap()].bytes).is_err());
}

#[tokio::test]
async fn native_publication_lost_reply_replays_the_same_single_record() {
    let fixture = native_fixture().await;
    let repository = FileRepository::new(fixture.store.clone());
    let candidate = fixture.record("metadata/v1.json", b"{}");
    fixture.store.file_record_reply_loss.store(true, Ordering::SeqCst);
    assert!(repository.publish(fixture.context, &candidate).await.is_err());
    let restarted = FileRepository::new(fixture.store.clone());
    assert_eq!(
        restarted.publish(fixture.context, &candidate).await.unwrap(),
        candidate
    );
    assert_eq!(
        restarted
            .load(fixture.context, &candidate.location)
            .await
            .unwrap(),
        Some(candidate)
    );
}

#[tokio::test]
async fn many_metadata_files_page_by_public_prefix_without_file_resolution_gets() {
    let fixture = native_fixture().await;
    let repository = FileRepository::new(fixture.store.clone());
    for index in 0..600 {
        for prefix in ["metadata", "other"] {
            let record = fixture.record(&format!("{prefix}/v{index:04}.json"), b"{}");
            repository.publish(fixture.context, &record).await.unwrap();
        }
    }
    let tokens = FileListTokens::new([1; 32]).unwrap();
    let grant = grant(&fixture);
    let mut request = FileListRequest {
        table: fixture.table,
        prefix: format!("{}metadata/", fixture.table.object_prefix()),
        delimiter: None,
        encoding_url: false,
        max_keys: 73,
        continuation_token: None,
        start_after: None,
    };
    let mut keys = Vec::new();
    loop {
        let before = fixture.store.reads.load(Ordering::SeqCst);
        let page = repository.list(&grant, &request, &tokens, 200).await.unwrap();
        // The two Ready root checks are independent of file-record resolution.
        assert_eq!(fixture.store.reads.load(Ordering::SeqCst) - before, 2);
        keys.extend(page.files.into_iter().map(|file| file.key));
        request.continuation_token = page.next_continuation_token;
        if request.continuation_token.is_none() {
            break;
        }
    }
    let expected: Vec<_> = (0..600)
        .map(|index| format!("{}metadata/v{index:04}.json", fixture.table.object_prefix()))
        .collect();
    assert_eq!(keys, expected);
    request.prefix = format!("{}metadata/v000", fixture.table.object_prefix());
    let page = repository.list(&grant, &request, &tokens, 200).await.unwrap();
    assert_eq!(page.files.len(), 10);
    let mut foreign_grant = grant.clone();
    foreign_grant.storage_namespace = Some(NamespaceId::random());
    assert!(repository
        .list(&foreign_grant, &request, &tokens, 200)
        .await
        .is_err());
}

#[tokio::test]
async fn native_grants_and_digest_checkpoints_bind_the_storage_namespace() {
    let fixture = native_fixture().await;
    let issuer = FileGrantIssuer::new([2; 32], 10_000).unwrap();
    let grant = grant(&fixture);
    let credentials = issuer.issue(grant.clone()).unwrap();
    let verified = issuer
        .verify(
            credentials.access_key_id(),
            credentials.session_token(),
            fixture.context,
            200,
        )
        .unwrap();
    assert_eq!(verified.grant(), &grant);
    let location = fixture.table.file("metadata/v1.json").unwrap();
    verified
        .grant()
        .authorize(FileOperation::Get, &location, 0, 2)
        .unwrap();
    let mut foreign = fixture.table;
    foreign.storage_namespace = Some(NamespaceId::random());
    assert!(verified
        .grant()
        .authorize(
            FileOperation::Get,
            &foreign.file("metadata/v1.json").unwrap(),
            0,
            2
        )
        .is_err());
    let owner = FileIdentity {
        table: fixture.table,
        file: crowdb_access_iceberg::key::FileId::random(),
    };
    let mut digest = FileDigest::new(owner);
    digest.update(b"hello").unwrap();
    let checkpoint = digest.checkpoint();
    let restored = FileDigest::restore(owner, &checkpoint).unwrap();
    assert_eq!(restored.finish(), digest.finish());
    assert!(FileDigest::restore(
        FileIdentity {
            table: foreign,
            ..owner
        },
        &checkpoint
    )
    .is_err());
    let legacy: TableLocation = TableLocation {
        storage_namespace: None,
        ..fixture.table
    };
    assert!(FileDigest::restore(
        FileIdentity {
            table: legacy,
            ..owner
        },
        &checkpoint
    )
    .is_err());
}
