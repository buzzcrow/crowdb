#[path = "common/store.rs"]
mod common;
#[path = "common/file.rs"]
mod fixture;

use crowdb_access_iceberg::catalog::{CatalogError, CatalogStore, RootState};
use crowdb_access_iceberg::file::{
    file_key, FileGrant, FileListRequest, FileListTokens, FileOperation, FileOperations, FileRepository,
};
use crowdb_access_iceberg::key::{CatalogId, OperationId, TableId};
use crowdb_access_iceberg::operation::mutation_identity;
use crowdb_access_iceberg::record::StorageRecord;
use fixture::TestFile;

fn grant(fixture: &TestFile) -> FileGrant {
    FileGrant {
        context: fixture.context,
        table: fixture.table.table,
        principal: [7; 32],
        nonce: OperationId::random(),
        issued_ms: 100,
        expires_ms: 10_000,
        operations: FileOperations::new(&[FileOperation::ListObjects]).unwrap(),
        max_request_bytes: 1024,
        max_file_bytes: 1024,
    }
}

fn request(fixture: &TestFile, max_keys: u16) -> FileListRequest {
    FileListRequest {
        table: fixture.table,
        prefix: fixture.table.object_prefix(),
        delimiter: None,
        encoding_url: false,
        max_keys,
        continuation_token: None,
        start_after: None,
    }
}

#[tokio::test]
async fn selected_published_files_are_visible_but_candidates_and_deleted_files_are_not() {
    let fixture = TestFile::new(common::TestStore::default()).await;
    let repository = FileRepository::new(fixture.store.clone());
    let selected = fixture.record("metadata/selected.json", b"{}");
    repository.publish(fixture.context, &selected).await.unwrap();
    let losing = fixture.record("metadata/selected.json", b"{\"different\":true}");
    assert!(repository.publish(fixture.context, &losing).await.is_err());
    let candidate = fixture.record("metadata/draft.json", b"{}");
    let key = file_key(fixture.context.catalog, candidate.file)
        .encode()
        .unwrap();
    let bytes = StorageRecord::File(Box::new(candidate)).encode().unwrap();
    fixture
        .store
        .compare_exchange(&key, None, &bytes, mutation_identity(&key, None, &bytes))
        .await
        .unwrap();
    let deleted = fixture.record("metadata/deleted.json", b"{}");
    repository.publish(fixture.context, &deleted).await.unwrap();
    repository
        .mark_deleted(fixture.context, &deleted, 200)
        .await
        .unwrap();
    let tokens = FileListTokens::new([42; 32]).unwrap();
    let page = repository
        .list(&grant(&fixture), &request(&fixture, 1000), &tokens, 300)
        .await
        .unwrap();
    assert_eq!(page.files.len(), 1);
    assert_eq!(page.files[0].key, selected.location.object_key());
    assert_eq!(page.files[0].length, 2);
    assert!(page.next_continuation_token.is_none());
}

#[tokio::test]
async fn two_table_principals_cannot_discover_each_others_published_names() {
    let fixture = TestFile::new(common::TestStore::default()).await;
    let repository = FileRepository::new(fixture.store.clone());
    let selected = fixture.record("data/own.json", b"{}");
    repository.publish(fixture.context, &selected).await.unwrap();
    let other_table = crowdb_access_iceberg::file::TableLocation {
        catalog: fixture.context.catalog,
        table: TableId::random(),
    };
    let mut foreign = fixture.record("data/foreign.json", b"{}");
    foreign.location = other_table.file("data/foreign.json").unwrap();
    repository.publish(fixture.context, &foreign).await.unwrap();
    let tokens = FileListTokens::new([42; 32]).unwrap();
    let own_grant = grant(&fixture);
    let own_request = request(&fixture, 1000);
    let mut other_request = own_request.clone();
    other_request.table = other_table;
    other_request.prefix = other_table.object_prefix();
    let mut other_grant = own_grant.clone();
    other_grant.table = other_table.table;
    other_grant.principal = [9; 32];
    for (grant, request, expected) in [
        (&own_grant, &own_request, selected),
        (&other_grant, &other_request, foreign),
    ] {
        let page = repository.list(grant, request, &tokens, 300).await.unwrap();
        assert_eq!(page.files.len(), 1);
        assert_eq!(page.files[0].key, expected.location.object_key());
    }
    assert!(repository
        .list(&own_grant, &other_request, &tokens, 300)
        .await
        .is_err());
    assert!(repository
        .list(&other_grant, &own_request, &tokens, 300)
        .await
        .is_err());
    other_request.table.catalog = CatalogId::random();
    assert!(repository
        .list(&own_grant, &other_request, &tokens, 300)
        .await
        .is_err());
}

#[tokio::test]
async fn pages_and_delimiters_preserve_order_without_duplicate_groups() {
    let fixture = TestFile::new(common::TestStore::default()).await;
    let repository = FileRepository::new(fixture.store.clone());
    for relative in ["a/one.json", "a/two.json", "b/one.json", "c.json", "雪&<>.json"] {
        repository
            .publish(fixture.context, &fixture.record(relative, b"{}"))
            .await
            .unwrap();
    }
    let tokens = FileListTokens::new([42; 32]).unwrap();
    let grant = grant(&fixture);
    let mut query = request(&fixture, 1);
    let mut keys = Vec::new();
    loop {
        let page = repository.list(&grant, &query, &tokens, 300).await.unwrap();
        assert!(page.files.len() <= 1);
        keys.extend(page.files.into_iter().map(|file| file.key));
        query.continuation_token = page.next_continuation_token;
        if query.continuation_token.is_none() {
            break;
        }
    }
    assert_eq!(keys.len(), 5);
    assert!(keys.windows(2).all(|pair| pair[0] < pair[1]));
    query.delimiter = Some("/".into());
    let mut groups = Vec::new();
    loop {
        let page = repository.list(&grant, &query, &tokens, 300).await.unwrap();
        assert!(page.files.len() + page.common_prefixes.len() <= 1);
        groups.extend(page.common_prefixes);
        query.continuation_token = page.next_continuation_token;
        if query.continuation_token.is_none() {
            break;
        }
    }
    assert_eq!(
        groups,
        vec![
            format!("{}a/", fixture.table.object_prefix()),
            format!("{}b/", fixture.table.object_prefix())
        ]
    );
}

#[tokio::test]
async fn token_scope_tampering_and_expiry_fail_before_file_scans() {
    let fixture = TestFile::new(common::TestStore::default()).await;
    let repository = FileRepository::new(fixture.store.clone());
    for key in ["a.json", "b.json"] {
        repository
            .publish(fixture.context, &fixture.record(key, b"{}"))
            .await
            .unwrap();
    }
    let tokens = FileListTokens::new([42; 32]).unwrap();
    let grant = grant(&fixture);
    let mut query = request(&fixture, 1);
    query.continuation_token = repository
        .list(&grant, &query, &tokens, 300)
        .await
        .unwrap()
        .next_continuation_token;
    assert!(query.continuation_token.is_some());
    for change in 0..7 {
        let scans = fixture.store.file_scans.load(std::sync::atomic::Ordering::SeqCst);
        let mut other_grant = grant.clone();
        let mut other_query = query.clone();
        match change {
            0 => other_grant.principal = [8; 32],
            1 => other_grant.nonce = OperationId::random(),
            2 => other_query.prefix.push('b'),
            3 => other_query.delimiter = Some("/".into()),
            4 => other_query.encoding_url = true,
            5 => other_query.table.table = TableId::random(),
            _ => other_query.table.catalog = CatalogId::random(),
        }
        assert!(repository
            .list(&other_grant, &other_query, &tokens, 300)
            .await
            .is_err());
        assert_eq!(
            fixture.store.file_scans.load(std::sync::atomic::Ordering::SeqCst),
            scans
        );
    }
    let mut altered = query.clone();
    altered.continuation_token = Some(format!("{}A", query.continuation_token.as_ref().unwrap()));
    assert!(repository.list(&grant, &altered, &tokens, 300).await.is_err());
    assert!(repository.list(&grant, &query, &tokens, 10_000).await.is_err());
    assert!(repository
        .list(&grant, &query, &FileListTokens::new([43; 32]).unwrap(), 300)
        .await
        .is_err());
}

#[tokio::test]
async fn live_cursor_skips_late_publication_behind_it_and_observes_deletion_ahead() {
    let fixture = TestFile::new(common::TestStore::default()).await;
    let repository = FileRepository::new(fixture.store.clone());
    for key in ["b.json", "d.json", "f.json"] {
        repository
            .publish(fixture.context, &fixture.record(key, b"{}"))
            .await
            .unwrap();
    }
    let tokens = FileListTokens::new([42; 32]).unwrap();
    let grant = grant(&fixture);
    let mut query = request(&fixture, 1);
    let page = repository.list(&grant, &query, &tokens, 300).await.unwrap();
    assert!(page.files[0].key.ends_with("b.json"));
    query.continuation_token = page.next_continuation_token;
    repository
        .publish(fixture.context, &fixture.record("a.json", b"{}"))
        .await
        .unwrap();
    repository
        .publish(fixture.context, &fixture.record("c.json", b"{}"))
        .await
        .unwrap();
    let deleted = repository
        .load(fixture.context, &fixture.table.file("d.json").unwrap())
        .await
        .unwrap()
        .unwrap();
    repository
        .mark_deleted(fixture.context, &deleted, 400)
        .await
        .unwrap();
    query.max_keys = 1000;
    let page = repository.list(&grant, &query, &tokens, 500).await.unwrap();
    assert_eq!(
        page.files
            .iter()
            .map(|file| file.key.rsplit('/').next().unwrap())
            .collect::<Vec<_>>(),
        ["c.json", "f.json"]
    );
    fixture.root(fixture.context, RootState::Fencing).await;
    assert!(repository
        .list(&grant, &request(&fixture, 1), &tokens, 500)
        .await
        .is_err());
}

#[tokio::test]
async fn storage_pages_remain_bounded_and_zero_limit_does_not_scan_files() {
    let fixture = TestFile::new(common::TestStore::default()).await;
    let repository = FileRepository::new(fixture.store.clone());
    for index in 0..260 {
        repository
            .publish(
                fixture.context,
                &fixture.record(&format!("data/{index:04}.json"), b"{}"),
            )
            .await
            .unwrap();
    }
    let tokens = FileListTokens::new([42; 32]).unwrap();
    let grant = grant(&fixture);
    let mut query = request(&fixture, 1000);
    let first = repository.list(&grant, &query, &tokens, 300).await.unwrap();
    assert_eq!(first.files.len(), 256);
    query.continuation_token = first.next_continuation_token;
    let second = repository.list(&grant, &query, &tokens, 300).await.unwrap();
    assert_eq!(second.files.len(), 4);
    assert!(second.next_continuation_token.is_none());
    let empty = repository
        .list(&grant, &request(&fixture, 0), &tokens, 300)
        .await
        .unwrap();
    assert!(empty.files.is_empty());
    let mut forbidden = grant.clone();
    forbidden.operations = FileOperations::new(&[FileOperation::Get]).unwrap();
    assert!(matches!(
        repository.list(&forbidden, &query, &tokens, 300).await,
        Err(CatalogError::Forbidden)
    ));
}
