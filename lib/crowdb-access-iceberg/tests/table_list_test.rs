#[path = "common/file_blocks.rs"]
mod blocks;
#[path = "common/store.rs"]
mod common;
#[path = "common/table_read.rs"]
#[allow(dead_code)]
mod fixture;
#[path = "common/table_metadata.rs"]
#[allow(dead_code)]
mod metadata;
#[path = "common/namespace_store.rs"]
mod namespace_store;
#[path = "common/namespace.rs"]
mod namespaces;

use crowdb_access_iceberg::{
    record::StorageRecord,
    table::{name_key, TableListLimits, TableLister},
};
use fixture::TestTable;

fn limits() -> TableListLimits {
    TableListLimits {
        page_size: 1,
        scanned: 100,
        names_bytes: 1024,
    }
}

#[tokio::test]
async fn listing_distinguishes_absent_and_empty_tokens_and_counts_stale_work() {
    let fixture = TestTable::new().await;
    let mut stale = fixture.mapping.clone();
    stale.name = "a".into();
    fixture
        .fixture
        .put(
            name_key(stale.catalog, stale.namespace, &stale.name).unwrap(),
            StorageRecord::TableMapping(stale),
        )
        .await;
    let lister = TableLister::new(fixture.fixture.store.clone(), &[7; 32]).unwrap();
    let context = fixture.fixture.context;
    let namespace = &fixture.parent.identifier;
    let first = lister
        .list(context, namespace, limits(), Some(""))
        .await
        .unwrap()
        .unwrap();
    assert!(first.names.is_empty());
    assert_eq!(first.scanned, 1);
    let token = first.next_page_token.unwrap();
    let second = lister
        .list(context, namespace, limits(), Some(&token))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(second.names, std::slice::from_ref(&fixture.head.name));
    assert!(second.next_page_token.is_none());
    let all = lister
        .list(context, namespace, limits(), None)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(all.scanned, 2);
    assert_eq!(all.names, second.names);
    assert!(lister
        .list(
            context,
            namespace,
            TableListLimits {
                scanned: 1,
                ..limits()
            },
            None
        )
        .await
        .is_err());
    assert!(lister
        .list(
            context,
            namespace,
            TableListLimits {
                names_bytes: 1,
                ..limits()
            },
            None
        )
        .await
        .is_err());
    assert!(lister
        .list(
            context,
            namespace,
            TableListLimits {
                page_size: 2,
                ..limits()
            },
            Some(&token)
        )
        .await
        .is_err());
    let replacement = fixture.fixture.authority(None, &["analytics"]);
    fixture.fixture.publish(&replacement).await;
    assert!(lister
        .list(context, namespace, limits(), Some(&token))
        .await
        .is_err());
}
