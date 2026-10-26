// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[path = "common/list_admission.rs"]
mod admission;
#[path = "common/file_blocks.rs"]
mod blocks;
#[path = "common/list_boundaries.rs"]
mod boundaries;
#[path = "common/store.rs"]
mod common;
#[path = "common/namespace.rs"]
mod fixture;
#[path = "common/namespace_store.rs"]
mod namespace_store;

use admission::{TestListFixture, TestListStore};
use crowdb_access_iceberg::{
    namespace::NamespaceLister,
    table::{TableListLimits, TableLister},
};

fn limits(page_size: usize) -> TableListLimits {
    TableListLimits {
        page_size,
        scanned: 100,
        names_bytes: 1024,
    }
}

#[tokio::test]
async fn table_lists_resolve_ancestors_once_across_internal_pages_and_external_continuations() {
    for depth in [1, 3] {
        for native in [false, true] {
            let test = TestListFixture::new(depth, native).await;
            let store = TestListStore::new(test.fixture.store.clone(), Vec::new());
            let lister = TableLister::new(store.clone(), &[7; 32]).unwrap();
            let all = lister
                .list(test.fixture.context, &test.leaf.identifier, limits(1), None)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(all.names, ["a", "b", "c"]);
            assert_eq!(all.scanned, 3);
            assert!(all.next_page_token.is_none());
            assert_eq!(store.counts(), (2 * depth + 7, 2, 3, 0));
            println!(
                "table-list depth={depth} native={native} complete={:?}",
                store.counts()
            );
            let before = store.counts();
            let first = lister
                .list(test.fixture.context, &test.leaf.identifier, limits(1), Some(""))
                .await
                .unwrap()
                .unwrap();
            assert_eq!(first.names, ["a"]);
            assert_eq!(first.scanned, 1);
            assert_eq!(store.counts(), (before.0 + 2 * depth + 5, before.1 + 2, 4, 0));
            let before = store.counts();
            let second = lister
                .list(
                    test.fixture.context,
                    &test.leaf.identifier,
                    limits(1),
                    first.next_page_token.as_deref(),
                )
                .await
                .unwrap()
                .unwrap();
            assert_eq!(second.names, ["b"]);
            assert_eq!(second.scanned, 1);
            assert_eq!(store.counts(), (before.0 + 2 * depth + 5, before.1 + 2, 5, 0));
        }
    }
}

#[tokio::test]
async fn namespace_pages_reuse_ancestor_admission_and_fence_every_continuation_independently() {
    for depth in [1, 3] {
        let test = TestListFixture::new(depth, false).await;
        let store = TestListStore::new(test.fixture.store.clone(), Vec::new());
        let lister = NamespaceLister::new(store.clone(), &[7; 32]).unwrap();
        let all = lister
            .page(test.fixture.context, Some(&test.leaf.identifier), 3, "")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(all.scanned, 3);
        assert_eq!(all.namespaces.len(), 3);
        assert!(all.next_page_token.is_none());
        assert_eq!(store.counts(), (2 * depth + 7, 2, 1, 0));
        println!("namespace-list depth={depth} complete={:?}", store.counts());
        let before = store.counts();
        let first = lister
            .page(test.fixture.context, Some(&test.leaf.identifier), 1, "")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(first.scanned, 1);
        assert_eq!(first.namespaces[0].name(), "a");
        assert_eq!(store.counts(), (before.0 + 2 * depth + 5, before.1 + 2, 2, 0));
        let before = store.counts();
        let second = lister
            .page(
                test.fixture.context,
                Some(&test.leaf.identifier),
                1,
                first.next_page_token.as_deref().unwrap(),
            )
            .await
            .unwrap()
            .unwrap();
        assert_eq!(second.scanned, 1);
        assert_eq!(second.namespaces[0].name(), "b");
        assert_eq!(store.counts(), (before.0 + 2 * depth + 5, before.1 + 2, 3, 0));
    }
}

#[tokio::test]
async fn root_namespace_lists_preserve_their_two_checks_without_creating_a_default_namespace() {
    let test = fixture::TestNamespace::new().await;
    let store = TestListStore::new(test.store.clone(), Vec::new());
    let lister = NamespaceLister::new(store.clone(), &[7; 32]).unwrap();
    assert!(lister
        .page(test.context, None, 100, "")
        .await
        .unwrap()
        .unwrap()
        .namespaces
        .is_empty());
    assert_eq!(store.counts(), (2, 2, 1, 0));
    for name in ["a", "b", "c"] {
        test.publish(&test.authority(None, &[name])).await;
    }
    let page = lister.page(test.context, None, 100, "").await.unwrap().unwrap();
    assert_eq!(page.namespaces.len(), 3);
    assert_eq!(store.counts(), (7, 4, 2, 0));
}
