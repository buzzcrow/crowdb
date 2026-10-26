// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{reads, request, setup};
use crowdb_access_iceberg::{
    catalog::CatalogStore,
    file::{canonical_file_key, ContentFormat, FileKind},
    key::FileId,
    operation::mutation_identity,
    record::StorageRecord,
    table::{storage_head_key, TableLifecycles, TableRepository},
};
use std::sync::{atomic::Ordering, Arc};

#[tokio::test]
async fn invalid_selected_metadata_still_prevents_every_drop_or_purge_mutation() {
    for native in [false, true] {
        for case in 0..4 {
            let test = setup(1, native).await;
            let selected = TableRepository::new(test.fixture.store.clone())
                .select(test.fixture.context, test.parent.namespace, "events")
                .await
                .unwrap()
                .unwrap();
            let file_key = canonical_file_key(&selected.head.metadata_location);
            let key = file_key.encode().unwrap();
            if case == 0 {
                let mut next = (**test.fixture.store.values.load()).clone();
                next.remove(&key);
                test.fixture.store.values.store(Arc::new(next));
            } else {
                let mut file = selected.metadata;
                match case {
                    1 => file.file = FileId::random(),
                    2 => file.digest[0] ^= 1,
                    _ => {
                        file.kind = FileKind::ManifestList;
                        file.format = ContentFormat::Avro;
                    }
                }
                let after = if case == 2 {
                    // Keep the file valid while making its head's selected digest wrong.
                    let mut head = selected.head.clone();
                    head.metadata_digest[0] ^= 1;
                    (
                        storage_head_key(head.storage_location()),
                        StorageRecord::TableHead(Box::new(head)),
                    )
                } else {
                    (file_key, StorageRecord::File(Box::new(file)))
                };
                let key = after.0.encode().unwrap();
                let before = test.fixture.store.values.load()[&key].bytes.clone();
                let bytes = after.1.encode().unwrap();
                test.fixture
                    .store
                    .compare_exchange(
                        &key,
                        Some(&before),
                        &bytes,
                        mutation_identity(&key, Some(&before), &bytes),
                    )
                    .await
                    .unwrap();
            }
            let store = Arc::new(reads::TestDropReads::new(test.fixture.store.clone()));
            let writes = test.fixture.store.writes.load(Ordering::SeqCst);
            let attempts = test.fixture.store.cas_attempts.load(Ordering::SeqCst);
            assert!(
                TableLifecycles::new(store.clone())
                    .execute(&request(&test, true))
                    .await
                    .is_err(),
                "native={native} case={case}"
            );
            assert_eq!(store.counts(), [1, 1, 1, 1, 1, 1]);
            assert_eq!(test.fixture.store.writes.load(Ordering::SeqCst), writes);
            assert_eq!(test.fixture.store.cas_attempts.load(Ordering::SeqCst), attempts);
            assert_eq!(store.deletes.load(Ordering::SeqCst), 0);
        }
    }
}
