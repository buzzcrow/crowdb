#[path = "common/store.rs"]
mod common;

use std::sync::{atomic::Ordering, Arc};

use common::TestStore;
use crowdb_access_iceberg::catalog::{CatalogStore, StoredValue};
use crowdb_access_iceberg::key::{CatalogId, CatalogScope, IcebergKey, OperationId};
use crowdb_access_iceberg::operation::{PayloadStore, MAX_PAYLOAD_BYTES, PAYLOAD_PAGE_BYTES};
use crowdb_access_iceberg::record::{StorageRecord, MAX_RECORD_BYTES};

#[tokio::test]
async fn payload_boundaries_round_trip_without_large_storage_records() {
    let store = Arc::new(TestStore::default());
    let payloads = PayloadStore::new(store.clone());
    for length in [
        0,
        1,
        PAYLOAD_PAGE_BYTES - 1,
        PAYLOAD_PAGE_BYTES,
        PAYLOAD_PAGE_BYTES + 1,
        MAX_PAYLOAD_BYTES,
    ] {
        let bytes = vec![17; length];
        let reference = payloads
            .put(CatalogId::random(), OperationId::random(), &bytes)
            .await
            .unwrap();
        assert_eq!(payloads.get(&reference).await.unwrap(), bytes);
        assert!(reference.page_count() <= 64);
        for index in 0..reference.page_count() {
            let key = reference
                .page_key(u16::try_from(index).unwrap())
                .unwrap()
                .encode()
                .unwrap();
            assert!(store.get(&key).await.unwrap().unwrap().bytes.len() < MAX_RECORD_BYTES);
        }
        assert!(reference
            .page_key(u16::try_from(reference.page_count()).unwrap())
            .is_err());
    }
    let writes = store.writes.load(Ordering::SeqCst);
    assert!(payloads
        .put(
            CatalogId::random(),
            OperationId::random(),
            &vec![0; MAX_PAYLOAD_BYTES + 1]
        )
        .await
        .is_err());
    assert_eq!(writes, store.writes.load(Ordering::SeqCst));
}

#[tokio::test]
async fn lost_page_replies_resume_immutable_payload_on_another_instance() {
    let bytes = vec![21; PAYLOAD_PAGE_BYTES * 2 + 1];
    for lost_page in 1..=3 {
        let store = Arc::new(TestStore::default());
        let catalog = CatalogId::random();
        let operation = OperationId::random();
        store.fail_after.store(lost_page, Ordering::SeqCst);
        assert!(PayloadStore::new(store.clone())
            .put(catalog, operation, &bytes)
            .await
            .is_err());
        let recovered = PayloadStore::new(store.clone());
        let reference = recovered.put(catalog, operation, &bytes).await.unwrap();
        assert_eq!(recovered.get(&reference).await.unwrap(), bytes);
        assert_eq!(store.writes.load(Ordering::SeqCst), 3);
    }
}

#[tokio::test]
async fn corrupt_missing_and_cross_domain_payloads_fail_closed() {
    let store = Arc::new(TestStore::default());
    let payloads = PayloadStore::new(store.clone());
    let reference = payloads
        .put(CatalogId::random(), OperationId::random(), &[9; 100])
        .await
        .unwrap();
    for changed in [
        crowdb_access_iceberg::operation::PayloadReference {
            catalog: CatalogId::random(),
            ..reference.clone()
        },
        crowdb_access_iceberg::operation::PayloadReference {
            operation: OperationId::random(),
            ..reference.clone()
        },
        crowdb_access_iceberg::operation::PayloadReference {
            length: 101,
            ..reference.clone()
        },
    ] {
        assert!(payloads.get(&changed).await.is_err());
    }
    let key = reference.page_key(0).unwrap();
    let encoded_key = key.encode().unwrap();
    let value = store.get(&encoded_key).await.unwrap().unwrap();
    let StorageRecord::PayloadPage(mut page) = StorageRecord::decode(&key, &value.bytes).unwrap() else {
        panic!("payload page")
    };
    page.bytes[0] ^= 1;
    let mut values = (*store.values.load_full()).clone();
    values.insert(
        encoded_key.clone(),
        StoredValue {
            bytes: StorageRecord::PayloadPage(page).encode().unwrap(),
            revision: 2,
        },
    );
    store.values.store(Arc::new(values));
    assert!(payloads.get(&reference).await.is_err());
    assert!(payloads
        .put(reference.catalog, reference.operation, &[9; 100])
        .await
        .is_err());
    let mut values = (*store.values.load_full()).clone();
    values.remove(&encoded_key);
    store.values.store(Arc::new(values));
    assert!(payloads.get(&reference).await.is_err());
}

#[test]
fn payload_keys_bind_operation_digest_and_bounded_page_index() {
    let mut suffix = OperationId::random().as_bytes().to_vec();
    suffix.extend_from_slice(&[1; 32]);
    suffix.extend_from_slice(&63_u16.to_be_bytes());
    let mut key = IcebergKey::Catalog {
        catalog: CatalogId::random(),
        scope: CatalogScope::OperationPayload,
        suffix,
    };
    assert_eq!(IcebergKey::decode(&key.encode().unwrap()).unwrap(), key);
    let IcebergKey::Catalog { suffix, .. } = &mut key else {
        unreachable!()
    };
    suffix[49] = 64;
    assert!(key.encode().is_err());
}
