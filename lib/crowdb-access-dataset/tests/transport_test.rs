use std::sync::Arc;

use async_trait::async_trait;
use crowdb_access_dataset::{
    CasOutcome, DatasetAuthority, DatasetError, DatasetIdentity, DatasetReadRequest, DatasetReadService,
    DatasetStore, NamespacePath, SnapshotId, StoreError, StoredValue,
};

struct EmptyStore;

#[async_trait]
impl DatasetStore for EmptyStore {
    async fn get(&self, _key: &[u8]) -> Result<Option<StoredValue>, StoreError> {
        Ok(None)
    }

    async fn compare_exchange(
        &self,
        _key: &[u8],
        _expected: Option<&[u8]>,
        _value: &[u8],
    ) -> Result<CasOutcome, StoreError> {
        Ok(CasOutcome::Applied(1))
    }
}

fn service() -> DatasetReadService {
    let authority = Arc::new(DatasetAuthority::new(Arc::new(EmptyStore)));
    DatasetReadService::new(
        authority,
        DatasetIdentity::new(NamespacePath::root(), "transport").unwrap(),
        1,
        10,
    )
    .unwrap()
}

#[tokio::test]
async fn transport_rejects_invalid_wire_request_before_admission() {
    let service = service();
    let request = DatasetReadRequest {
        snapshot: SnapshotId::random(),
        sample_ids: Vec::new(),
        fields: Vec::new(),
    };
    assert!(matches!(
        service.read(request, 10).await,
        Err(crowdb_access_dataset::ReadTransportError::Invalid(
            DatasetError::InvalidManifest
        ))
    ));
    assert_eq!(service.in_flight(), 0);
}

#[tokio::test]
async fn transport_cancel_releases_window_and_rejects_future_reads() {
    let service = service();
    service.cancel();
    let request = DatasetReadRequest {
        snapshot: SnapshotId::random(),
        sample_ids: vec![b"sample".to_vec()],
        fields: vec!["value".into()],
    };
    assert!(service.read(request, 10).await.is_err());
    assert_eq!(service.in_flight(), 0);
    assert!(service.is_cancelled());
}
