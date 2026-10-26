// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

use async_trait::async_trait;
use crowdb_access_iceberg::{
    catalog::{CasOutcome, CatalogStore, StoreError, StoredValue},
    key::{CatalogId, IcebergKey, SystemScope},
    namespace::{ChildScan, NamespaceMappingState, NamespaceProperties, NamespaceStore},
    operation::mutation_identity,
    record::StorageRecord,
};
use crowdb_chunk_kv_client::MultiScanPage;
use crowdb_protocol::chunk_kv::ClientRequestId;

pub enum TestInterleave {
    ClaimProperties,
    ReleaseProperties,
    ClaimEpoch,
    Clear,
}

pub struct TestConflictStore {
    pub inner: Arc<crate::common::TestStore>,
    pub interleave: TestInterleave,
    pub armed: AtomicBool,
}

impl TestConflictStore {
    async fn interleave(&self, key: &IcebergKey, expected: Option<&[u8]>, value: &[u8]) {
        let record = StorageRecord::decode(key, value).unwrap();
        let selected = match (&self.interleave, &record) {
            (
                TestInterleave::ClaimProperties | TestInterleave::ClaimEpoch,
                StorageRecord::NamespaceAuthority(authority),
            ) => expected.is_some() && authority.creating_child.is_some(),
            (TestInterleave::ReleaseProperties, StorageRecord::NamespaceAuthority(authority)) => {
                expected.is_some() && authority.creating_child.is_none()
            }
            (TestInterleave::Clear, StorageRecord::NamespaceMapping(mapping)) => {
                mapping.state == NamespaceMappingState::Published
            }
            _ => false,
        };
        if !selected || !self.armed.swap(false, Ordering::SeqCst) {
            return;
        }
        let key = if matches!(self.interleave, TestInterleave::Clear) {
            IcebergKey::System {
                scope: SystemScope::ActiveRoot,
                suffix: Vec::new(),
            }
        } else {
            key.clone()
        };
        let encoded = key.encode().unwrap();
        let before = self.inner.values.load()[&encoded].bytes.clone();
        let record = match StorageRecord::decode(&key, &before).unwrap() {
            StorageRecord::NamespaceAuthority(mut authority) => {
                authority.mutation_revision += 1;
                if matches!(self.interleave, TestInterleave::ClaimEpoch) {
                    authority.name_epoch += 1;
                } else {
                    authority.creating_child = None;
                    authority.property_revision += 1;
                    authority.properties =
                        NamespaceProperties::new([("rival".into(), "kept".into())].into()).unwrap();
                }
                StorageRecord::NamespaceAuthority(authority)
            }
            StorageRecord::Active(mut root) => {
                root.context = root.context.replacement(CatalogId::random()).unwrap();
                StorageRecord::Active(root)
            }
            other => panic!("unexpected interleaving target: {other:?}"),
        };
        let after = record.encode().unwrap();
        assert!(matches!(
            self.inner
                .compare_exchange(
                    &encoded,
                    Some(&before),
                    &after,
                    mutation_identity(&encoded, Some(&before), &after)
                )
                .await
                .unwrap(),
            CasOutcome::Applied(_)
        ));
    }
}

#[async_trait]
impl CatalogStore for TestConflictStore {
    async fn get(&self, key: &[u8]) -> Result<Option<StoredValue>, StoreError> {
        self.inner.get(key).await
    }
    async fn compare_exchange(
        &self,
        key: &[u8],
        expected: Option<&[u8]>,
        value: &[u8],
        identity: ClientRequestId,
    ) -> Result<CasOutcome, StoreError> {
        self.interleave(&IcebergKey::decode(key)?, expected, value).await;
        self.inner.compare_exchange(key, expected, value, identity).await
    }
}

#[async_trait]
impl NamespaceStore for TestConflictStore {
    async fn scan_children(&self, scan: ChildScan) -> Result<MultiScanPage, StoreError> {
        self.inner.scan_children(scan).await
    }
    async fn delete_mapping(
        &self,
        key: &[u8],
        expected: &[u8],
        identity: ClientRequestId,
    ) -> Result<CasOutcome, StoreError> {
        self.inner.delete_mapping(key, expected, identity).await
    }
}
