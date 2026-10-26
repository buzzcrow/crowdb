// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crate::{publication, split_store};
use crowdb_access_dataset::{split::*, *};
use std::{
    collections::BTreeMap,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc, Mutex,
    },
};

pub struct TestWindowStore {
    pub inner: Arc<dyn DatasetStore>,
    pub pages: Mutex<BTreeMap<Vec<u8>, usize>>,
    pub caps: Mutex<Vec<usize>>,
    pub cancel_page: AtomicBool,
    pub stall_page: AtomicBool,
    pub writes: AtomicUsize,
}

impl TestWindowStore {
    pub fn reset(&self) {
        self.pages.lock().unwrap().clear();
        self.caps.lock().unwrap().clear();
        self.writes.store(0, Ordering::Relaxed);
    }
    pub fn page_reads(&self) -> usize {
        self.pages.lock().unwrap().values().sum()
    }
}

#[async_trait::async_trait]
impl DatasetStore for TestWindowStore {
    async fn get(&self, key: &[u8]) -> Result<Option<StoredValue>, StoreError> {
        self.inner.get(key).await
    }
    async fn get_bounded(
        &self,
        key: &[u8],
        budget: MetadataScanBudget,
        cancel: &ReadCancellation,
    ) -> Result<Option<StoredValue>, StoreError> {
        if key.starts_with(b"DS\0SM\0") {
            *self.pages.lock().unwrap().entry(key.to_vec()).or_default() += 1;
            self.caps.lock().unwrap().push(budget.bytes);
            if self.cancel_page.swap(false, Ordering::Relaxed) {
                cancel.cancel();
            }
            if self.stall_page.load(Ordering::Relaxed) {
                std::future::pending::<()>().await;
            }
        }
        self.inner.get_bounded(key, budget, cancel).await
    }
    async fn scan_page(
        &self,
        request: &MetadataScan,
        cancel: &ReadCancellation,
    ) -> Result<MetadataPage, StoreError> {
        self.inner.scan_page(request, cancel).await
    }
    async fn compare_exchange(
        &self,
        key: &[u8],
        expected: Option<&[u8]>,
        value: &[u8],
    ) -> Result<CasOutcome, StoreError> {
        self.writes.fetch_add(1, Ordering::Relaxed);
        self.inner.compare_exchange(key, expected, value).await
    }
    async fn delete(&self, key: &[u8]) -> Result<(), StoreError> {
        self.writes.fetch_add(1, Ordering::Relaxed);
        self.inner.delete(key).await
    }
}

pub struct TestWindowFixture {
    pub store: Arc<TestWindowStore>,
    pub authority: Arc<DatasetAuthority>,
    pub identity: DatasetIdentity,
    pub snapshot: SnapshotId,
    pub record: SplitRevisionRecord,
}

impl TestWindowFixture {
    pub async fn new(ids: Vec<Vec<u8>>, split: Selection) -> Self {
        let (inner, authority, identity, _) = split_store::fixture().await;
        let mut manifest: ManifestRecord =
            bincode::deserialize(&publication::publication_manifest(1)).unwrap();
        let fields = manifest.samples[0].fields.clone();
        manifest.samples = ids
            .into_iter()
            .map(|sample_id| SampleRecord {
                sample_id,
                fields: fields.clone(),
            })
            .collect();
        let snapshot = authority
            .prepare_snapshot_candidate(
                &identity,
                None,
                bincode::serialize(&manifest).unwrap(),
                OperationId::random(),
            )
            .await
            .unwrap();
        let record = authority
            .prepare_split_revision(
                &identity,
                snapshot,
                &SplitPreparation {
                    revision: SplitRevisionId::random(),
                    policy: SplitPolicy::default(),
                    splits: vec![DraftSplit {
                        name: SplitName::new("window").unwrap(),
                        definition: SplitDefinition::Selection(split),
                        origin: MembershipOrigin::Evaluated,
                    }],
                },
                &ReadCancellation::new(),
            )
            .await
            .unwrap();
        let store = Arc::new(TestWindowStore {
            inner,
            pages: Mutex::default(),
            caps: Mutex::default(),
            cancel_page: AtomicBool::new(false),
            stall_page: AtomicBool::new(false),
            writes: AtomicUsize::new(0),
        });
        Self {
            authority: Arc::new(DatasetAuthority::new(store.clone())),
            store,
            identity,
            snapshot,
            record,
        }
    }
    pub fn spec(&self, selection: Selection) -> ReadSpec {
        ReadSpec {
            snapshot: self.snapshot.to_string(),
            selection,
            fields: vec!["marker".into()],
            batch_size: 20,
            partition: ReadPartition {
                worker: 0,
                workers: 1,
                rank: 0,
                world_size: 1,
            },
            shuffle: None,
            split: Some(ReadSplit {
                revision: Some(self.record.revision),
                name: self.record.splits[0].name.clone(),
            }),
        }
    }
    pub fn page_key(&self, index: u32) -> Vec<u8> {
        key::membership_page(
            &self.identity,
            self.snapshot,
            self.record.revision,
            &self.record.splits[0].name,
            index,
        )
        .unwrap()
    }
    pub async fn replace(&self, key: &[u8], bytes: &[u8]) {
        let old = self.store.inner.get(key).await.unwrap();
        assert!(matches!(
            self.store
                .inner
                .compare_exchange(key, old.as_ref().map(|value| value.bytes.as_slice()), bytes)
                .await
                .unwrap(),
            CasOutcome::Applied(_)
        ));
    }
    pub async fn bind(&self, spec: ReadSpec) -> (DatasetReads, ReadBinding) {
        let reads = DatasetReads::new(self.authority.clone());
        let binding = reads.bind(&self.identity, spec).await.unwrap();
        self.store.reset();
        (reads, binding)
    }
    pub async fn repage(&mut self, items: usize) {
        let mut members = Vec::new();
        for binding in &self.record.splits[0].pages {
            let value = self
                .store
                .inner
                .get(&self.page_key(binding.page))
                .await
                .unwrap()
                .unwrap();
            members.extend(MembershipPage::decode(&value.bytes).unwrap().members);
        }
        let mut bindings = Vec::new();
        for (index, members) in members.chunks(items).enumerate() {
            let page = MembershipPage {
                version: 1,
                snapshot: self.snapshot,
                revision: self.record.revision,
                name: self.record.splits[0].name.clone(),
                page: u32::try_from(index).unwrap(),
                members: members.to_vec(),
            };
            self.replace(&self.page_key(page.page), &page.encode().unwrap())
                .await;
            bindings.push(page.binding().unwrap());
        }
        self.record.splits[0].pages = bindings;
        let key = key::revision(&self.identity, self.snapshot, self.record.revision).unwrap();
        self.replace(&key, &self.record.encode().unwrap()).await;
    }
}

pub fn ids(count: u8) -> Vec<Vec<u8>> {
    (0..count).map(|id| vec![id, 0, 255]).collect()
}
pub fn all() -> Selection {
    Selection::Not(Box::new(Selection::Explicit(vec![])))
}
pub fn range(start: u8, end: u8) -> Selection {
    Selection::Range {
        start: vec![start],
        end: vec![end],
    }
}
