// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crate::access_iceberg::{
    gc::seed::{seed_expired_backlog, seed_head},
    now_ms,
};
use crowdb_access_iceberg::{
    catalog::{CasOutcome, CatalogContext, CatalogStore, RoutedCatalogStore},
    file::{
        canonical_file_key, ContentFormat, DeletedFile, FileContent, FileKind, FileRecord, FileRepository,
        TableLocation,
    },
    gc::{CandidatePhase, GcCandidate, GcPhase, GcRepository, GcStalledReason, GcTask, GcTaskKind},
    key::{CatalogId, CatalogScope, FileId, IcebergKey, NamespaceId, OperationId, TableId},
    operation::mutation_identity,
    record::StorageRecord,
    table::{storage_head_key, TablePurgeTask},
};
use sha2::{Digest, Sha256};
use std::sync::Arc;

const POISON: &[u8] = b"invalid-gc-task-remains-for-the-whole-case";

async fn seed_sparse(store: Arc<RoutedCatalogStore>, context: CatalogContext) -> [DeletedFile; 2] {
    let live = seed_head(store.as_ref(), context).await;
    let location = TableLocation {
        catalog: context.catalog,
        table: live,
        storage_namespace: Some(NamespaceId::from_bytes(live.as_bytes()).unwrap()),
    };
    let repository = FileRepository::new(store.clone());
    let now = now_ms();
    let mut sparse = Vec::new();
    for (id, deleted_ms) in [(1, now), (2, now - GcTask::PURGE_DELAY_MS - 60_000)] {
        let bytes = format!("{{\"sparse\":{id}}}");
        let file = FileRecord {
            file: FileId::from_bytes(&[id; 16]).unwrap(),
            location: location.file(&format!("metadata/sparse-{id}.json")).unwrap(),
            kind: FileKind::Metadata,
            format: ContentFormat::Json,
            length: bytes.len() as u64,
            digest: Sha256::digest(bytes.as_bytes()).into(),
            content: FileContent::select_inline(FileKind::Metadata, bytes.as_bytes()).unwrap(),
            hint: None,
        };
        assert_eq!(repository.publish(context, &file).await.unwrap(), file);
        sparse.push(repository.mark_deleted(context, &file, deleted_ms).await.unwrap());
    }
    sparse.try_into().unwrap()
}

async fn seed_poison(store: &RoutedCatalogStore, context: CatalogContext) -> Vec<u8> {
    let poison_catalog = CatalogId::from_bytes(&[1; 16]).unwrap();
    assert_ne!(poison_catalog, context.catalog);
    let poison = IcebergKey::Catalog {
        catalog: poison_catalog,
        scope: CatalogScope::GcTask,
        suffix: OperationId::from_bytes(&[1; 16]).unwrap().as_bytes().to_vec(),
    }
    .encode()
    .unwrap();
    assert!(matches!(
        store
            .compare_exchange(&poison, None, POISON, mutation_identity(&poison, None, POISON))
            .await
            .unwrap(),
        CasOutcome::Applied(_)
    ));
    poison
}

pub(super) struct TestMatrixRecords {
    pub store: Arc<RoutedCatalogStore>,
    pub context: CatalogContext,
    pub table: TableId,
    pub files: Vec<FileRecord>,
    pub marker: TablePurgeTask,
    sparse: [DeletedFile; 2],
    poison: Vec<u8>,
    marker_bytes: Vec<u8>,
    sparse_deadlines: [Option<(u64, u64)>; 2],
}
impl TestMatrixRecords {
    pub(super) async fn seed(
        store: Arc<RoutedCatalogStore>,
        context: CatalogContext,
        table: TableId,
        large: FileRecord,
    ) -> Self {
        let mut backlog = seed_expired_backlog(store.as_ref(), context, table, 8).await;
        backlog.files.push(large);
        let head_key = storage_head_key(backlog.files[0].location.table());
        let head = store.get(&head_key.encode().unwrap()).await.unwrap().unwrap();
        let StorageRecord::TableHead(head) = StorageRecord::decode(&head_key, &head.bytes).unwrap() else {
            panic!("tombstone head missing")
        };
        let marker = TablePurgeTask {
            activation_epoch: context.activation_epoch,
            head: *head,
            dropped_ms: backlog.dropped_ms,
        };
        let marker_bytes = store
            .get(&marker.key().encode().unwrap())
            .await
            .unwrap()
            .unwrap()
            .bytes;
        let sparse = seed_sparse(store.clone(), context).await;
        let poison = seed_poison(store.as_ref(), context).await;
        println!(
            "gc_matrix_seed {}",
            serde_json::json!({
                "catalog_id":context.catalog.to_string(),"table_id":table.to_string(),
                "dropped_ms":marker.dropped_ms,"not_before_ms":marker.dropped_ms+GcTask::PURGE_DELAY_MS,
                "marker_key":marker.key().encode().unwrap(),"marker_bytes_sha256":format!("{:x}",Sha256::digest(&marker_bytes)),
                "files":backlog.files.iter().map(|file| serde_json::json!({"file_id":file.file.to_string(),"logical_bytes":file.length})).collect::<Vec<_>>(),
                "sparse":sparse.iter().map(|deleted| serde_json::json!({"file_id":deleted.file.file.to_string(),"deleted_ms":deleted.deleted_ms,"minimum_not_before_ms":deleted.deleted_ms+GcTask::PURGE_DELAY_MS})).collect::<Vec<_>>(),
                "poison_catalog":"01010101010101010101010101010101","poison_key":poison,"poison_bytes":POISON,
            })
        );
        Self {
            store,
            context,
            table,
            files: backlog.files,
            marker,
            sparse,
            poison,
            marker_bytes,
            sparse_deadlines: [None; 2],
        }
    }
    async fn raw(&self, key: Vec<u8>) -> Result<Option<Vec<u8>>, String> {
        self.store
            .get(&key)
            .await
            .map(|value| value.map(|value| value.bytes))
            .map_err(|error| error.to_string())
    }
    pub(super) async fn task(&self) -> Result<Option<GcTask>, String> {
        GcRepository::new(self.store.clone())
            .task(
                self.context.catalog,
                OperationId::from_bytes(self.table.as_bytes()).unwrap(),
            )
            .await
            .map_err(|error| error.to_string())
    }
    pub(super) async fn multipart(&self) -> Result<Option<GcTask>, String> {
        GcRepository::new(self.store.clone())
            .task(
                self.context.catalog,
                OperationId::from_bytes(b"multipart-gc-v01").unwrap(),
            )
            .await
            .map_err(|error| error.to_string())
    }
    pub(super) fn validate(&self, task: &GcTask) -> Result<(), String> {
        if task.context != self.context
            || task.kind != GcTaskKind::PurgeTable
            || task.head.as_ref() != Some(&self.marker.head)
            || task.created_ms != self.marker.dropped_ms
            || task.not_before_ms != self.marker.dropped_ms + GcTask::PURGE_DELAY_MS
            || task.paused
            || task.phase == GcPhase::Quarantined
        {
            return Err(format!(
                "purge task changed fixed identity/deadline or stopped: {task:?}"
            ));
        }
        Ok(())
    }
    pub(super) async fn remaining(&self) -> Result<usize, String> {
        let mut left = 0;
        for file in &self.files {
            left += usize::from(
                self.raw(canonical_file_key(&file.location).encode().unwrap())
                    .await?
                    .is_some(),
            );
        }
        Ok(left)
    }
    pub(super) async fn candidate(
        &self,
        file: &FileRecord,
        generation: u64,
    ) -> Result<Option<GcCandidate>, String> {
        let mut suffix = file.location.table().table.as_bytes().to_vec();
        suffix.extend_from_slice(&generation.to_be_bytes());
        suffix.extend_from_slice(file.file.as_bytes());
        let key = IcebergKey::Catalog {
            catalog: self.context.catalog,
            scope: CatalogScope::GcCandidate,
            suffix,
        };
        let Some(value) = self.raw(key.encode().unwrap()).await? else {
            return Ok(None);
        };
        let StorageRecord::GcCandidate(candidate) =
            StorageRecord::decode(&key, &value).map_err(|error| error.to_string())?
        else {
            return Err("invalid GC candidate record".into());
        };
        Ok(Some(*candidate))
    }
    pub(super) async fn observe_sparse(&mut self) -> Result<(), String> {
        for index in 0..self.sparse.len() {
            let deleted = &self.sparse[index];
            let Some(candidate) = self.candidate(&deleted.file, 0).await? else {
                continue;
            };
            let times = (candidate.first_seen_ms, candidate.not_before_ms);
            let original_deadline = deleted.deleted_ms + GcTask::PURGE_DELAY_MS;
            if candidate.file != deleted.file
                || candidate.first_seen_ms > candidate.not_before_ms
                || candidate.not_before_ms < original_deadline
                || candidate.first_seen_ms > now_ms()
                || (index == 0 && candidate.not_before_ms != original_deadline)
            {
                return Err(format!(
                    "invalid discovered sparse candidate binding/times: {candidate:?}"
                ));
            }
            if let Some(original) = self.sparse_deadlines[index] {
                if original != times {
                    return Err(format!(
                        "sparse candidate deadline changed after observation: {original:?} -> {times:?}"
                    ));
                }
            } else {
                println!(
                    "gc_matrix_sparse_first_observation {}",
                    serde_json::json!({"file_id":deleted.file.file.to_string(),"deleted_ms":deleted.deleted_ms,"first_seen_ms":candidate.first_seen_ms,"not_before_ms":candidate.not_before_ms,"phase":format!("{:?}",candidate.phase)})
                );
                self.sparse_deadlines[index] = Some(times);
            }
        }
        Ok(())
    }
    pub(super) async fn sparse_complete(&self) -> Result<bool, String> {
        Ok(self
            .candidate(&self.sparse[1].file, 0)
            .await?
            .is_some_and(|candidate| candidate.phase == CandidatePhase::Complete))
    }
    pub(super) async fn verify(&self, task: &GcTask) -> Result<serde_json::Value, String> {
        self.validate(task)?;
        if task.phase != GcPhase::Complete
            || task.deleted != self.files.len() as u64
            || task.reclaimed_bytes != self.files.iter().map(|file| file.length).sum::<u64>()
            || task.stalled != GcStalledReason::None
            || task.retry_at_ms != 0
            || task.deferred_ranges
            || task.fenced
        {
            return Err(format!(
                "purge backlog has not discharged its exact obligations: {task:?}"
            ));
        }
        let mut candidates = Vec::new();
        for file in &self.files {
            if self
                .raw(canonical_file_key(&file.location).encode().unwrap())
                .await?
                .is_some()
            {
                return Err(format!("dead canonical remains: {}", file.file));
            }
            let candidate = self
                .candidate(file, self.marker.head.generation)
                .await?
                .ok_or("purge candidate missing")?;
            if candidate.file != *file
                || candidate.task != task.identity
                || candidate.generation != self.marker.head.generation
                || candidate.phase != CandidatePhase::Complete
                || candidate.completed_round == 0
                || candidate.completed_round > task.sweep_round
                || candidate.cursor.pending.is_some()
                || !candidate.cursor.frames.is_empty()
            {
                return Err(format!("dead candidate is not exactly Complete: {candidate:?}"));
            }
            candidates.push(serde_json::json!({"file_id":file.file.to_string(),"logical_bytes":file.length,"generation":candidate.generation,"first_seen_ms":candidate.first_seen_ms,"not_before_ms":candidate.not_before_ms,"phase":format!("{:?}",candidate.phase),"next_location":candidate.cursor.next_location,"completed_round":candidate.completed_round}));
        }
        let sparse = self.verify_sparse().await?;
        if self.raw(self.poison.clone()).await? != Some(POISON.to_vec()) {
            return Err("invalid catalog poison was changed or deleted".into());
        }
        if self.raw(self.marker.key().encode().unwrap()).await? != Some(self.marker_bytes.clone()) {
            return Err("original purge marker changed across restart".into());
        }
        Ok(
            serde_json::json!({"dead_canonical_absent":self.files.len(),"exact_candidates_complete":self.files.len(),"candidates":candidates,"marker_key":self.marker.key().encode().unwrap(),"marker_bytes_sha256":format!("{:x}",Sha256::digest(&self.marker_bytes)),"marker_identity_preserved":true,"poison_catalog":"01010101010101010101010101010101","poison_unchanged":true,"poison_key":self.poison,"poison_bytes":POISON,"sparse":sparse}),
        )
    }
    async fn verify_sparse(&self) -> Result<serde_json::Value, String> {
        let mut report = Vec::new();
        for (index, deleted) in self.sparse.iter().enumerate() {
            let key = canonical_file_key(&deleted.file.location);
            let value = self.raw(key.encode().unwrap()).await?;
            let candidate = self
                .candidate(&deleted.file, 0)
                .await?
                .ok_or("sparse candidate was never discovered")?;
            if candidate.file != deleted.file
                || self.sparse_deadlines[index] != Some((candidate.first_seen_ms, candidate.not_before_ms))
                || candidate.first_seen_ms > candidate.not_before_ms
                || candidate.not_before_ms < deleted.deleted_ms + GcTask::PURGE_DELAY_MS
                || candidate.generation != 0
                || candidate.task != OperationId::from_bytes(b"multipart-gc-v01").unwrap()
            {
                return Err(format!("sparse fixed deadline or binding changed: {candidate:?}"));
            }
            if index == 0 {
                let value = value.ok_or("future canonical was removed too early")?;
                if StorageRecord::decode_storage(&key, &value).map_err(|error| error.to_string())?
                    != StorageRecord::DeletedFile(Box::new(deleted.clone()))
                    || candidate.phase != CandidatePhase::Retained
                    || candidate.not_before_ms != deleted.deleted_ms + GcTask::PURGE_DELAY_MS
                    || now_ms() >= candidate.not_before_ms
                {
                    return Err(format!("future candidate was prematurely retired: {candidate:?}"));
                }
            } else if value.is_some()
                || candidate.phase != CandidatePhase::Complete
                || candidate.completed_round == 0
                || candidate.not_before_ms > now_ms()
            {
                return Err(format!("higher expired candidate did not finish: {candidate:?}"));
            }
            report.push(serde_json::json!({"file_id":deleted.file.file.to_string(),"deleted_ms":deleted.deleted_ms,"first_seen_ms":candidate.first_seen_ms,"not_before_ms":candidate.not_before_ms,"phase":format!("{:?}",candidate.phase),"canonical_present":index==0}));
        }
        Ok(serde_json::json!(report))
    }
}
