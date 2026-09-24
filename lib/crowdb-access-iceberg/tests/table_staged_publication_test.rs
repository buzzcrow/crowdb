#[path = "common/file_blocks.rs"]
mod blocks;
#[path = "common/store.rs"]
mod common;
#[path = "common/manifest_entry.rs"]
#[allow(dead_code)]
mod fixture;
#[path = "common/manifest_list.rs"]
#[allow(dead_code)]
mod list_fixture;
#[path = "common/table_metadata.rs"]
#[allow(dead_code)]
mod metadata;
#[path = "common/namespace_store.rs"]
mod namespace_store;
#[path = "common/namespace.rs"]
#[allow(dead_code)]
mod namespaces;
#[path = "common/parquet_metadata.rs"]
#[allow(dead_code)]
mod parquet;
#[path = "common/table_recovery.rs"]
mod recovery_store;
#[path = "common/snapshot_files.rs"]
#[allow(dead_code)]
mod snapshot;
#[path = "common/table_staging.rs"]
mod staging;

use crowdb_access_iceberg::{
    commit::TableCreatePhase,
    key::{CatalogScope, IcebergKey},
    namespace::NamespaceDropper,
    table::TableRepository,
};
use serde_json::json;
use staging::TestStaged;
use std::sync::atomic::Ordering;

#[tokio::test]
async fn recovery_retains_live_drafts_and_phase_fences_expiry() {
    use crowdb_access_iceberg::commit::{
        CommitPreparationLimits, CommitProofLimits, CommitRequestLimits, PriorManifestLimits, TableRecovery,
        TableRecoveryKind,
    };
    let test = TestStaged::new().await;
    test.stage().await;
    let limits = staging::limits();
    let recovery = TableRecovery::new(
        test.namespace.store.clone(),
        test.blocks.clone(),
        CommitProofLimits {
            preparation: CommitPreparationLimits {
                request: CommitRequestLimits {
                    json: metadata::limits(),
                    requirements: 100,
                    updates: 100,
                },
                evaluation: limits.evaluation,
            },
            prior: PriorManifestLimits {
                snapshots: 10,
                references: 100,
                index_bytes: 1_000_000,
                manifests: limits.snapshots.files.manifests,
            },
            snapshots: limits.snapshots,
            auxiliary: limits.auxiliary,
        },
    );
    let live = recovery
        .recover_page(test.namespace.context, TableRecoveryKind::Create, None, 1999)
        .await
        .unwrap();
    assert_eq!(live.retained, 1);
    assert_eq!(test.operation().await.phase, TableCreatePhase::Staged);
    test.namespace.store.fail_after.store(
        test.namespace.store.writes.load(Ordering::SeqCst) + 1,
        Ordering::SeqCst,
    );
    let uncertain = recovery
        .recover_page(test.namespace.context, TableRecoveryKind::Create, None, 2000)
        .await
        .unwrap();
    assert_eq!(uncertain.failures.len(), 1);
    let expired = recovery
        .recover_page(test.namespace.context, TableRecoveryKind::Create, None, 2000)
        .await
        .unwrap();
    assert_eq!(expired.progressed, 1);
    assert!(expired.failures.is_empty());
    assert_eq!(test.operation().await.phase, TableCreatePhase::Aborted);
}

#[tokio::test]
async fn draft_is_invisible_and_sdk_initial_updates_publish_exactly_one_generation() {
    for version in [1, 2, 3] {
        let mut test = TestStaged::new().await;
        let mut body: serde_json::Value = serde_json::from_slice(&test.request.body).unwrap();
        body["properties"] = json!({"format-version":version.to_string()});
        test.request.body = serde_json::to_vec(&body).unwrap();
        let staged = test.stage().await;
        let response = test.response(&staged).await;
        assert!(response.get("metadata-location").is_none());
        assert_eq!(response["metadata"]["schemas"][0]["fields"][0]["id"], 1);
        for key in test.namespace.store.values.load().keys() {
            assert!(!matches!(
                IcebergKey::decode(key).unwrap(),
                IcebergKey::Catalog {
                    scope: CatalogScope::TableName | CatalogScope::TableHead | CatalogScope::File,
                    ..
                }
            ));
        }
        let request = test.commit_request().await;
        let creator = test.creator();
        let committed = creator.commit_staged(&request).await.unwrap();
        assert_eq!(committed.status, 200);
        let output = test.response(&committed).await;
        assert!(output["metadata-location"].is_string());
        assert_eq!(output["metadata"]["metadata-log"], json!([]));
        assert_eq!(output["metadata"]["properties"]["transaction"], "committed");
        assert_eq!(
            output["metadata"]["table-uuid"],
            response["metadata"]["table-uuid"]
        );
        let selected = TableRepository::new(test.namespace.store.clone())
            .select(test.namespace.context, test.parent.namespace, "events")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(selected.head.generation, 1);
        assert_eq!(selected.head.pending_operation, None);
        assert_eq!(creator.commit_staged(&request).await.unwrap(), committed);
        assert_eq!(test.stage().await, staged);
        assert!(!creator
            .expire_stage(test.namespace.context, selected.head.table, 9999)
            .await
            .unwrap());
        let mut changed = request.clone();
        changed.body.push(b' ');
        assert!(creator.commit_staged(&changed).await.is_err());
    }
}

#[tokio::test]
async fn every_staging_reply_loss_recovers_the_original_draft() {
    let baseline = TestStaged::new().await;
    let before = baseline.namespace.store.writes.load(Ordering::SeqCst);
    baseline.stage().await;
    let writes = baseline.namespace.store.writes.load(Ordering::SeqCst) - before;
    for offset in 1..=writes {
        let test = TestStaged::new().await;
        let store = &test.namespace.store;
        store
            .fail_after
            .store(store.writes.load(Ordering::SeqCst) + offset, Ordering::SeqCst);
        assert!(test.creator().stage(&test.request, 2000).await.is_err());
        store.fail_after.store(0, Ordering::SeqCst);
        let interrupted = crowdb_access_iceberg::commit::TableCreateJournal::new(store.clone())
            .load(test.namespace.context, test.request.identity.operation)
            .await
            .unwrap();
        let staged = test.stage().await;
        assert_eq!(test.stage().await, staged);
        let operation = test.operation().await;
        if let Some(interrupted) = interrupted {
            assert_eq!(interrupted.candidate, operation.candidate);
            assert_eq!(interrupted.document, operation.document);
            assert_eq!(interrupted.response, operation.response);
        }
        assert_eq!(operation.phase, TableCreatePhase::Staged);
        assert_eq!(operation.stage.unwrap().response, staged.body);
    }
}

#[tokio::test]
async fn expiry_reply_loss_never_reactivates_a_draft() {
    for offset in [1, 2] {
        let test = TestStaged::new().await;
        let request = test.commit_request().await;
        let operation = test.operation().await;
        let store = &test.namespace.store;
        store
            .fail_after
            .store(store.writes.load(Ordering::SeqCst) + offset, Ordering::SeqCst);
        assert!(test
            .creator()
            .expire_stage(test.namespace.context, operation.candidate.table, 2000)
            .await
            .is_err());
        store.fail_after.store(0, Ordering::SeqCst);
        test.creator()
            .expire_stage(test.namespace.context, operation.candidate.table, 2000)
            .await
            .unwrap();
        assert_eq!(test.operation().await.phase, TableCreatePhase::Aborted);
        assert!(test.creator().commit_staged(&request).await.is_err());
    }
}

#[tokio::test]
async fn every_final_commit_reply_loss_recovers_without_expiring_bound_publication() {
    let baseline = TestStaged::new().await;
    let request = baseline.commit_request().await;
    let before = baseline.namespace.store.writes.load(Ordering::SeqCst);
    baseline.creator().commit_staged(&request).await.unwrap();
    let writes = baseline.namespace.store.writes.load(Ordering::SeqCst) - before;
    assert!(writes >= 20);
    for offset in 1..=writes {
        let test = TestStaged::new().await;
        let request = test.commit_request().await;
        let store = &test.namespace.store;
        store
            .fail_after
            .store(store.writes.load(Ordering::SeqCst) + offset, Ordering::SeqCst);
        assert!(
            test.creator().commit_staged(&request).await.is_err(),
            "offset {offset}"
        );
        store.fail_after.store(0, Ordering::SeqCst);
        let interrupted = test.operation().await;
        if interrupted.stage.as_ref().unwrap().binding.is_some() {
            assert!(!test
                .creator()
                .expire_stage(test.namespace.context, interrupted.candidate.table, 9999)
                .await
                .unwrap());
        }
        let result = test.creator().commit_staged(&request).await.unwrap();
        assert_eq!(result.status, 200, "offset {offset}");
        let completed = test.operation().await;
        assert_eq!(completed.phase, TableCreatePhase::Complete);
        assert_eq!(completed.candidate.table_uuid, interrupted.candidate.table_uuid);
        if interrupted.stage.unwrap().binding.is_some() {
            assert_eq!(completed.candidate, interrupted.candidate);
            assert_eq!(completed.document, interrupted.document);
        }
        let mut retry = request;
        retry.timestamp_ms = 9999;
        assert_eq!(test.creator().commit_staged(&retry).await.unwrap(), result);
    }
}

#[tokio::test]
async fn expired_or_foreign_draft_cannot_publish_and_parent_drop_remains_safe() {
    let test = TestStaged::new().await;
    let request = test.commit_request().await;
    for changed in [0, 1, 2] {
        let mut foreign = request.clone();
        match changed {
            0 => foreign.principal = "other".into(),
            1 => foreign.name = "other".into(),
            _ => foreign.identity = test.request.identity,
        }
        assert!(test.creator().commit_staged(&foreign).await.is_err());
        assert_eq!(test.operation().await.phase, TableCreatePhase::Staged);
    }
    let operation = test.operation().await;
    assert!(!test
        .creator()
        .expire_stage(test.namespace.context, operation.candidate.table, 1999)
        .await
        .unwrap());
    assert!(test
        .creator()
        .expire_stage(test.namespace.context, operation.candidate.table, 2000)
        .await
        .unwrap());
    assert!(test.creator().commit_staged(&request).await.is_err());
    let test = TestStaged::new().await;
    let request = test.commit_request().await;
    let outcome = NamespaceDropper::new(test.namespace.store.clone())
        .drop_namespace(&test.drop_request())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(outcome.status, 204);
    assert_eq!(test.creator().commit_staged(&request).await.unwrap().status, 404);
}
