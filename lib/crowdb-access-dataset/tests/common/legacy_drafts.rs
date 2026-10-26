// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_access_dataset::{composition::*, split::*, *};
use serde_json::json;
use sha2::{Digest, Sha256};

#[derive(serde::Serialize)]
struct TestEditMarker {
    id: OperationId,
    digest: [u8; 32],
}

#[derive(serde::Serialize)]
struct TestEditReceipt {
    marker: TestEditMarker,
    draft: CompositionDraft,
}

pub async fn editing_composition(
    store: &dyn DatasetStore,
    authority: &DatasetAuthority,
    identity: &DatasetIdentity,
    operation: OperationId,
    parent: SnapshotId,
) -> CompositionDraft {
    let draft = CompositionDraft {
        record: CompositionRecord {
            version: 3,
            operation,
            parent: Some(parent),
            parent_split_revision: None,
            source: CompositionSource::All,
            revision: 1,
            edit_pages: 0,
            source_lease_deadline: 0,
            progress: DraftProgress::Editing,
        },
        schema: authority.get_manifest(identity, parent).await.unwrap().schema,
        splits: vec![],
        policy: SplitPolicy::default(),
        preparation: None,
    };
    composition(store, identity, &draft).await;
    draft
}

pub fn key(prefix: &[u8], identity: &DatasetIdentity, operation: OperationId) -> Vec<u8> {
    [prefix, identity.key().unwrap().as_slice(), operation.as_bytes()].concat()
}

pub async fn composition(store: &dyn DatasetStore, identity: &DatasetIdentity, draft: &CompositionDraft) {
    draft.record.validate().unwrap();
    assert_eq!(draft.record.edit_pages, 0);
    let bytes = serde_json::to_vec(&json!({
        "draft": draft, "edit": null, "journal": [], "evaluation": null,
    }))
    .unwrap();
    install(store, &key(b"DS\0CD\0", identity, draft.record.operation), &bytes).await;
}

pub async fn composition_edit(
    store: &dyn DatasetStore,
    identity: &DatasetIdentity,
    operation: OperationId,
    samples: Vec<SampleChange>,
    applied: bool,
) -> (OperationId, Vec<u8>) {
    let root = key(b"DS\0CD\0", identity, operation);
    let stored = store.get(&root).await.unwrap().unwrap();
    let mut state: serde_json::Value = serde_json::from_slice(&stored.bytes).unwrap();
    let revision = state["draft"]["record"]["revision"].as_u64().unwrap();
    let id = OperationId::random();
    let payload = serde_json::to_vec(&json!({
        "id": id, "expected_revision": revision, "schema": null,
        "samples": samples, "splits": null, "policy": null,
    }))
    .unwrap();
    state["registered_edits"] = json!([id]);
    if applied {
        state["draft"]["record"]["revision"] = (revision + 1).into();
        state["draft"]["record"]["edit_pages"] = 1.into();
        state["draft"]["record"]["progress"] = json!(DraftProgress::Editing);
        state["draft"]["preparation"] = serde_json::Value::Null;
        state["journal"] = json!([id]);
        let marker = TestEditMarker {
            id,
            digest: Sha256::digest(&payload).into(),
        };
        state["edit"] = json!(marker);
        state["evaluation"] = serde_json::Value::Null;
        let mut page = key(b"DS\0CE\0", identity, operation);
        page.extend(id.as_bytes());
        install(store, &page, &payload).await;
        let mut receipt = key(b"DS\0CX\0", identity, operation);
        receipt.extend(id.as_bytes());
        install(
            store,
            &receipt,
            &serde_json::to_vec(&TestEditReceipt {
                marker,
                draft: serde_json::from_value(state["draft"].clone()).unwrap(),
            })
            .unwrap(),
        )
        .await;
    }
    replace(store, &root, &stored.bytes, &state).await;
    (id, payload)
}

pub async fn composition_window(
    store: &dyn DatasetStore,
    identity: &DatasetIdentity,
    operation: OperationId,
    samples: &[SampleRecord],
) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut scope = operation.as_bytes().to_vec();
    scope.extend(1u64.to_be_bytes());
    let base = identity.key().unwrap();
    let mut rows = Vec::new();
    for sample in samples {
        let mut row = [b"DS\0CM\0".as_slice(), &base, &scope].concat();
        for byte in &sample.sample_id {
            if *byte == 0 {
                row.extend([0, 255]);
            } else {
                row.push(*byte);
            }
        }
        row.extend([0, 0]);
        let bytes = serde_json::to_vec(sample).unwrap();
        install(store, &row, &bytes).await;
        rows.push((row, bytes));
    }
    let page = [b"DS\0CW\0".as_slice(), &base, &scope, &0u64.to_be_bytes()].concat();
    let binding = [0u8; 32];
    install(
        store,
        &page,
        &serde_json::to_vec(&json!({
            "next": {
                "preview": {"input_revision": 1, "next_page": 1, "members": 0, "complete": false, "binding": binding},
                "phase": {"Source": {"after": samples.last().map(|sample| &sample.sample_id)}},
            },
            "rows": samples.iter().map(|sample| json!({"id": sample.sample_id, "expected": null, "sample": sample})).collect::<Vec<_>>(),
        }))
        .unwrap(),
    )
    .await;
    rows
}

pub async fn prepared_composition(
    store: &dyn DatasetStore,
    identity: &DatasetIdentity,
    operation: OperationId,
    snapshot: SnapshotId,
) {
    let root = key(b"DS\0CD\0", identity, operation);
    let stored = store.get(&root).await.unwrap().unwrap();
    let mut state: serde_json::Value = serde_json::from_slice(&stored.bytes).unwrap();
    state["draft"]["record"]["progress"] = json!(DraftProgress::Prepared {
        target: CommitTarget::Snapshot { snapshot },
    });
    state["draft"]["preparation"] = json!(CompositionPreparation {
        candidate: snapshot,
        split_operation: None,
        split_preview: None,
    });
    state["edit"] = serde_json::Value::Null;
    state["evaluation"] = serde_json::Value::Null;
    replace(store, &root, &stored.bytes, &state).await;
}

pub async fn replace(store: &dyn DatasetStore, key: &[u8], old: &[u8], value: &serde_json::Value) {
    assert!(matches!(
        store
            .compare_exchange(key, Some(old), &serde_json::to_vec(value).unwrap())
            .await
            .unwrap(),
        CasOutcome::Applied(_)
    ));
}

pub async fn split(store: &dyn DatasetStore, identity: &DatasetIdentity, record: &SplitDraftRecord) {
    record.validate().unwrap();
    let bytes = serde_json::to_vec(&json!({
        "record":record, "work_version":1, "initial":false, "edit":null, "evaluation":null, "source_leases":[],
    })).unwrap();
    install(store, &key(b"DS\0SD\0", identity, record.operation), &bytes).await;
}

pub async fn install(store: &dyn DatasetStore, key: &[u8], bytes: &[u8]) {
    assert!(matches!(
        store.compare_exchange(key, None, bytes).await.unwrap(),
        CasOutcome::Applied(_)
    ));
}
