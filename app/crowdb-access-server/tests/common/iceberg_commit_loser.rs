use std::path::Path;

use crowdb_access_iceberg::{
    catalog::CatalogContext,
    commit::{TableCommitJournal, TableCommitPhase},
    file::FileRepository,
    namespace::{NamespaceIdentifier, NamespaceRepository},
    table::TableRepository,
};
use serde_json::json;

use super::{case, child::TestCommitChild, common::TestIcebergStack, process::TestIcebergProcess};

pub async fn verify(stack: &TestIcebergStack, context: CatalogContext, directory: &Path, offset: usize) {
    let setup = TestIcebergProcess::start(&stack.cluster.mgmt_endpoints).await;
    let case = case::TestCommitCase::prepare(
        &format!("http://{}", setup.address),
        "update",
        "losing-candidate".into(),
    )
    .await;
    drop(setup);
    let mut loser = TestCommitChild::start(
        &stack.cluster.mgmt_endpoints,
        directory.join("loser.json"),
        offset,
        false,
    )
    .await;
    let endpoint = format!("http://{}", loser.address);
    let path = case.path.clone();
    let identity = case.identity.clone();
    let body = case.body.clone();
    let request = tokio::spawn(async move { case::post(&endpoint, &path, &identity, &body).await });
    assert_eq!(loser.paused().await["label"], "head-2");
    let store = stack.store().await;
    let journal = TableCommitJournal::new(store.clone());
    let operation = journal
        .load(context, case.identity.parse().unwrap())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(operation.phase, TableCommitPhase::Publishing);
    let candidate = operation.candidate.unwrap();
    let files = FileRepository::new(store.clone());
    assert!(files
        .load(context, &candidate.metadata_location)
        .await
        .unwrap()
        .is_some());
    let winner = TestCommitChild::start(
        &stack.cluster.mgmt_endpoints,
        directory.join("winner.json"),
        usize::MAX,
        false,
    )
    .await;
    case::success(
        &format!("http://{}", winner.address),
        &case.path,
        &json!({"requirements":[],"updates":[{"action":"set-properties","updates":{"winner":"selected"}}]}),
    )
    .await;
    drop(winner);
    drop(loser);
    assert!(request.await.unwrap().is_err());
    let recovery = TestIcebergProcess::start(&stack.cluster.mgmt_endpoints).await;
    let endpoint = format!("http://{}", recovery.address);
    let first = case::post(&endpoint, &case.path, &case.identity, &case.body)
        .await
        .unwrap();
    assert_eq!(first.status(), 409);
    let bytes = first.bytes().await.unwrap();
    let replay = case::post(&endpoint, &case.path, &case.identity, &case.body)
        .await
        .unwrap();
    assert_eq!(replay.status(), 409);
    assert_eq!(replay.bytes().await.unwrap(), bytes);
    let changed = case::post(&endpoint, &case.path, &case.identity, &format!("{} ", case.body))
        .await
        .unwrap();
    assert_eq!(changed.status(), 409);
    assert_eq!(
        journal
            .load(context, case.identity.parse().unwrap())
            .await
            .unwrap()
            .unwrap()
            .phase,
        TableCommitPhase::Rejected
    );
    let parent = NamespaceRepository::new(store.clone())
        .load(
            context,
            &NamespaceIdentifier::new(vec!["analytics".into()]).unwrap(),
        )
        .await
        .unwrap()
        .unwrap();
    let selected = TableRepository::new(store)
        .select(context, parent.namespace, &case.name)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(selected.head.generation, 2);
    assert_ne!(selected.head.metadata_location, candidate.metadata_location);
    assert!(files
        .load(context, &candidate.metadata_location)
        .await
        .unwrap()
        .is_some());
}
