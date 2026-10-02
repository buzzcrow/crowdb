use std::path::Path;

use crowdb_access_iceberg::{
    catalog::{CatalogContext, RoutedCatalogStore},
    commit::TableCommitJournal,
    file::FileRepository,
    namespace::{NamespaceIdentifier, NamespaceRepository},
    table::{SelectedTable, TableRepository},
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
    assert!(journal
        .load(context, case.identity.parse().unwrap())
        .await
        .unwrap()
        .is_none());
    let before = selected(store.clone(), context, &case.name).await;
    assert_eq!(before.head.generation, 1);
    let operation: crowdb_access_iceberg::key::OperationId = case.identity.parse().unwrap();
    let candidate_location = before
        .head
        .metadata_location
        .table()
        .file(&format!("metadata/{operation}.metadata.json"))
        .unwrap();
    let files = FileRepository::new(store.clone());
    let candidate = files.load(context, &candidate_location).await.unwrap().unwrap();
    let winner = TestCommitChild::start(
        &stack.cluster.mgmt_endpoints,
        directory.join("winner.json"),
        usize::MAX,
        false,
    )
    .await;
    let winning_response = case::success(
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
    assert_eq!(first.status(), 503);
    let bytes = first.bytes().await.unwrap();
    let error: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(error["error"]["type"], "ServiceUnavailableException");
    let replay = case::post(&endpoint, &case.path, &case.identity, &case.body)
        .await
        .unwrap();
    assert_eq!(replay.status(), 503);
    assert_eq!(replay.bytes().await.unwrap(), bytes);
    let changed = case::post(&endpoint, &case.path, &case.identity, &format!("{} ", case.body))
        .await
        .unwrap();
    assert_eq!(changed.status(), 503);
    assert!(journal
        .load(context, case.identity.parse().unwrap())
        .await
        .unwrap()
        .is_none());
    let current = selected(store, context, &case.name).await;
    assert_eq!(current.head.generation, 2);
    assert_ne!(current.head.metadata_location, candidate_location);
    assert_eq!(
        current.head.metadata_location.to_string(),
        winning_response["metadata-location"].as_str().unwrap()
    );
    assert_eq!(
        files.load(context, &candidate_location).await.unwrap().unwrap(),
        candidate
    );
}

async fn selected(
    store: std::sync::Arc<RoutedCatalogStore>,
    context: CatalogContext,
    name: &str,
) -> SelectedTable {
    let parent = NamespaceRepository::new(store.clone())
        .load(
            context,
            &NamespaceIdentifier::new(vec!["analytics".into()]).unwrap(),
        )
        .await
        .unwrap()
        .unwrap();
    TableRepository::new(store)
        .select(context, parent.namespace, name)
        .await
        .unwrap()
        .unwrap()
}
