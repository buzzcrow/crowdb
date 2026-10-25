// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_console_shared::clients::http::ServerClient;
use crowdb_console_shared::mgmt::{AddGroupRequest, AddStoreRequest, RemoteReplicaInfo};

#[tokio::test]
#[ignore = "requires the owned release KV container"]
async fn full_store_group_remote_cycle() {
    let base = crate::common::management::origin(&crate::common::management::addresses(1)[0]);
    let client = ServerClient::new(base).unwrap();

    let stores_before = client.list_stores().await.expect("list_stores before add_store");
    assert!(
        stores_before.is_empty(),
        "fresh deploy should not auto-create stores: {stores_before:?}"
    );

    let store_id: u64 = 5;
    let group_id: u64 = 50;
    let replica_id: u64 = 500;

    client
        .add_store(&AddStoreRequest {
            store_id,
            port: Some(7002),
        })
        .await
        .expect("add_store");

    // 2. list_stores sees it.
    let stores = client.list_stores().await.expect("list_stores");
    assert!(
        stores.iter().any(|s| s.store_id == store_id),
        "add_store not reflected in list_stores: {stores:?}"
    );

    // 3. get_store returns an empty store until a group is added.
    let detail = client.get_store(store_id).await.expect("get_store");
    assert_eq!(detail.store_id, store_id);
    assert!(
        detail.groups.is_empty(),
        "new store should start empty: {detail:?}"
    );

    // 4. Add the first group explicitly.
    let group_id_2: u64 = 60;
    let replica_id_2: u64 = 600;
    client
        .add_group(
            store_id,
            &AddGroupRequest {
                group_id,
                replica_id,
                initial_role: None,
                start_election: None,
                deployment: None,
            },
        )
        .await
        .expect("add_group first");

    // 5. Add a second group.
    client
        .add_group(
            store_id,
            &AddGroupRequest {
                group_id: group_id_2,
                replica_id: replica_id_2,
                initial_role: None,
                start_election: None,
                deployment: None,
            },
        )
        .await
        .expect("add_group");

    let groups = client.list_groups(store_id).await.expect("list_groups");
    assert_eq!(groups.len(), 2, "expected both groups, got {groups:?}");
    assert!(groups.iter().any(|g| g.group_id == group_id));
    assert!(groups.iter().any(|g| g.group_id == group_id_2));

    // 6. Add a remote replica on the second group.
    let remotes = vec![RemoteReplicaInfo {
        replica_id: 601,
        endpoint: "127.0.0.1:39999".into(),
        voting: true,
    }];
    client
        .add_remote_replicas(store_id, group_id_2, &remotes)
        .await
        .expect("add_remote_replicas");
    let got = client
        .list_remote_replicas(store_id, group_id_2)
        .await
        .expect("list_remote_replicas");
    assert_eq!(got, remotes, "add_remote_replicas round trip");

    // 6. Delete the remote.
    client
        .remove_remote_replica(store_id, group_id_2, 601)
        .await
        .expect("remove_remote_replica");
    let got = client
        .list_remote_replicas(store_id, group_id_2)
        .await
        .expect("list_remote_replicas after delete");
    assert!(got.is_empty(), "remote should be gone: {got:?}");

    // 7. Delete the second group.
    client
        .remove_group(store_id, group_id_2)
        .await
        .expect("remove_group");
    let groups = client
        .list_groups(store_id)
        .await
        .expect("list_groups after delete");
    assert_eq!(groups.len(), 1);
    assert_eq!(groups[0].group_id, group_id);

    // 8. Tear the whole store down. The server implements
    //    DELETE /stores/{sid}; the call must succeed and the store must
    //    disappear from the listing.
    client.remove_store(store_id).await.expect("remove_store");
    let stores_after = client.list_stores().await.expect("list_stores after remove");
    assert!(
        stores_after.iter().all(|s| s.store_id != store_id),
        "store {store_id} still present after remove: {stores_after:?}"
    );
}
