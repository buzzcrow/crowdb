// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_console_shared::ops::kv_logical::{
    complete_initial_membership_for_tests, initial_membership_for_tests,
};
use crowdb_paxos_kv::cluster::{
    group::PxGroup,
    local_replica::{PxLocalReplica, PxLocalReplicaRole},
    paxos_kv_server::PaxosKvServer,
    px_kv_store::PxKvStore,
};
use crowdb_paxos_kv_client::{ClientConfig, GroupMembershipClient, PaxosKvClient};
use crowdb_protocol::kv_membership::{GroupMember, GroupMembershipState};
use std::sync::Arc;

async fn authority() -> (Arc<PxKvStore>, GroupMembershipClient, Vec<GroupMember>) {
    let store = Arc::new(PxKvStore::new(0, "127.0.0.1:0".parse().unwrap()));
    store.add_group(PxGroup::new(
        0,
        PxLocalReplica::new(1, PxLocalReplicaRole::Leader),
    ));
    store.start().await.unwrap();
    let endpoint = store.listen_addr().unwrap().to_string();
    let client = Arc::new(PaxosKvClient::new(ClientConfig::new(Vec::new())));
    client.seed_leader(0, 0, endpoint.clone());
    let member = GroupMember {
        node_id: 1,
        replica_id: 10,
        endpoint,
        voting: true,
    };
    (store, GroupMembershipClient::from_shared(client), vec![member])
}

#[tokio::test]
async fn publication_replay_completes_installing_membership_without_resetting_ready_state() {
    let (store, authority, members) = authority().await;
    let original = authority.create(0, 1, members.clone()).await.unwrap();
    let replay = initial_membership_for_tests(&authority, members.clone())
        .await
        .unwrap();
    assert_eq!(original.record(), replay.record());
    complete_initial_membership_for_tests(&authority, &replay)
        .await
        .unwrap();
    let ready = initial_membership_for_tests(&authority, members).await.unwrap();
    assert_eq!(ready.record().installation, GroupMembershipState::Ready);
    complete_initial_membership_for_tests(&authority, &ready)
        .await
        .unwrap();
    assert_eq!(
        authority.read(0, 1).await.unwrap().unwrap().record(),
        ready.record()
    );
    store.stop();
}

#[tokio::test]
async fn publication_replay_rejects_other_members_and_superseded_epochs() {
    let (store, authority, members) = authority().await;
    let installing = authority.create(0, 1, members.clone()).await.unwrap();
    let mut foreign = members.clone();
    foreign[0].replica_id = 99;
    assert!(initial_membership_for_tests(&authority, foreign).await.is_err());
    authority.complete(&installing).await.unwrap();
    authority.begin_change(0, 1, 1, members.clone()).await.unwrap();
    assert!(initial_membership_for_tests(&authority, members).await.is_err());
    assert!(complete_initial_membership_for_tests(&authority, &installing)
        .await
        .is_err());
    assert_eq!(authority.read(0, 1).await.unwrap().unwrap().record().epoch, 2);
    store.stop();
}
