// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::sync::Arc;

use crowdb_kv::cluster::group::PxGroup;
use crowdb_kv::cluster::kv_server::KvServer;
use crowdb_kv::cluster::{PxKvStore, PxLocalReplica, PxLocalReplicaRole};
use crowdb_kv_client::{ClientConfig, CrowdbKvClient, Error, GroupMembershipClient};
use crowdb_protocol::kv_membership::{GroupMember, GroupMembershipState};

struct TestMembershipServer {
    store: Arc<PxKvStore>,
    endpoint: String,
}

impl TestMembershipServer {
    async fn start() -> Self {
        let store = Arc::new(PxKvStore::new(0, "127.0.0.1:0".parse().unwrap()));
        store.add_group(PxGroup::new(
            0,
            PxLocalReplica::new(1, PxLocalReplicaRole::Leader),
        ));
        store.start().await.unwrap();
        let endpoint = store.listen_addr().unwrap().to_string();
        Self { store, endpoint }
    }

    fn client(&self) -> GroupMembershipClient {
        let kv = Arc::new(CrowdbKvClient::new(ClientConfig::new(Vec::new())));
        kv.seed_leader(0, 0, self.endpoint.clone());
        GroupMembershipClient::from_shared(kv)
    }
}

impl Drop for TestMembershipServer {
    fn drop(&mut self) {
        self.store.stop();
    }
}

fn member(id: u64) -> GroupMember {
    GroupMember {
        replica_id: id,
        node_id: id,
        endpoint: format!("127.0.0.1:{}", 10000 + id),
        voting: true,
    }
}

#[tokio::test]
async fn independent_clients_cannot_submit_two_changes_or_skip_installation() {
    let server = TestMembershipServer::start().await;
    let first = server.client();
    let second = server.client();
    let created = first.create(4, 5, vec![member(1)]).await.unwrap();
    assert!(matches!(
        second.begin_change(4, 5, 1, vec![member(1), member(2)]).await,
        Err(Error::MembershipConflict { .. })
    ));
    first.complete(&created).await.unwrap();
    let (a, b) = tokio::join!(
        first.begin_change(4, 5, 1, vec![member(1), member(2)]),
        second.begin_change(4, 5, 1, vec![member(1), member(3)])
    );
    let winner = match (a, b) {
        (Ok(winner), Err(Error::MembershipConflict { .. }))
        | (Err(Error::MembershipConflict { .. }), Ok(winner)) => winner,
        results => panic!("expected one CAS winner and one conflict: {results:?}"),
    };
    assert_eq!(winner.record().epoch, 2);
    assert_eq!(winner.record().members.len(), 2);
    assert_eq!(
        winner.record().installation,
        GroupMembershipState::Installing {
            previous_members: vec![member(1)]
        }
    );
    assert!(matches!(
        first.begin_change(4, 5, 2, vec![member(1), member(4)]).await,
        Err(Error::MembershipConflict { .. })
    ));
    second.create(4, 6, vec![member(7)]).await.unwrap();
    second.complete(&winner).await.unwrap();
    let confirmed = first.complete(&winner).await.unwrap();
    assert_eq!(confirmed.record().epoch, 2);
    assert_eq!(confirmed.record().installation, GroupMembershipState::Ready);
    assert!(matches!(
        first.complete(&created).await,
        Err(Error::MembershipConflict { .. })
    ));
    let recovered = server.client().read(4, 5).await.unwrap().unwrap();
    assert_eq!(recovered.record().epoch, 2);
    assert_eq!(recovered.record().members, winner.record().members);
    assert_eq!(recovered.record().installation, GroupMembershipState::Ready);
}
