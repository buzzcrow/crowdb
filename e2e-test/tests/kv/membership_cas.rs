// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crate::common::management::{addresses, client as http_client, origin, wait_for_leader};
use crowdb_paxos_kv_client::{ClientConfig, Error, GroupMembershipClient, PaxosKvClient};
use crowdb_protocol::kv_membership::{GroupMember, GroupMembershipState};

async fn client() -> GroupMembershipClient {
    let ips = addresses(1);
    wait_for_leader(&http_client(), &origin(&ips[0])).await;
    GroupMembershipClient::from_shared(std::sync::Arc::new(PaxosKvClient::new(ClientConfig::new(vec![
        origin(&ips[0]),
    ]))))
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
#[ignore = "requires the owned release-container Group-0 profile"]
async fn independent_clients_cannot_submit_two_changes_or_skip_installation() {
    let first = client().await;
    let second = client().await;
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
    let recovered = client().await.read(4, 5).await.unwrap().unwrap();
    assert_eq!(recovered.record().epoch, 2);
    assert_eq!(recovered.record().members, winner.record().members);
    assert_eq!(recovered.record().installation, GroupMembershipState::Ready);
}
