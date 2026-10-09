// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_protocol::key::{KvGroupMembersKey, TextKey};
use crowdb_protocol::kv_membership::{canonical_members, GroupMember, GroupMembership, GroupMembershipState};

fn member(replica_id: u64, node_id: u64) -> GroupMember {
    GroupMember {
        replica_id,
        node_id,
        endpoint: format!("127.0.0.1:{}", 10000 + node_id),
        voting: true,
    }
}

#[test]
fn complete_membership_rejects_conflicting_identities_and_missing_authority() {
    assert!(canonical_members(vec![member(1, 1), member(1, 2)]).is_err());
    assert!(canonical_members(vec![member(1, 1), member(2, 1)]).is_err());
    assert!(canonical_members(Vec::new()).is_err());
    let mut missing = member(1, 1);
    missing.endpoint.clear();
    assert!(canonical_members(vec![missing]).is_err());
    let mut nonvoting = member(1, 1);
    nonvoting.voting = false;
    assert!(canonical_members(vec![nonvoting]).is_err());
    assert!(serde_json::from_str::<GroupMembership>(r#"{"store_id":1,"group_id":2}"#).is_err());
}

#[test]
fn installed_and_previous_members_are_canonical_epoch_qualified_configuration() {
    let members = canonical_members(vec![member(2, 2), member(1, 1)]).unwrap();
    assert_eq!(members[0].replica_id, 1);
    let mut record = GroupMembership {
        store_id: 4,
        group_id: 5,
        epoch: 2,
        members,
        installation: GroupMembershipState::Installing {
            previous_members: vec![member(1, 1)],
        },
    };
    record.validate().unwrap();
    record.epoch = 0;
    assert!(record.validate().is_err());
    let key = KvGroupMembersKey {
        store_id: 4,
        group_id: 5,
    };
    assert_eq!(key.to_path(), "/kv/members/4/5");
    assert_eq!(KvGroupMembersKey::from_path(&key.to_path()).unwrap(), key);
}
