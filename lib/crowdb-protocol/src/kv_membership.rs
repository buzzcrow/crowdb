// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Complete, epoch-qualified group membership authority.

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct GroupMember {
    pub replica_id: u64,
    pub node_id: u64,
    pub endpoint: String,
    pub voting: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum GroupMembershipState {
    Installing { previous_members: Vec<GroupMember> },
    Ready,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct GroupMembership {
    pub store_id: u64,
    pub group_id: u64,
    pub epoch: u64,
    pub members: Vec<GroupMember>,
    pub installation: GroupMembershipState,
}

impl GroupMembership {
    /// Validate a complete authoritative configuration without legacy defaults.
    ///
    /// # Errors
    /// Rejects epoch zero, unordered/duplicate identities, empty endpoints,
    /// or a membership with no voting replica.
    pub fn validate(&self) -> Result<(), String> {
        if self.epoch == 0 {
            return Err("membership epoch must be positive".into());
        }
        validate_members(&self.members)?;
        if let GroupMembershipState::Installing { previous_members } = &self.installation {
            if !previous_members.is_empty() {
                validate_members(previous_members)?;
            }
        }
        Ok(())
    }
}

/// Sort members into canonical identity order and validate them.
///
/// # Errors
/// Rejects duplicate replica/node identities, empty endpoints or no voters.
pub fn canonical_members(mut members: Vec<GroupMember>) -> Result<Vec<GroupMember>, String> {
    members.sort_unstable_by_key(|member| member.replica_id);
    validate_members(&members)?;
    Ok(members)
}

fn validate_members(members: &[GroupMember]) -> Result<(), String> {
    let mut nodes = BTreeSet::new();
    let mut previous_replica = None;
    for member in members {
        if previous_replica.is_some_and(|previous| previous >= member.replica_id)
            || !nodes.insert(member.node_id)
        {
            return Err("membership identities must be unique and ordered".into());
        }
        if member.endpoint.trim().is_empty() {
            return Err("member endpoint must be present".into());
        }
        previous_replica = Some(member.replica_id);
    }
    if !members.iter().any(|member| member.voting) {
        return Err("membership needs a voting replica".into());
    }
    Ok(())
}
