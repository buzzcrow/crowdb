// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Group-0 authority for complete membership changes.

use std::sync::Arc;

use crowdb_protocol::key::{KvGroupMembersKey, TextKey};
use crowdb_protocol::kv_membership::{canonical_members, GroupMember, GroupMembership, GroupMembershipState};

use crate::{CrowdbKvClient, Error, GetOutcome, ReadMode, Result};

/// An exact authoritative configuration and its Group-0 CAS revision.
#[derive(Clone, Debug)]
pub struct GroupMembershipSnapshot {
    record: GroupMembership,
    revision: u64,
}

impl GroupMembershipSnapshot {
    #[must_use]
    pub fn record(&self) -> &GroupMembership {
        &self.record
    }
}

/// Shared membership authority; independent UI servers use the same CAS key.
#[derive(Clone)]
pub struct GroupMembershipClient {
    kv: Arc<CrowdbKvClient>,
}

impl GroupMembershipClient {
    #[must_use]
    pub fn from_shared(kv: Arc<CrowdbKvClient>) -> Self {
        Self { kv }
    }

    /// Read the exact current configuration without consulting legacy records.
    ///
    /// # Errors
    /// Returns transport, decoding or configuration validation errors.
    pub async fn read(&self, store_id: u64, group_id: u64) -> Result<Option<GroupMembershipSnapshot>> {
        let key = KvGroupMembersKey { store_id, group_id }.to_path();
        let GetOutcome::Found { value, revision } = self
            .kv
            .get(0, 0, key.as_bytes(), ReadMode::Linearizable, None)
            .await?
        else {
            return Ok(None);
        };
        let record: GroupMembership =
            serde_json::from_slice(&value).map_err(|error| Error::SysdataDecode {
                key: key.clone(),
                reason: error.to_string(),
            })?;
        record.validate().map_err(|reason| Error::SysdataDecode {
            key: key.clone(),
            reason,
        })?;
        if record.store_id != store_id || record.group_id != group_id {
            return Err(Error::SysdataDecode {
                key,
                reason: "membership identity differs from key".into(),
            });
        }
        Ok(Some(GroupMembershipSnapshot { record, revision }))
    }

    /// Create fresh membership at epoch one in Installing state.
    ///
    /// # Errors
    /// Existing authority returns conflict. Unknown CAS outcomes stay unknown;
    /// callers reconcile by reading the exact epoch and complete members.
    pub async fn create(
        &self,
        store_id: u64,
        group_id: u64,
        members: Vec<GroupMember>,
    ) -> Result<GroupMembershipSnapshot> {
        let key = KvGroupMembersKey { store_id, group_id }.to_path();
        let members = canonical_members(members).map_err(|reason| Error::SysdataDecode { key, reason })?;
        self.publish(
            GroupMembership {
                store_id,
                group_id,
                epoch: 1,
                members,
                installation: GroupMembershipState::Installing {
                    previous_members: Vec::new(),
                },
            },
            0,
            0,
        )
        .await
    }

    /// Submit a complete successor only while the expected epoch is Ready.
    ///
    /// # Errors
    /// Stale epochs, absent authority or an Installing record return conflict.
    /// No conflict is retried using a newer epoch.
    pub async fn begin_change(
        &self,
        store_id: u64,
        group_id: u64,
        expected_epoch: u64,
        members: Vec<GroupMember>,
    ) -> Result<GroupMembershipSnapshot> {
        let current = self
            .read(store_id, group_id)
            .await?
            .ok_or_else(|| conflict(store_id, group_id, expected_epoch))?;
        if current.record.epoch != expected_epoch
            || current.record.installation != GroupMembershipState::Ready
        {
            return Err(conflict(store_id, group_id, expected_epoch));
        }
        let epoch = expected_epoch
            .checked_add(1)
            .ok_or_else(|| Error::Server("membership epoch exhausted".into()))?;
        let members = canonical_members(members).map_err(|reason| Error::SysdataDecode {
            key: KvGroupMembersKey { store_id, group_id }.to_path(),
            reason,
        })?;
        self.publish(
            GroupMembership {
                store_id,
                group_id,
                epoch,
                members,
                installation: GroupMembershipState::Installing {
                    previous_members: current.record.members,
                },
            },
            current.revision,
            expected_epoch,
        )
        .await
    }

    /// Record completion after the coordinator verifies installation/fencing.
    ///
    /// # Errors
    /// A changed authoritative record returns conflict. An unknown write is
    /// confirmed by rereading the exact Ready epoch, never by clearing blindly.
    pub async fn complete(&self, installed: &GroupMembershipSnapshot) -> Result<GroupMembershipSnapshot> {
        let mut record = installed.record.clone();
        if record.installation == GroupMembershipState::Ready {
            return Err(conflict(record.store_id, record.group_id, record.epoch));
        }
        record.installation = GroupMembershipState::Ready;
        let epoch = record.epoch;
        match self.publish(record.clone(), installed.revision, epoch).await {
            Ok(completed) => Ok(completed),
            Err(error @ (Error::MembershipConflict { .. } | Error::OutcomeUnknown)) => {
                if let Ok(Some(current)) = self.read(record.store_id, record.group_id).await {
                    if current.record == record {
                        return Ok(current);
                    }
                }
                Err(error)
            }
            Err(error) => Err(error),
        }
    }

    /// Remove the authoritative record after the group has been removed from
    /// every hosting node. A lost delete response is confirmed by a
    /// linearizable read and never treated as success blindly.
    ///
    /// # Errors
    /// Returns transport, delete, or confirmation errors.
    pub async fn remove(&self, store_id: u64, group_id: u64) -> Result<()> {
        let key = KvGroupMembersKey { store_id, group_id }.to_path();
        match self.kv.delete(0, 0, key.as_bytes(), None).await {
            Ok(_) => Ok(()),
            Err(error @ (Error::OutcomeUnknown | Error::CasFailed { .. })) => {
                match self.read(store_id, group_id).await? {
                    None => Ok(()),
                    Some(_) => Err(error),
                }
            }
            Err(error) => Err(error),
        }
    }

    async fn publish(
        &self,
        record: GroupMembership,
        expected_revision: u64,
        expected_epoch: u64,
    ) -> Result<GroupMembershipSnapshot> {
        let key = KvGroupMembersKey {
            store_id: record.store_id,
            group_id: record.group_id,
        }
        .to_path();
        record.validate().map_err(|reason| Error::SysdataDecode {
            key: key.clone(),
            reason,
        })?;
        let payload = serde_json::to_vec(&record).map_err(|error| Error::SysdataDecode {
            key: key.clone(),
            reason: error.to_string(),
        })?;
        let outcome = self
            .kv
            .put_cas(0, 0, key.as_bytes(), &payload, expected_revision)
            .await
            .map_err(|error| match error {
                Error::CasFailed { .. } | Error::CasBusy => {
                    conflict(record.store_id, record.group_id, expected_epoch)
                }
                other => other,
            })?;
        Ok(GroupMembershipSnapshot {
            record,
            revision: outcome.revision,
        })
    }
}

fn conflict(store_id: u64, group_id: u64, expected_epoch: u64) -> Error {
    Error::MembershipConflict {
        store_id,
        group_id,
        expected_epoch,
    }
}
