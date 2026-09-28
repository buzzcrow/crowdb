use std::collections::{BTreeMap, BTreeSet};

use crowdb_kv_client::{ClientConfig, CrowdbKvClient, CrowdbSysmdClient};
use crowdb_protocol::common::{GroupValue, ReplicaValue, StoreValue};
use thiserror::Error;

use crate::{
    BootstrapSession, DeploymentProfile, GroupRole, ManifestError, MonitorEvent, MonitorEventKind,
    MonitorLog, MonitorLogError,
};

const STEP: &str = "logical-topology";

#[derive(Debug, Error)]
pub enum LogicalBootstrapError {
    #[error("Group 0 logical topology request failed: {0}")]
    Client(#[from] crowdb_kv_client::Error),
    #[error("bootstrap manifest failed: {0}")]
    Manifest(#[from] ManifestError),
    #[error("monitor lifecycle log failed: {0}")]
    MonitorLog(#[from] MonitorLogError),
    #[error("logical topology conflicts with the deployment profile")]
    Conflict,
    #[error("logical topology bootstrap state is invalid: {0}")]
    Invalid(&'static str),
}

#[derive(Default)]
struct Observed {
    stores: BTreeSet<u64>,
    groups: BTreeSet<(u64, u64)>,
    replicas: BTreeSet<(u64, u64, u64)>,
}

pub struct LogicalBootstrap {
    client: CrowdbSysmdClient,
}

impl LogicalBootstrap {
    #[must_use]
    pub fn new(management_seed: String) -> Self {
        Self {
            client: CrowdbSysmdClient::new(CrowdbKvClient::new(ClientConfig::new(vec![management_seed]))),
        }
    }

    /// # Errors
    /// Reconciles only matching, profile-owned records after the runtime KV groups exist.
    pub async fn reconcile(
        &self,
        session: &mut BootstrapSession,
        profile: &DeploymentProfile,
        events: &mut MonitorLog,
    ) -> Result<(), LogicalBootstrapError> {
        let result = self.reconcile_inner(session, profile, events).await;
        if result.is_err() {
            record(events, MonitorEventKind::BootstrapFailed).await?;
        }
        result
    }

    async fn reconcile_inner(
        &self,
        session: &mut BootstrapSession,
        profile: &DeploymentProfile,
        events: &mut MonitorLog,
    ) -> Result<(), LogicalBootstrapError> {
        profile
            .validate()
            .map_err(|_| LogicalBootstrapError::Invalid("deployment profile is invalid"))?;
        self.client.kv().refresh_topology().await?;
        let expected = expected(profile);
        let found = self.preflight(&expected).await?;
        let complete = session
            .manifest()
            .step_complete(STEP)
            .ok_or(LogicalBootstrapError::Invalid(
                "logical step is absent from manifest",
            ))?;
        if complete {
            return if found.complete(&expected) {
                Ok(())
            } else {
                Err(LogicalBootstrapError::Invalid(
                    "completed logical topology is incomplete",
                ))
            };
        }
        if session.manifest().next_step() != Some(STEP) {
            return Err(LogicalBootstrapError::Invalid("logical step is out of order"));
        }
        record(events, MonitorEventKind::BootstrapStepStarted).await?;
        self.write_missing(&expected, &found).await?;
        if !self.preflight(&expected).await?.complete(&expected) {
            return Err(LogicalBootstrapError::Invalid("logical topology is incomplete"));
        }
        session.complete_step(STEP)?;
        record(events, MonitorEventKind::BootstrapStepCompleted).await?;
        Ok(())
    }

    async fn preflight(&self, expected: &Expected) -> Result<Observed, LogicalBootstrapError> {
        let mut found = Observed::default();
        for store in self.client.list_stores().await? {
            if expected.stores.get(&store.store_id) != Some(&store) {
                return Err(LogicalBootstrapError::Conflict);
            }
            found.stores.insert(store.store_id);
        }
        for store_id in expected.stores.keys() {
            for group in self.client.list_groups_in_store(*store_id).await? {
                let key = (group.store_id, group.group_id);
                if expected.groups.get(&key) != Some(&group) {
                    return Err(LogicalBootstrapError::Conflict);
                }
                found.groups.insert(key);
            }
        }
        for (store_id, group_id) in expected.groups.keys() {
            for replica in self.client.list_replicas_in_group(*store_id, *group_id).await? {
                let key = (replica.store_id, replica.group_id, replica.replica_id);
                if expected.replicas.get(&key) != Some(&replica) {
                    return Err(LogicalBootstrapError::Conflict);
                }
                found.replicas.insert(key);
            }
        }
        Ok(found)
    }

    async fn write_missing(
        &self,
        expected: &Expected,
        found: &Observed,
    ) -> Result<(), LogicalBootstrapError> {
        for (store_id, store) in &expected.stores {
            if !found.stores.contains(store_id) {
                let write = self.client.add_store(*store_id, &store.node_ids).await;
                let actual = self.client.get_store(*store_id).await?;
                verify_write(write, actual.as_ref() == Some(store))?;
            }
        }
        for ((store_id, group_id), group) in &expected.groups {
            if !found.groups.contains(&(*store_id, *group_id)) {
                let write = self.client.add_group(*store_id, *group_id).await;
                let actual = self.client.get_group(*store_id, *group_id).await?;
                verify_write(write, actual.as_ref() == Some(group))?;
            }
        }
        for ((store_id, group_id, replica_id), replica) in &expected.replicas {
            if !found.replicas.contains(&(*store_id, *group_id, *replica_id)) {
                let write = self.client.add_replica(replica).await;
                let actual = self.client.get_replica(*store_id, *group_id, *replica_id).await?;
                verify_write(write, actual.as_ref() == Some(replica))?;
            }
        }
        Ok(())
    }
}

struct Expected {
    stores: BTreeMap<u64, StoreValue>,
    groups: BTreeMap<(u64, u64), GroupValue>,
    replicas: BTreeMap<(u64, u64, u64), ReplicaValue>,
}

impl Observed {
    fn complete(&self, expected: &Expected) -> bool {
        self.stores.len() == expected.stores.len()
            && self.groups.len() == expected.groups.len()
            && self.replicas.len() == expected.replicas.len()
    }
}

fn expected(profile: &DeploymentProfile) -> Expected {
    let mut node_ids = BTreeMap::<u64, BTreeSet<u64>>::new();
    let mut groups = BTreeMap::new();
    let mut replicas = BTreeMap::new();
    for group in &profile.groups {
        node_ids.entry(group.store_id).or_default().insert(group.node_id);
        groups.insert(
            (group.store_id, group.group_id),
            GroupValue {
                store_id: group.store_id,
                group_id: group.group_id,
            },
        );
        replicas.insert(
            (group.store_id, group.group_id, group.replica_id),
            ReplicaValue {
                store_id: group.store_id,
                group_id: group.group_id,
                replica_id: group.replica_id,
                node_id: group.node_id,
                role: match group.role {
                    GroupRole::System => "system",
                    GroupRole::Data => "data",
                }
                .to_owned(),
                voting: true,
                endpoint: group.rpc_endpoint.clone(),
            },
        );
    }
    Expected {
        stores: node_ids
            .into_iter()
            .map(|(store_id, node_ids)| {
                (
                    store_id,
                    StoreValue {
                        store_id,
                        node_ids: node_ids.into_iter().collect(),
                    },
                )
            })
            .collect(),
        groups,
        replicas,
    }
}

fn verify_write(
    write: Result<(), crowdb_kv_client::Error>,
    matches: bool,
) -> Result<(), LogicalBootstrapError> {
    if matches {
        Ok(())
    } else {
        write?;
        Err(LogicalBootstrapError::Invalid("Group 0 write did not persist"))
    }
}

async fn record(events: &mut MonitorLog, kind: MonitorEventKind) -> Result<(), MonitorLogError> {
    events
        .record(&MonitorEvent {
            kind,
            service: Some(STEP),
            pid: None,
            attempt: None,
        })
        .await
}

pub fn logical_step_names() -> impl Iterator<Item = &'static str> {
    [STEP].into_iter()
}
