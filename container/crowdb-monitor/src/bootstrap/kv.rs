use std::collections::BTreeSet;
use std::time::Duration;

use crowdb_protocol::mgmt::{AddGroupRequest, GroupSummary, StoreListResponse, SystemInitRequest};
use serde::Deserialize;
use thiserror::Error;
use tokio::time::{sleep, Instant};

use crate::{BootstrapSession, DeploymentProfile, GroupProfile, GroupRole, ManifestError};

const READY_DEADLINE: Duration = Duration::from_secs(30);
const POLL_INTERVAL: Duration = Duration::from_millis(100);

#[derive(Debug, Error)]
pub enum KvBootstrapError {
    #[error("KV management request failed: {0}")]
    Http(#[from] reqwest::Error),
    #[error("bootstrap manifest failed: {0}")]
    Manifest(#[from] ManifestError),
    #[error("KV bootstrap state is invalid: {0}")]
    Invalid(&'static str),
}

#[derive(Deserialize)]
struct GroupReadiness {
    ready: bool,
    leader_id: u64,
    voting_replicas: u32,
    reachable_replicas: u32,
}

pub struct KvBootstrap {
    client: reqwest::Client,
    base_url: reqwest::Url,
}

impl KvBootstrap {
    /// # Errors
    /// Rejects an invalid management endpoint or HTTP client configuration.
    pub fn new(base_url: &str) -> Result<Self, KvBootstrapError> {
        let base_url = reqwest::Url::parse(base_url)
            .map_err(|_| KvBootstrapError::Invalid("KV management URI is invalid"))?;
        if base_url.scheme() != "http" || base_url.path() != "/" || base_url.query().is_some() {
            return Err(KvBootstrapError::Invalid(
                "KV management URI must be an HTTP origin",
            ));
        }
        let client = reqwest::Client::builder()
            .no_proxy()
            .timeout(Duration::from_secs(5))
            .redirect(reqwest::redirect::Policy::none())
            .build()?;
        Ok(Self { client, base_url })
    }

    /// # Errors
    /// Rejects unknown topology, conflicting identities, or uncertain creation results.
    pub async fn reconcile(
        &self,
        session: &mut BootstrapSession,
        profile: &DeploymentProfile,
    ) -> Result<(), KvBootstrapError> {
        let ordered = ordered_groups(profile)?;
        self.reject_unknown_stores().await?;
        let known = self.list_groups().await?;
        let expected = ordered
            .iter()
            .map(|group| group.group_id)
            .collect::<BTreeSet<_>>();
        if known.iter().any(|group| !expected.contains(&group.group_id)) {
            return Err(KvBootstrapError::Invalid("KV store contains an unknown group"));
        }
        for group in ordered {
            let name = step_name(group);
            let complete = session
                .manifest()
                .step_complete(&name)
                .ok_or(KvBootstrapError::Invalid("KV step is absent from manifest"))?;
            if let Some(existing) = self
                .list_groups()
                .await?
                .iter()
                .find(|entry| entry.group_id == group.group_id)
            {
                verify_group(existing, group)?;
                self.wait_ready(group).await?;
                if !complete {
                    session.complete_step(&name)?;
                }
                continue;
            }
            if complete || session.manifest().next_step() != Some(name.as_str()) {
                return Err(KvBootstrapError::Invalid("completed KV group is missing"));
            }
            self.create_group(group).await?;
            let existing = self.wait_present(group).await?;
            verify_group(&existing, group)?;
            self.wait_ready(group).await?;
            session.complete_step(&name)?;
        }
        Ok(())
    }

    async fn reject_unknown_stores(&self) -> Result<(), KvBootstrapError> {
        let response = self
            .client
            .get(self.url("stores"))
            .send()
            .await?
            .error_for_status()?;
        let stores: StoreListResponse = response.json().await?;
        if stores.stores.iter().any(|store| store.store_id != 0) {
            return Err(KvBootstrapError::Invalid("KV server contains an unknown store"));
        }
        Ok(())
    }

    async fn list_groups(&self) -> Result<Vec<GroupSummary>, KvBootstrapError> {
        let response = self.client.get(self.url("stores/0/groups")).send().await?;
        if response.status() == reqwest::StatusCode::NOT_FOUND {
            return Ok(Vec::new());
        }
        Ok(response.error_for_status()?.json().await?)
    }

    async fn create_group(&self, group: &GroupProfile) -> Result<(), KvBootstrapError> {
        let response = if group.role == GroupRole::System {
            self.client
                .post(self.url("system/init"))
                .json(&SystemInitRequest {
                    replica_id: group.replica_id,
                    start_election: true,
                })
                .send()
                .await
        } else {
            self.client
                .post(self.url("stores/0/groups"))
                .json(&AddGroupRequest {
                    group_id: group.group_id,
                    replica_id: group.replica_id,
                    initial_role: None,
                    start_election: Some(true),
                })
                .send()
                .await
        };
        match response {
            Ok(response)
                if response.status().is_client_error()
                    && response.status() != reqwest::StatusCode::CONFLICT =>
            {
                Err(KvBootstrapError::Invalid("KV rejected group creation"))
            }
            Ok(_) | Err(_) => Ok(()),
        }
    }

    async fn wait_present(&self, group: &GroupProfile) -> Result<GroupSummary, KvBootstrapError> {
        let deadline = Instant::now() + READY_DEADLINE;
        loop {
            if let Some(existing) = self
                .list_groups()
                .await?
                .into_iter()
                .find(|entry| entry.group_id == group.group_id)
            {
                return Ok(existing);
            }
            if Instant::now() >= deadline {
                return Err(KvBootstrapError::Invalid("created KV group is not visible"));
            }
            sleep(POLL_INTERVAL).await;
        }
    }

    async fn wait_ready(&self, group: &GroupProfile) -> Result<(), KvBootstrapError> {
        let deadline = Instant::now() + READY_DEADLINE;
        let path = format!("stores/{}/groups/{}/ready", group.store_id, group.group_id);
        loop {
            let response = self.client.get(self.url(&path)).send().await?;
            if response.status() == reqwest::StatusCode::OK {
                let readiness: GroupReadiness = response.json().await?;
                if readiness.ready
                    && readiness.leader_id == group.replica_id
                    && readiness.voting_replicas == 1
                    && readiness.reachable_replicas == 1
                {
                    return Ok(());
                }
            } else if response.status() != reqwest::StatusCode::SERVICE_UNAVAILABLE {
                return Err(KvBootstrapError::Invalid(
                    "KV group readiness endpoint is incompatible",
                ));
            }
            if Instant::now() >= deadline {
                return Err(KvBootstrapError::Invalid("KV group leadership deadline expired"));
            }
            sleep(POLL_INTERVAL).await;
        }
    }

    fn url(&self, path: &str) -> reqwest::Url {
        self.base_url
            .join(path)
            .expect("validated origin accepts relative paths")
    }
}

/// # Errors
/// Rejects group layouts unsupported by the current one-store KV bootstrap.
pub fn kv_step_names(profile: &DeploymentProfile) -> Result<Vec<String>, KvBootstrapError> {
    Ok(ordered_groups(profile)?.into_iter().map(step_name).collect())
}

fn ordered_groups(profile: &DeploymentProfile) -> Result<Vec<&GroupProfile>, KvBootstrapError> {
    let mut groups = profile.groups.iter().collect::<Vec<_>>();
    groups.sort_by_key(|group| group.group_id);
    if groups.first().map_or(true, |group| {
        group.store_id != 0 || group.group_id != 0 || group.role != GroupRole::System
    }) || groups.iter().any(|group| group.store_id != 0)
    {
        return Err(KvBootstrapError::Invalid(
            "KV bootstrap requires system group 0 in store 0",
        ));
    }
    Ok(groups)
}

fn step_name(group: &GroupProfile) -> String {
    format!("kv-group-{}-{}", group.store_id, group.group_id)
}

fn verify_group(actual: &GroupSummary, expected: &GroupProfile) -> Result<(), KvBootstrapError> {
    if actual.local_replica_id != expected.replica_id || actual.remote_count != 0 {
        return Err(KvBootstrapError::Invalid(
            "KV group identity or membership conflicts with manifest",
        ));
    }
    Ok(())
}
