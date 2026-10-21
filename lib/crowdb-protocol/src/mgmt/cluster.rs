// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Observational cluster readiness; these facts never authorize deployment.

use serde::{Deserialize, Serialize};

/// Confirmed application identity and native management seeds for remote attach.
/// Reading this record grants no ownership of the hosting runtime resources.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClusterTargetIdentity {
    pub version: u32,
    pub cluster_id: String,
    pub group0_management_endpoints: Vec<String>,
    pub group0_rpc_endpoints: Vec<String>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ClusterPhase {
    Waiting,
    Initializing,
    Ready,
    Degraded,
    Unavailable,
    Failed,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum DependencyKind {
    Identity,
    Authority,
    Metadata,
    Storage,
    Member,
    Service,
    Protocol,
    Runtime,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum DependencyState {
    Ready,
    Waiting,
    Initializing,
    Stopped,
    /// A validated terminal failure, not a probe or transport timeout.
    Failed,
    Unknown,
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReadinessReason {
    pub dependency: String,
    pub code: String,
    /// Operator-facing explanation without credential contents.
    pub detail: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DependencyObservation {
    pub id: String,
    pub kind: DependencyKind,
    pub required: bool,
    pub state: DependencyState,
    pub observed_at_unix_ms: u64,
    pub max_age_ms: u64,
    pub revision: Option<u64>,
    /// Operation associated with this observation, if applicable.
    pub operation_id: Option<String>,
    /// Current committed operation; a superseded observation is never current health.
    pub current_operation_id: Option<String>,
    pub reason: Option<ReadinessReason>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClusterMemberObservation {
    pub discovery_id: String,
    pub node_id: Option<u64>,
    pub management_endpoints: Vec<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RuntimeObservation {
    pub runtime: String,
    pub endpoint: String,
    pub source: String,
    pub image_digest: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClusterObservation {
    pub version: u32,
    pub cluster: String,
    /// Unknown until bootstrap identity is authoritatively observed.
    pub cluster_id: Option<String>,
    /// A confirmed binding, not merely a UUID reserved by an in-progress bootstrap.
    pub bound: bool,
    pub initial_voters: u8,
    pub policy: Option<super::node::deployment::NodeDeploymentPolicy>,
    pub observed_at_unix_ms: u64,
    pub members: Vec<ClusterMemberObservation>,
    pub operation_id: Option<String>,
    pub work_accepted: bool,
    /// Historical setup completion, distinct from the dependency facts below.
    pub setup_completed: bool,
    pub dependencies: Vec<DependencyObservation>,
    /// Remote checks may have application facts without any local runtime access.
    pub runtime: Option<RuntimeObservation>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClusterStatus {
    pub phase: ClusterPhase,
    pub reasons: Vec<ReadinessReason>,
    pub observation: ClusterObservation,
}
