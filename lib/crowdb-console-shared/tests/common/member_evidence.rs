// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_console_shared::{
    bootstrap_intent::BootstrapIntent,
    config::{NodeEntry, RackEntry, ServerEntry},
    deployment::{registry::NodeRecord, PreparedBootstrap},
    ops::cluster::readiness::member_evidence,
    ConsoleConfig,
};
use crowdb_protocol::mgmt::{
    cluster::{DependencyKind, DependencyObservation, DependencyState},
    node::{
        deployment::NodeDeploymentPolicy,
        observation::{NodeRuntimeObservation, NodeServiceObservation},
        NodeBinding, NodeServiceAction, NodeServiceIntent,
    },
    SystemBootstrapIdentity,
};

pub struct TestMemberEvidence {
    pub bootstrap: PreparedBootstrap,
    pub expected: NodeRecord,
    pub policy: NodeDeploymentPolicy,
    pub intents: Vec<NodeServiceIntent>,
    pub node: NodeRuntimeObservation,
}

impl TestMemberEvidence {
    pub fn new() -> Self {
        let identity = SystemBootstrapIdentity {
            cluster_id: uuid::Uuid::new_v4().to_string(),
            operation_id: uuid::Uuid::new_v4().to_string(),
            configuration_digest: "accepted-digest".into(),
        };
        let expected = NodeRecord {
            web_port: 9090,
            discovery_id: uuid::Uuid::new_v4().to_string(),
            node_id: 1,
            physical_host_id: "test-host".into(),
            rack_id: 1,
            host: "node-one".into(),
            ssh_port: 22,
            ssh_user: "crowdb".into(),
            operation_id: uuid::Uuid::new_v4().to_string(),
            confirmed: true,
            host_key: None,
            cancelled: false,
        };
        let config = Self::config();
        let bootstrap = PreparedBootstrap {
            identity: identity.clone(),
            intent: BootstrapIntent::capture(&config, &[1]).unwrap(),
            nodes: vec![expected.clone()],
        };
        let policy = NodeDeploymentPolicy::default();
        let intents: Vec<_> = ["diskdb", "diskio", "chunkdb", "chunk-kv", "access"]
            .into_iter()
            .map(|kind| NodeServiceIntent {
                cluster_id: identity.cluster_id.clone(),
                node_id: 1,
                service_id: format!("{kind}-1"),
                kind: kind.into(),
                operation_id: uuid::Uuid::new_v4().to_string(),
                action: NodeServiceAction::Start,
                configuration: String::new(),
                environment: std::collections::BTreeMap::new(),
            })
            .collect();
        let mut services: Vec<_> = intents
            .iter()
            .map(|intent| NodeServiceObservation {
                detail: "Service process and readiness match the current executed operation".into(),
                service_id: intent.service_id.clone(),
                kind: intent.kind.clone(),
                operation_id: Some(intent.operation_id.clone()),
                state: DependencyState::Ready,
                observed_at_unix_ms: 1000,
                max_age_ms: 3000,
                reason_code: None,
            })
            .collect();
        services.push(NodeServiceObservation {
            detail: "KV has current supervised process health".into(),
            service_id: "kv".into(),
            kind: "kv".into(),
            operation_id: None,
            state: DependencyState::Ready,
            observed_at_unix_ms: 1000,
            max_age_ms: 3000,
            reason_code: None,
        });
        let operation = intents.last().unwrap().operation_id.clone();
        let protocols = ["s3", "iceberg", "dataset"]
            .into_iter()
            .map(|protocol| DependencyObservation {
                id: format!("node/1/protocol/{protocol}"),
                kind: DependencyKind::Protocol,
                detail: format!("{protocol} authenticated request succeeded"),
                required: true,
                state: DependencyState::Ready,
                observed_at_unix_ms: 1000,
                max_age_ms: 3000,
                revision: None,
                operation_id: Some(operation.clone()),
                current_operation_id: Some(operation.clone()),
                reason: None,
            })
            .collect();
        let node = NodeRuntimeObservation {
            version: 1,
            discovery_id: expected.discovery_id.clone(),
            policy: policy.clone(),
            binding: Some(NodeBinding {
                node_id: 1,
                bootstrap: identity,
                management_seeds: vec!["http://node-one:10000".into()],
            }),
            setup: None,
            observed_at_unix_ms: 1000,
            services,
            protocols,
        };
        Self {
            bootstrap,
            expected,
            policy,
            intents,
            node,
        }
    }

    fn config() -> ConsoleConfig {
        let mut server = ServerEntry::new("kv-1", "http://node-one:10000");
        server.node_id = Some(1);
        server.rpc_url = Some("node-one:10100".into());
        ConsoleConfig {
            racks: vec![RackEntry {
                id: 1,
                name: "local".into(),
            }],
            nodes: vec![NodeEntry {
                id: 1,
                rack_id: 1,
                host: "node-one".into(),
                ssh_port: 22,
                ssh_user: "crowdb".into(),
                ssh_key: None,
                ssh_password: None,
                ssh_credential_ref: None,
            }],
            servers: vec![server],
            ..Default::default()
        }
    }

    pub fn project(&self) -> Vec<DependencyObservation> {
        member_evidence(
            &self.bootstrap,
            &self.expected,
            &self.policy,
            Some(&self.intents),
            Some(&self.node),
            1000,
        )
    }
}
