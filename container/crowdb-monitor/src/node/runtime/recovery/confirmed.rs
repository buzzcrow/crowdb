// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Current authority publishes non-voter pins; retained pins recover only existing routes.

use super::super::{durable, AcceptedNode, Result};
use crowdb_console_shared::{
    deployment::{
        recovery::{ConfirmedMemberTrust, MemberTrust, TrustedMember},
        registry, PreparedBootstrap,
    },
    ConsoleConfig,
};
use crowdb_kv_client::{ClientConfig, CrowdbKvClient, GetOutcome, ReadMode};
use crowdb_protocol::mgmt::node::NodeBinding;
use ssh_key::PublicKey;
use std::{path::Path, time::Duration};

async fn authority(
    root: &Path,
    accepted: &AcceptedNode,
) -> Result<(PreparedBootstrap, registry::NodeRegistry)> {
    let seeds = durable::read::<NodeBinding>(&root.join("node-binding.json"))?
        .filter(|binding| binding.bootstrap == accepted.bootstrap)
        .map(|binding| binding.management_seeds)
        .or_else(|| {
            accepted
                .admission
                .as_ref()
                .map(|grant| grant.management_seeds.clone())
        })
        .ok_or("confirmed recovery requires authority contacts")?;
    tokio::time::timeout(Duration::from_secs(3), async {
        let client = CrowdbKvClient::new(ClientConfig::new(seeds));
        let GetOutcome::Found { value, .. } = client
            .get(
                0,
                0,
                crowdb_console_shared::deployment::CLUSTER_KEY,
                ReadMode::Linearizable,
                None,
            )
            .await?
        else {
            return Err("cluster authority is absent".into());
        };
        let operation: PreparedBootstrap = serde_json::from_slice(&value)?;
        operation.validate()?;
        let (registry, _) = registry::read(&client)
            .await?
            .ok_or("confirmed node registry is absent")?;
        if operation.identity != accepted.bootstrap || registry.cluster_id != accepted.bootstrap.cluster_id {
            return Err("confirmed recovery authority belongs to another bootstrap".into());
        }
        Ok((operation, registry))
    })
    .await?
}

fn own_member(
    root: &Path,
    accepted: &AcceptedNode,
    registry: &registry::NodeRegistry,
) -> Result<TrustedMember> {
    let operation = &accepted
        .admission
        .as_ref()
        .ok_or("node has no admission grant")?
        .operation_id;
    let discovery = crate::NodeIdentity::load_existing(root)?.uuid().to_string();
    let node = registry
        .nodes
        .iter()
        .find(|node| {
            node.node_id == accepted.node_id
                && node.discovery_id == discovery
                && node.operation_id == *operation
                && !node.cancelled
        })
        .ok_or("local admission is not current in confirmed authority")?;
    let host_key = node
        .host_key
        .as_ref()
        .ok_or("admission has no authenticated public key")?;
    let local = PublicKey::from_openssh(&std::fs::read_to_string(
        root.join("ssh/ssh_host_ed25519_key.pub"),
    )?)?;
    if local.key_data() != PublicKey::from_openssh(host_key)?.key_data() {
        return Err("confirmed admission key differs from this node".into());
    }
    Ok(TrustedMember {
        node_id: node.node_id,
        discovery_id: discovery,
        admission_operation_id: operation.clone(),
        host_key: host_key.clone(),
    })
}

pub(in crate::node::runtime) async fn prepare_admitted(root: &Path, accepted: &AcceptedNode) -> Result<()> {
    if accepted.admission.is_none() {
        return Ok(());
    }
    let (operation, registry) = authority(root, accepted).await?;
    let initial = MemberTrust::from_bootstrap(&operation)?;
    let mut admitted = initial.clone();
    admitted.members.push(own_member(root, accepted, &registry)?);
    admitted.validate()?;
    let path = root.join("prepared-bootstrap.json");
    if let Some(previous) = durable::read::<PreparedBootstrap>(&path)? {
        if previous != operation {
            return Err("retained recovery manifest differs".into());
        }
    } else {
        durable::write(&path, &operation)?;
    }
    initial.seal(&root.join("member-trust.json"))?;
    admitted.seal(&admitted_path(root, accepted)?)?;
    Ok(())
}

pub(super) fn admitted_path(root: &Path, accepted: &AcceptedNode) -> Result<std::path::PathBuf> {
    let grant = accepted.admission.as_ref().ok_or("node has no admission grant")?;
    let operation = uuid::Uuid::parse_str(&grant.operation_id)?;
    Ok(root.join(format!("admitted-member-trust-{operation}.json")))
}

pub(super) fn admitted(root: &Path, accepted: &AcceptedNode) -> Result<MemberTrust> {
    let operation = PreparedBootstrap::load(&root.join("prepared-bootstrap.json"))?;
    let initial = MemberTrust::load(&root.join("member-trust.json"))?;
    initial.validate_bootstrap(&operation)?;
    let admitted = MemberTrust::load(&admitted_path(root, accepted)?)?;
    if admitted.bootstrap != accepted.bootstrap
        || admitted.members.len() != initial.members.len() + 1
        || initial
            .members
            .iter()
            .any(|member| !admitted.members.contains(member))
    {
        return Err("admitted recovery pins differ from initial authority".into());
    }
    Ok(admitted)
}

pub(super) fn snapshot(root: &Path, operation: &PreparedBootstrap) -> Result<Option<ConfirmedMemberTrust>> {
    let snapshot = durable::read::<ConfirmedMemberTrust>(&root.join("confirmed-member-trust.json"))?;
    if let Some(snapshot) = &snapshot {
        snapshot.validate(operation)?;
        if snapshot.initial != MemberTrust::load(&root.join("member-trust.json"))? {
            return Err("confirmed recovery snapshot changes initial pins".into());
        }
    }
    Ok(snapshot)
}

fn store(
    root: &Path,
    accepted: &AcceptedNode,
    operation: &PreparedBootstrap,
    snapshot: &ConfirmedMemberTrust,
) -> Result<()> {
    snapshot.validate(operation)?;
    if snapshot.initial != MemberTrust::load(&root.join("member-trust.json"))? {
        return Err("confirmed recovery snapshot changes initial pins".into());
    }
    if accepted.admission.is_some() {
        let own = own_member(root, accepted, &snapshot.registry)?;
        if !snapshot.current.members.contains(&own) {
            return Err("local admission is not confirmed for recovery".into());
        }
        let path = root.join("service-address.json");
        if !path.exists() {
            let node = snapshot
                .registry
                .nodes
                .iter()
                .find(|node| node.node_id == accepted.node_id)
                .ok_or("local confirmed address is absent")?;
            durable::write(&path, &node.host)?;
        }
    }
    let path = root.join("confirmed-member-trust.json");
    let previous = self::snapshot(root, operation)?;
    if previous.as_ref() != Some(snapshot) {
        durable::write(&path, snapshot)?;
    }
    Ok(())
}

pub(in crate::node::runtime) async fn accept(root: &Path, snapshot: &ConfirmedMemberTrust) -> Result<()> {
    let accepted: AcceptedNode =
        durable::read(&root.join("accepted-node.json"))?.ok_or("node not accepted")?;
    if super::super::admission::is_cancelled(root, &accepted)
        || root
            .join(format!(
                "retired-bootstrap-{}.json",
                accepted.bootstrap.operation_id
            ))
            .exists()
    {
        return Err("member recovery is retired".into());
    }
    let (operation, current) = authority(root, &accepted).await?;
    if current != snapshot.registry {
        return Err("confirmed recovery registry was superseded".into());
    }
    store(root, &accepted, &operation, snapshot)
}

pub(super) async fn refresh(root: &Path, accepted: &AcceptedNode) -> Result<()> {
    let (operation, registry) = authority(root, accepted).await?;
    if let Some(grant) = &accepted.admission {
        if snapshot(root, &operation)?.is_some()
            && !registry.nodes.iter().any(|node| {
                node.node_id == accepted.node_id
                    && node.operation_id == grant.operation_id
                    && node.confirmed
                    && !node.cancelled
            })
        {
            durable::write(
                &root.join(format!("ssh/cancelled-{}.json", grant.operation_id)),
                grant,
            )?;
            return Err("confirmed authority retired this admission".into());
        }
    }
    let initial = MemberTrust::load(&root.join("member-trust.json"))?;
    let snapshot = ConfirmedMemberTrust::new(initial, registry)?;
    store(root, accepted, &operation, &snapshot)
}

pub(super) fn contacts(root: &Path, operation: &PreparedBootstrap) -> Result<(MemberTrust, ConsoleConfig)> {
    if let Some(snapshot) = snapshot(root, operation)? {
        let config = snapshot.topology(operation)?;
        Ok((snapshot.current, config))
    } else {
        let initial = MemberTrust::load(&root.join("member-trust.json"))?;
        initial.validate_bootstrap(operation)?;
        Ok((initial, operation.intent.to_config()))
    }
}
