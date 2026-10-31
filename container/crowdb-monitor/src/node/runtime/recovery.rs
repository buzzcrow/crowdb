// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Seal authenticated member pins before starting bootstrap replicas.

use std::path::Path;

use crowdb_console_shared::deployment::{
    recovery::{MemberTrust, TrustedMember},
    PreparedBootstrap,
};
use ssh_key::PublicKey;

use super::{durable, AcceptedNode, Result};
use crate::NodeIdentity;
mod confirmed;
mod discovery;
mod owners;
mod publication;
pub(super) use confirmed::{accept as accept_confirmed, prepare_admitted};
pub(super) use discovery::start;
#[cfg(feature = "test-util")]
pub use discovery::{member_recovery_destinations_for_tests, unique_member_endpoints_for_tests};

pub(super) fn publish_local(root: &Path, addresses: &[std::net::IpAddr]) -> Result<()> {
    durable::write(
        &root.join("node-endpoint.json"),
        &crowdb_protocol::mgmt::node::recovery::MemberEndpoint {
            discovery_id: NodeIdentity::load_existing(root)?.uuid().to_string(),
            node_id: 0,
            boot_id: uuid::Uuid::new_v4().to_string(),
            addresses: addresses.to_vec(),
        },
    )?;
    Ok(())
}

pub(super) fn identity(root: &Path) -> Result<crowdb_protocol::mgmt::node::recovery::MemberRecoveryIdentity> {
    use crowdb_protocol::mgmt::node::{
        recovery::{MemberEndpoint, MemberRecoveryIdentity},
        NodeBinding,
    };
    let accepted: AcceptedNode =
        durable::read(&root.join("accepted-node.json"))?.ok_or("node is not prepared")?;
    check(root, &accepted)?;
    if accepted.admission.is_some() && !ready(root, &accepted)? {
        return Err("member admission is not confirmed for address recovery".into());
    }
    if super::admission::is_cancelled(root, &accepted)
        || root
            .join(format!(
                "retired-bootstrap-{}.json",
                accepted.bootstrap.operation_id
            ))
            .exists()
    {
        return Err("member identity is retired".into());
    }
    let mut endpoint: MemberEndpoint =
        durable::read(&root.join("node-endpoint.json"))?.ok_or("node endpoint is absent")?;
    endpoint.node_id = accepted.node_id;
    if endpoint.discovery_id != NodeIdentity::load_existing(root)?.uuid().to_string() {
        return Err("local endpoint identity differs".into());
    }
    let binding = durable::read(&root.join("node-binding.json"))?.unwrap_or(NodeBinding {
        node_id: accepted.node_id,
        bootstrap: accepted.bootstrap.clone(),
        management_seeds: Vec::new(),
    });
    Ok(MemberRecoveryIdentity {
        version: 1,
        binding,
        endpoint,
    })
}

pub(super) fn capture(root: &Path, operation: &PreparedBootstrap) -> Result<MemberTrust> {
    operation.validate()?;
    let local = NodeIdentity::load_existing(root)?.uuid().to_string();
    let members = operation
        .nodes
        .iter()
        .map(|node| {
            let key = if node.discovery_id == local {
                std::fs::read_to_string(root.join("ssh/ssh_host_ed25519_key.pub"))?
            } else {
                super::trust::pinned_host(root, &node.host, node.ssh_port)?
            };
            Ok(TrustedMember {
                discovery_id: node.discovery_id.clone(),
                node_id: node.node_id,
                admission_operation_id: node.operation_id.clone(),
                host_key: PublicKey::new(PublicKey::from_openssh(&key)?.key_data().clone(), "")
                    .to_openssh()?,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let trust = MemberTrust {
        version: 1,
        bootstrap: operation.identity.clone(),
        members,
    };
    trust.validate_bootstrap(operation)?;
    Ok(trust)
}

pub(super) fn accept(root: &Path, trust: &MemberTrust) -> Result<()> {
    let accepted: AcceptedNode =
        durable::read(&root.join("accepted-node.json"))?.ok_or("member trust requires accepted bootstrap")?;
    if !accepted.prepared
        || accepted.admission.is_some()
        || super::admission::is_cancelled(root, &accepted)
        || root
            .join(format!(
                "retired-bootstrap-{}.json",
                accepted.bootstrap.operation_id
            ))
            .exists()
    {
        return Err("member trust requires active accepted bootstrap preparation".into());
    }
    let operation = PreparedBootstrap::load(&root.join("prepared-bootstrap.json"))?;
    trust.validate_bootstrap(&operation)?;
    let local = NodeIdentity::load_existing(root)?.uuid().to_string();
    if trust.bootstrap != accepted.bootstrap
        || !trust
            .members
            .iter()
            .any(|member| member.node_id == accepted.node_id && member.discovery_id == local)
    {
        return Err("member trust differs from local acceptance".into());
    }
    // Every pin must agree with the keys established by mutual SSH admission,
    // including this node's own host key. The coordinator cannot replace them.
    let expected = capture(root, &operation)?;
    if *trust != expected {
        return Err("member trust differs from authenticated admission keys".into());
    }
    trust.seal(&root.join("member-trust.json"))?;
    let group = crowdb_protocol::mgmt::node::recovery::RecoveryGroup {
        store_id: 0,
        group_id: 0,
        deployment: None,
        members: operation
            .intent
            .members()
            .iter()
            .enumerate()
            .map(
                |(index, node)| crowdb_protocol::mgmt::node::recovery::RecoveryReplica {
                    replica_id: index as u64 + 1,
                    node_id: *node,
                },
            )
            .collect(),
    };
    if !root.join("recovery-groups.json").exists() {
        durable::write(
            &root.join("recovery-groups.json"),
            &crowdb_protocol::mgmt::node::recovery::RecoveryGroups {
                bootstrap: operation.identity,
                groups: vec![group],
            },
        )?;
    }
    Ok(())
}

pub(super) fn check(root: &Path, accepted: &AcceptedNode) -> Result<()> {
    if super::admission::is_cancelled(root, accepted)
        || root
            .join(format!(
                "retired-bootstrap-{}.json",
                accepted.bootstrap.operation_id
            ))
            .exists()
    {
        return Err("member recovery is retired".into());
    }
    let trust = if accepted.admission.is_some() {
        confirmed::admitted(root, accepted)?
    } else {
        MemberTrust::load(&root.join("member-trust.json"))?
    };
    let operation = PreparedBootstrap::load(&root.join("prepared-bootstrap.json"))?;
    if accepted.admission.is_none() {
        trust.validate_bootstrap(&operation)?;
    }
    let local = NodeIdentity::load_existing(root)?.uuid().to_string();
    let member = trust
        .members
        .iter()
        .find(|member| member.node_id == accepted.node_id && member.discovery_id == local)
        .ok_or("retained member trust differs from local identity")?;
    let current = PublicKey::from_openssh(&std::fs::read_to_string(
        root.join("ssh/ssh_host_ed25519_key.pub"),
    )?)?;
    if trust.bootstrap != accepted.bootstrap
        || accepted
            .admission
            .as_ref()
            .is_some_and(|grant| member.admission_operation_id != grant.operation_id)
        || current.key_data() != PublicKey::from_openssh(&member.host_key)?.key_data()
    {
        return Err("retained member trust differs from local identity".into());
    }
    Ok(())
}

pub(super) fn ready(root: &Path, accepted: &AcceptedNode) -> Result<bool> {
    if accepted.admission.is_some() {
        for path in [
            root.join("prepared-bootstrap.json"),
            root.join("member-trust.json"),
            confirmed::admitted_path(root, accepted)?,
            root.join("confirmed-member-trust.json"),
        ] {
            match std::fs::symlink_metadata(path) {
                Ok(_) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
                Err(error) => return Err(error.into()),
            }
        }
        check(root, accepted)?;
        let operation = PreparedBootstrap::load(&root.join("prepared-bootstrap.json"))?;
        let admitted = confirmed::admitted(root, accepted)?;
        let own = admitted
            .members
            .iter()
            .find(|member| member.node_id == accepted.node_id)
            .ok_or("local admitted pin is absent")?;
        return Ok(confirmed::snapshot(root, &operation)?
            .is_some_and(|snapshot| snapshot.current.members.contains(own)));
    }
    match std::fs::symlink_metadata(root.join("member-trust.json")) {
        Ok(_) => {
            check(root, accepted)?;
            Ok(true)
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(error.into()),
    }
}

#[cfg(feature = "test-util")]
/// # Errors
/// Propagates the real acceptance, key validation and persistence failures.
pub fn accept_member_trust_for_tests(root: &Path, trust: &MemberTrust) -> Result<()> {
    accept(root, trust)
}

#[cfg(feature = "test-util")]
/// # Errors
/// Propagates the real retained identity validation failures.
pub fn check_member_trust_for_tests(root: &Path) -> Result<()> {
    let accepted = durable::read(&root.join("accepted-node.json"))?.ok_or("missing accepted node")?;
    check(root, &accepted)
}

#[cfg(feature = "test-util")]
/// # Errors
/// Uses the production retained-trust admission fence without starting services.
pub fn member_trust_ready_for_tests(root: &Path) -> Result<bool> {
    let accepted = durable::read(&root.join("accepted-node.json"))?.ok_or("missing accepted node")?;
    ready(root, &accepted)
}

#[cfg(feature = "test-util")]
pub use publication::{apply_member_endpoints_for_tests, rebind_recovery_services_for_tests};

/// Read a live monitor's retained identity without waiting for lifecycle control.
///
/// # Errors
/// Rejects an inactive monitor, changed keys, missing trust and retired identity.
pub async fn read_recovery_identity(
    root: &Path,
) -> Result<crowdb_protocol::mgmt::node::recovery::MemberRecoveryIdentity> {
    crate::probe_liveness(root).await?;
    identity(root)
}

#[cfg(feature = "test-util")]
pub use owners::capture_recovery_owners_for_tests;
