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
    Ok(())
}

pub(super) fn check(root: &Path, accepted: &AcceptedNode) -> Result<()> {
    if accepted.admission.is_some() {
        return Ok(());
    }
    let trust = MemberTrust::load(&root.join("member-trust.json"))?;
    let operation = PreparedBootstrap::load(&root.join("prepared-bootstrap.json"))?;
    trust.validate_bootstrap(&operation)?;
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
        || current.key_data() != PublicKey::from_openssh(&member.host_key)?.key_data()
    {
        return Err("retained member trust differs from local identity".into());
    }
    Ok(())
}

pub(super) fn ready(root: &Path, accepted: &AcceptedNode) -> Result<bool> {
    if accepted.admission.is_some() {
        return Ok(true);
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
