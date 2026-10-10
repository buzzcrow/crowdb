// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::path::Path;

use super::durable;

pub(super) fn install(root: &Path, operation: &str, public_key: &str) -> std::io::Result<()> {
    let operation = uuid::Uuid::parse_str(operation).map_err(std::io::Error::other)?;
    if root.join(format!("ssh/cancelled-{operation}.json")).exists() {
        return Err(std::io::Error::other("admission operation cancelled"));
    }
    let key = ssh_key::PublicKey::from_openssh(public_key).map_err(std::io::Error::other)?;
    if key.algorithm() != ssh_key::Algorithm::Ed25519 {
        return Err(std::io::Error::other("node admission requires Ed25519"));
    }
    let path = root.join("ssh/authorized_keys");
    let mut text = read_keys(&path)?;
    let entry = format!(
        "{} crowdb:{operation}",
        key.to_openssh()
            .map_err(std::io::Error::other)?
            .split_whitespace()
            .take(2)
            .collect::<Vec<_>>()
            .join(" ")
    );
    if !text.lines().any(|line| line == entry) {
        if !text.is_empty() && !text.ends_with('\n') {
            text.push('\n');
        }
        text.push_str(&entry);
        text.push('\n');
        durable::write_bytes(&path, text.as_bytes())?;
    }
    Ok(())
}

pub(super) fn remove(root: &Path, operation: &str) -> std::io::Result<()> {
    let operation = uuid::Uuid::parse_str(operation).map_err(std::io::Error::other)?;
    durable::write(&root.join(format!("ssh/cancelled-{operation}.json")), &operation)?;
    let path = root.join("ssh/authorized_keys");
    let marker = format!("crowdb:{operation}");
    let retained = read_keys(&path)?
        .lines()
        .filter(|line| line.split_whitespace().last() != Some(marker.as_str()))
        .fold(String::new(), |mut text, line| {
            text.push_str(line);
            text.push('\n');
            text
        });
    durable::write_bytes(&path, retained.as_bytes())
}

fn read_keys(path: &Path) -> std::io::Result<String> {
    match std::fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_file() && metadata.len() <= 1024 * 1024 => {
            std::fs::read_to_string(path)
        }
        Ok(_) => Err(std::io::Error::other("invalid authorized_keys")),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(String::new()),
        Err(error) => Err(error),
    }
}

pub(super) fn host(root: &Path, host: &str, port: u16, public_key: &str) -> std::io::Result<()> {
    if host.is_empty()
        || host.len() > 256
        || host
            .bytes()
            .any(|byte| !byte.is_ascii_alphanumeric() && !b".:-".contains(&byte))
        || port == 0
    {
        return Err(std::io::Error::other("invalid SSH host"));
    }
    let key = ssh_key::PublicKey::from_openssh(public_key).map_err(std::io::Error::other)?;
    if key.algorithm() != ssh_key::Algorithm::Ed25519 {
        return Err(std::io::Error::other("Ed25519 host key required"));
    }
    let token = if port == 22 {
        host.to_owned()
    } else {
        format!("[{host}]:{port}")
    };
    let path = root.join("ssh/known_hosts");
    let mut text = read_keys(&path)?;
    let key = key
        .to_openssh()
        .map_err(std::io::Error::other)?
        .split_whitespace()
        .take(2)
        .collect::<Vec<_>>()
        .join(" ");
    let entry = format!("{token} {key}");
    if let Some(line) = text
        .lines()
        .find(|line| line.split_whitespace().next() == Some(token.as_str()))
    {
        if line != entry {
            return Err(std::io::Error::other("SSH host key changed"));
        }
        return Ok(());
    }
    if !text.is_empty() && !text.ends_with('\n') {
        text.push('\n');
    }
    text.push_str(&entry);
    text.push('\n');
    durable::write_bytes(&path, text.as_bytes())
}
