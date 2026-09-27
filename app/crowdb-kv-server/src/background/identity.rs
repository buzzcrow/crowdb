// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Stable service registration identity owned by one durable node root.

use std::io::{self, Write};
use std::path::Path;

use crowdb_protocol::common::KvServerIdentity;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct StoredIdentity {
    version: u32,
    instance_id: u64,
    node_id: Option<u64>,
}

/// Load or durably create a registration identity before the server starts.
///
/// # Errors
/// Rejects malformed state, changed explicit identities, and persistence errors.
pub fn load_or_create(
    root: &Path,
    requested: Option<u64>,
    node_id: Option<u64>,
) -> io::Result<KvServerIdentity> {
    std::fs::create_dir_all(root)?;
    let path = root.join("service-identity.json");
    match std::fs::read(&path) {
        Ok(body) => return decode(&body, requested, node_id),
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => return Err(error),
    }
    let generated = crowdb_kv_client::new_client_id();
    let stored = StoredIdentity {
        version: 1,
        instance_id: requested.unwrap_or(generated),
        node_id,
    };
    let body = serde_json::to_vec(&stored)?;
    let identity = decode(&body, requested, node_id)?;
    let temporary = root.join(format!(
        ".service-identity-{}-{generated}.tmp",
        std::process::id()
    ));
    let result = (|| {
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temporary)?;
        file.write_all(&body)?;
        file.sync_all()?;
        // Publish a complete file without replacing another creator's identity.
        match std::fs::hard_link(&temporary, &path) {
            Ok(()) => {
                std::fs::File::open(root)?.sync_all()?;
                Ok(identity)
            }
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
                decode(&std::fs::read(&path)?, requested, node_id)
            }
            Err(error) => Err(error),
        }
    })();
    let _ = std::fs::remove_file(temporary);
    result
}

fn decode(body: &[u8], requested: Option<u64>, node_id: Option<u64>) -> io::Result<KvServerIdentity> {
    let stored: StoredIdentity = serde_json::from_slice(body)?;
    if stored.version != 1
        || stored.instance_id == 0
        || stored.node_id != node_id
        || requested.is_some_and(|id| id != stored.instance_id)
    {
        return Err(io::Error::other(
            "service registration identity does not match this node",
        ));
    }
    Ok(KvServerIdentity {
        instance_id: stored.instance_id,
        node_id: stored.node_id,
    })
}
