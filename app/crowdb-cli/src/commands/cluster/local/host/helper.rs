// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Embed the same host validators used by direct OCI launchers.

use super::super::state::{self, invalid};
use crowdb_console_shared::error::Result;
use serde_json::Value;
use std::{
    fs,
    io::Write,
    os::unix::fs::DirBuilderExt,
    path::PathBuf,
    process::{Command, Stdio},
};

macro_rules! host_module {
    ($name:literal) => {
        (
            $name,
            include_str!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/../../container/oci-image/",
                $name
            )),
        )
    };
}

const MODULES: &[(&str, &str)] = &[
    host_module!("host_lifecycle.py"),
    host_module!("host_inputs.py"),
    host_module!("host_runtime.py"),
    (
        "runtime.py",
        include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../container/oci-image/runtime.py"
        )),
    ),
    (
        "run_node.py",
        include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../container/oci-image/run-node.py"
        )),
    ),
    (
        "storage.py",
        include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../container/oci-image/storage.py"
        )),
    ),
    (
        "storage_identity.py",
        include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../container/oci-image/storage_identity.py"
        )),
    ),
    (
        "builder.py",
        include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../container/oci-image/builder.py"
        )),
    ),
];

struct Modules(PathBuf);
impl Drop for Modules {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn execute(request: &Value) -> Result<Value> {
    let parent = crowdb_protocol::port::namespace::runtime_root().join("artifacts/cli/host-runtime");
    state::check_path(&parent)?;
    fs::DirBuilder::new()
        .recursive(true)
        .mode(0o700)
        .create(&parent)?;
    let directory = parent.join(uuid::Uuid::new_v4().to_string());
    fs::DirBuilder::new().mode(0o700).create(&directory)?;
    let modules = Modules(directory);
    for (name, source) in MODULES {
        fs::write(modules.0.join(name), source)?;
    }
    let mut child = Command::new("python3")
        .arg(modules.0.join("host_lifecycle.py"))
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|error| {
            invalid(&format!(
                "host runtime lifecycle requires Python 3.11 or newer: {error}"
            ))
        })?;
    child
        .stdin
        .take()
        .ok_or_else(|| invalid("host runtime helper has no input"))?
        .write_all(&serde_json::to_vec(request).map_err(|_| invalid("invalid host request"))?)?;
    let output = child.wait_with_output()?;
    if !output.status.success() {
        return Err(invalid(&String::from_utf8_lossy(&output.stderr)));
    }
    serde_json::from_slice(&output.stdout).map_err(|_| invalid("invalid host runtime helper response"))
}

pub(super) async fn invoke(request: Value) -> Result<Value> {
    tokio::task::spawn_blocking(move || execute(&request))
        .await
        .map_err(|error| invalid(&format!("host runtime helper failed: {error}")))?
}
