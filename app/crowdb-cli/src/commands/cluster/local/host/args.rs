// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use clap::{Args, ValueEnum};
use serde::Serialize;
use std::{net::IpAddr, path::PathBuf};

#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum, Serialize)]
#[serde(rename_all = "kebab-case")]
pub(super) enum Runtime {
    Docker,
    Containerd,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum, Serialize)]
#[serde(rename_all = "kebab-case")]
pub(super) enum Network {
    Bridge,
    Host,
}

#[derive(Args, Debug, Serialize)]
pub(crate) struct HostArgs {
    #[arg(long, value_enum)]
    pub(super) runtime: Option<Runtime>,
    #[arg(long, value_enum)]
    pub(super) network: Option<Network>,
    /// Local Unix runtime socket; accepted with the daemon identity.
    #[arg(long)]
    runtime_endpoint: Option<String>,
    /// Containerd snapshotter; host mode only.
    #[arg(long, value_parser = ["overlayfs", "native"])]
    snapshotter: Option<String>,
    #[arg(long)]
    data_root: Option<PathBuf>,
    #[arg(long)]
    physical_host_id: Option<String>,
    #[arg(long)]
    interface: Option<String>,
    /// This host's management IP on the selected interface; port 9090.
    #[arg(long)]
    management_address: Option<IpAddr>,
    #[arg(long)]
    password_file: Option<PathBuf>,
    /// Explicit production node policy, including supplied media and public URIs.
    #[arg(long)]
    node_policy_file: Option<PathBuf>,
    #[arg(long)]
    device: Vec<PathBuf>,
    #[arg(long)]
    disk_file: Vec<PathBuf>,
    #[arg(long)]
    seed: Vec<String>,
}

impl HostArgs {
    pub(super) fn has_host_inputs(&self) -> bool {
        self.runtime_endpoint.is_some()
            || self.snapshotter.is_some()
            || self.data_root.is_some()
            || self.physical_host_id.is_some()
            || self.interface.is_some()
            || self.management_address.is_some()
            || self.password_file.is_some()
            || self.node_policy_file.is_some()
            || !self.device.is_empty()
            || !self.disk_file.is_empty()
            || !self.seed.is_empty()
    }
}
