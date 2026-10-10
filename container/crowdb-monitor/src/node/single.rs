// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{serve_node_management, NodeRuntimeConfig};

/// Run the same node discovery beside the automatic disk/topology startup policy.
///
/// # Errors
/// Propagates bootstrap, discovery and persistent identity failures.
pub async fn run_single_node(
    config: NodeRuntimeConfig,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let profile = crate::DeploymentProfile::load(&config.profile)?;
    super::NodeIdentity::load_or_create(&profile.paths.data_root)?;
    let (stop, stopped) = tokio::sync::oneshot::channel();
    let root = profile.paths.data_root;
    let mut discovery = tokio::spawn(async move {
        serve_node_management(
            &root,
            config.bind,
            &config.discovery,
            config.physical_host_id,
            async {
                let _ = stopped.await;
            },
        )
        .await
    });
    let result = tokio::select! {
        result = crate::run_preview(&config.profile) => result.map_err(|error| error.to_string().into()),
        result = &mut discovery => { result??; Err("node discovery unexpectedly stopped".into()) },
    };
    let _ = stop.send(());
    if !discovery.is_finished() {
        discovery.await??;
    }
    result
}
