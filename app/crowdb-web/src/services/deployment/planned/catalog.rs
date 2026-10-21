// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Only the accepted initial owner may initialize the shared Chunk-KV catalog.

use crowdb_protocol::mgmt::node::NodeBinding;

use crate::{
    error::{err_502, map_config_err},
    services::Failure,
    state::AppState,
};

pub(super) async fn configure(
    state: &AppState,
    binding: &NodeBinding,
    configuration: String,
) -> Result<String, Failure> {
    let ctx = state.op_context().await.map_err(map_config_err)?;
    let accepted = crowdb_console_shared::deployment::preparation::chunk_kv_binding(&ctx, &binding.bootstrap)
        .await
        .map_err(map_config_err)?;
    let mut config: toml::Value =
        toml::from_str(&configuration).map_err(|error| err_502(error.to_string()))?;
    if accepted.node_id == binding.node_id {
        let mut bootstrap = toml::Value::try_from(accepted).map_err(|error| err_502(error.to_string()))?;
        bootstrap
            .as_table_mut()
            .expect("bootstrap table")
            .remove("node_id");
        config
            .as_table_mut()
            .ok_or_else(|| err_502("Chunk-KV configuration must be a table"))?
            .insert("bootstrap_partition".into(), bootstrap);
    }
    toml::to_string_pretty(&config).map_err(|error| err_502(error.to_string()))
}
