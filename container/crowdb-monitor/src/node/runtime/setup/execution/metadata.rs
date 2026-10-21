// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Default metadata precedes both disk admission and planned service startup.

use std::sync::Arc;

use crowdb_console_shared::{
    deployment::preparation,
    ops::{kv_logical, OpContext},
    ConsoleConfig,
};
use crowdb_kv_client::CrowdbKvClient;
use crowdb_protocol::mgmt::node::NodeBinding;

use super::Result;

pub(super) async fn ready(binding: &NodeBinding, kv: &Arc<CrowdbKvClient>) -> Result<bool> {
    let ctx = OpContext::with_shared_client_preserving_hint(
        Arc::clone(kv),
        &binding.management_seeds,
        ConsoleConfig::default(),
    );
    let accepted = preparation::reserve(&ctx, &binding.bootstrap).await?;
    Ok(kv_logical::ensure_default_group(&ctx, &accepted).await?)
}
