// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{verify_group, GroupProfile, KvBootstrap, KvBootstrapError};

impl KvBootstrap {
    pub(super) async fn confirm_system_acceptance(
        &self,
        group: &GroupProfile,
        bootstrap: crowdb_protocol::mgmt::SystemBootstrapIdentity,
    ) -> Result<(), KvBootstrapError> {
        let client = crowdb_console_shared::clients::http::ServerClient::new(self.base_url.as_str())?;
        let request = crowdb_protocol::mgmt::SystemPrepareRequest {
            replica_id: group.replica_id,
            bootstrap,
        };
        let accepted = client.system_prepare(&request).await?;
        if accepted.replica_id != request.replica_id || accepted.bootstrap != request.bootstrap {
            return Err(KvBootstrapError::Invalid(
                "system bootstrap acceptance differs from profile",
            ));
        }
        Ok(())
    }

    pub(super) async fn initialize_system(
        &self,
        group: &GroupProfile,
        bootstrap: crowdb_protocol::mgmt::SystemBootstrapIdentity,
    ) -> Result<(), KvBootstrapError> {
        use crowdb_console_shared::{
            config::ServerEntry,
            ops::{cluster::initialize_system_replicas, OpContext},
            ConsoleConfig,
        };
        if group.replica_id != 1 {
            return Err(KvBootstrapError::Invalid(
                "single startup requires initial system replica 1",
            ));
        }
        let mut config = ConsoleConfig::default();
        let mut server = ServerEntry::new("single-kv", self.base_url.to_string());
        server.node_id = Some(group.node_id);
        server.rpc_url = Some(group.rpc_endpoint.clone());
        config.servers.push(server);
        let context = OpContext::new(
            group.rpc_endpoint.clone(),
            vec![self.base_url.to_string()],
            config,
        );
        if let Err(error) = initialize_system_replicas(&context, &[group.node_id], &bootstrap).await {
            // A lost reply is proven by matching durable acceptance and actual
            // replica identity; never replay creation just because its reply was lost.
            self.confirm_system_acceptance(group, bootstrap).await?;
            let groups = self.list_groups().await?;
            let Some(existing) = groups.iter().find(|existing| existing.group_id == 0) else {
                return Err(error.into());
            };
            verify_group(existing, group)?;
        }
        Ok(())
    }
}
