// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Operator-only diskdb ownership health monitor.

use std::time::{SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use crowdb_protocol::chunk_kv::{DomainFailurePolicy, DomainMonitorDescriptor};
use crowdb_protocol::common::InstanceValue;
use crowdb_protocol::key::InstanceKey;
use tracing::warn;

use crate::group0_control_plane::Group0ControlPlane;

use super::{DomainMonitorDriver, DomainMonitorFuture};

pub struct DiskdbOwnershipMonitorDriver;

impl DiskdbOwnershipMonitorDriver {
    #[must_use]
    pub fn new() -> Self {
        Self
    }
}

impl Default for DiskdbOwnershipMonitorDriver {
    fn default() -> Self {
        Self::new()
    }
}

impl DomainMonitorDriver for DiskdbOwnershipMonitorDriver {
    fn domain(&self) -> &'static str {
        "diskdb"
    }

    fn driver_version(&self) -> u32 {
        1
    }

    fn tick<'a>(
        &'a self,
        control: &'a Group0ControlPlane,
        descriptor: &'a DomainMonitorDescriptor,
    ) -> DomainMonitorFuture<'a> {
        Box::pin(async move {
            if descriptor.failure_policy != DomainFailurePolicy::OperatorOnly {
                return Err("diskdb monitor requires operator-only failure policy".into());
            }
            let prefix = InstanceKey::text_prefix_for_service(&descriptor.service_registry_name);
            let observations = control
                .scan_all_prefix(Bytes::from(prefix), 256)
                .await
                .map_err(|error| error.to_string())?;
            let dead_before = wall_time_ms().saturating_sub(descriptor.dead_after_ms);
            for observation in observations {
                let instance: InstanceValue =
                    serde_json::from_slice(&observation.value).map_err(|error| error.to_string())?;
                if instance.last_heartbeat_ms < dead_before {
                    warn!(
                        instance_id = instance.instance_id,
                        "diskdb owner is dead; operator action is required"
                    );
                }
            }
            Ok(())
        })
    }
}

fn wall_time_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| {
            u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
        })
}
