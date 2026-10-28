// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Sticky physical resources arbitrate admissions across Rack CAS domains.

use super::{mutation, HardwareClient};
use crate::{Error, GetOutcome, ReadMode, Result};
use crowdb_protocol::{
    disk_storage::{DiskStorageRecord, PhysicalBacking},
    key::{DiskKey, TextKey},
};
use serde::{Deserialize, Serialize};

mod authority;
mod legacy;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Reservation {
    version: u32,
    placement: String,
    operation_id: String,
    backing_identity: String,
    physical_backing: PhysicalBacking,
}

impl HardwareClient {
    /// Reserve a verified physical resource at one immutable disk placement.
    /// A reservation alone grants neither membership nor permission to write.
    /// It survives interrupted admission and explicit Remove; retries reuse it.
    ///
    /// # Errors
    /// Rejects mismatched node authority, aliases, changed requests and incomplete
    /// legacy verification. Version-1 bootstrap remains a trusted compatibility API.
    pub async fn reserve_disk_resource(&self, record: &DiskStorageRecord) -> Result<()> {
        record.validate().map_err(|error| Error::Server(error.into()))?;
        let Some(backing) = &record.physical_backing else {
            return Ok(());
        };
        let hosts = authority::hosts(&self.kv, record).await?;
        let key = DiskKey {
            rack_id: record.rack_id,
            node_id: record.node_id,
            disk_group_id: record.disk_group_id,
            disk_id: record.disk_id,
        };
        self.reserve_disk_id(&key).await?;
        let path = resource_key(backing);
        let expected = Reservation {
            version: 1,
            placement: key.to_path(),
            operation_id: record.operation_id.clone(),
            backing_identity: record.backing_identity.clone(),
            physical_backing: backing.clone(),
        };
        for _ in 0..10 {
            if let GetOutcome::Found { value, .. } = self
                .kv
                .get(0, 0, path.as_bytes(), ReadMode::Linearizable, None)
                .await?
            {
                if value.len() > 16384 {
                    return Err(Error::Server(
                        "physical resource reservation exceeds bound".into(),
                    ));
                }
                let actual: Reservation = mutation::decode(&path, &value)?;
                if actual != expected {
                    return Err(Error::Server("physical resource is reserved by another disk or operation; resume its original operation".into()));
                }
                return Ok(());
            }
            legacy::verify(self, record, &hosts).await?;
            let bytes = serde_json::to_vec(&expected).map_err(|error| Error::Server(error.to_string()))?;
            match self.kv.put_cas(0, 0, path.as_bytes(), &bytes, 0).await {
                Ok(_) => return Ok(()),
                Err(Error::CasFailed { .. } | Error::CasBusy | Error::OutcomeUnknown) => {}
                Err(error) => return Err(error),
            }
        }
        Err(Error::OutcomeUnknown)
    }
}

fn resource_key(backing: &PhysicalBacking) -> String {
    fn encode(value: &str) -> String {
        const HEX: &[u8; 16] = b"0123456789abcdef";
        value
            .bytes()
            .flat_map(|byte| {
                [
                    char::from(HEX[usize::from(byte >> 4)]),
                    char::from(HEX[usize::from(byte & 15)]),
                ]
            })
            .collect()
    }
    format!(
        "/hw/disk-resource/{}/{}",
        encode(&backing.physical_host_id),
        encode(&backing.resource_identity)
    )
}
