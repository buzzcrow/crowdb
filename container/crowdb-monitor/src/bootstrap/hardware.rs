use std::collections::BTreeSet;

use crowdb_kv_client::{ClientConfig, CrowdbKvClient, HardwareClient};
use crowdb_protocol::common::{DiskId, HwStatus, NodeValue, RackValue};
use crowdb_protocol::diskdb::rpc::{DiskGroupValue, DiskType, DiskValue};
use thiserror::Error;

use crate::{
    BootstrapSession, DeploymentProfile, ManifestError, MonitorEvent, MonitorEventKind, MonitorLog,
    MonitorLogError,
};

const STEP: &str = "hardware-topology";
const UNIT_BYTES: u64 = 1024 * 1024;

#[derive(Debug, Error)]
pub enum HardwareBootstrapError {
    #[error("Group 0 hardware request failed: {0}")]
    Client(#[from] crowdb_kv_client::Error),
    #[error("bootstrap manifest failed: {0}")]
    Manifest(#[from] ManifestError),
    #[error("monitor lifecycle log failed: {0}")]
    MonitorLog(#[from] MonitorLogError),
    #[error("hardware bootstrap state is invalid: {0}")]
    Invalid(&'static str),
}

struct ExpectedHardware {
    rack_id: u64,
    node_id: u64,
    group_id: u64,
    rack: RackValue,
    node: NodeValue,
    group: DiskGroupValue,
    disks: Vec<(DiskId, DiskValue)>,
}

#[derive(Default)]
struct ExistingHardware {
    rack: bool,
    node: bool,
    group: bool,
    disks: BTreeSet<(u64, u64)>,
}

pub struct HardwareBootstrap {
    client: HardwareClient,
}

impl HardwareBootstrap {
    #[must_use]
    pub fn new(management_seed: String) -> Self {
        let kv = CrowdbKvClient::new(ClientConfig::new(vec![management_seed]));
        Self {
            client: HardwareClient::new(kv),
        }
    }

    /// # Errors
    /// Refuses unknown or conflicting Group 0 hardware before any mutation.
    pub async fn reconcile(
        &self,
        session: &mut BootstrapSession,
        profile: &DeploymentProfile,
        events: &mut MonitorLog,
    ) -> Result<(), HardwareBootstrapError> {
        let result = self.reconcile_inner(session, profile, events).await;
        if result.is_err() {
            events
                .record(&MonitorEvent {
                    kind: MonitorEventKind::BootstrapFailed,
                    service: Some(STEP),
                    pid: None,
                    attempt: None,
                })
                .await?;
        }
        result
    }

    async fn reconcile_inner(
        &self,
        session: &mut BootstrapSession,
        profile: &DeploymentProfile,
        events: &mut MonitorLog,
    ) -> Result<(), HardwareBootstrapError> {
        let expected = expected(profile)?;
        let complete = session
            .manifest()
            .step_complete(STEP)
            .ok_or(HardwareBootstrapError::Invalid(
                "hardware step is absent from manifest",
            ))?;
        self.client.kv().refresh_topology().await?;
        let found = self.preflight(&expected).await?;
        if complete
            && (!found.rack || !found.node || !found.group || found.disks.len() != expected.disks.len())
        {
            return Err(HardwareBootstrapError::Invalid(
                "completed hardware topology is incomplete",
            ));
        }
        if complete {
            return Ok(());
        }
        if session.manifest().next_step() != Some(STEP) {
            return Err(HardwareBootstrapError::Invalid("hardware step is out of order"));
        }
        record(events, MonitorEventKind::BootstrapStepStarted).await?;
        if !found.rack {
            let write = self.client.add_rack(expected.rack_id, &expected.rack).await;
            let actual = self.client.get_rack(expected.rack_id).await?;
            verify_written(write, actual.is_some_and(|value| rack_matches(&value, &expected)))?;
        }
        if !found.node {
            let write = self
                .client
                .add_node(expected.rack_id, expected.node_id, &expected.node)
                .await;
            let actual = self.client.get_node(expected.rack_id, expected.node_id).await?;
            verify_written(write, actual.is_some_and(|value| node_matches(&value, &expected)))?;
        }
        if !found.group {
            let write = self
                .client
                .add_disk_group(
                    expected.rack_id,
                    expected.node_id,
                    expected.group_id,
                    &expected.group,
                )
                .await;
            let actual = self
                .client
                .get_disk_group(expected.rack_id, expected.node_id, expected.group_id)
                .await?;
            verify_written(
                write,
                actual.is_some_and(|value| group_matches(&value.value, &expected)),
            )?;
        }
        for (disk_id, disk_value) in &expected.disks {
            if found.disks.contains(&(disk_id.high, disk_id.low)) {
                continue;
            }
            let write = self
                .client
                .add_disk(
                    expected.rack_id,
                    expected.node_id,
                    expected.group_id,
                    disk_id,
                    disk_value,
                )
                .await;
            let actual = self
                .client
                .get_disk(expected.rack_id, expected.node_id, expected.group_id, disk_id)
                .await?;
            verify_written(
                write,
                actual.is_some_and(|value| disk_matches(&value, disk_value)),
            )?;
        }
        self.preflight(&expected).await?;
        session.complete_step(STEP)?;
        record(events, MonitorEventKind::BootstrapStepCompleted).await?;
        Ok(())
    }

    async fn preflight(
        &self,
        expected: &ExpectedHardware,
    ) -> Result<ExistingHardware, HardwareBootstrapError> {
        let mut found = ExistingHardware::default();
        for (rack_id, value) in self.client.list_racks().await? {
            if rack_id != expected.rack_id || !rack_matches(&value, expected) {
                return Err(HardwareBootstrapError::Invalid(
                    "Group 0 rack conflicts with profile",
                ));
            }
            found.rack = true;
        }
        for (rack_id, node_id, value) in self.client.list_nodes().await? {
            if rack_id != expected.rack_id || node_id != expected.node_id || !node_matches(&value, expected) {
                return Err(HardwareBootstrapError::Invalid(
                    "Group 0 node conflicts with profile",
                ));
            }
            found.node = true;
        }
        for entry in self.client.list_disk_groups().await? {
            if entry.rack_id != expected.rack_id
                || entry.node_id != expected.node_id
                || entry.dg_id != expected.group_id
                || !group_matches(&entry.value, expected)
            {
                return Err(HardwareBootstrapError::Invalid(
                    "Group 0 disk group conflicts with profile",
                ));
            }
            found.group = true;
        }
        for entry in self.client.list_all_disks().await? {
            let matching = expected
                .disks
                .iter()
                .find(|(disk_id, _)| *disk_id == entry.disk_id);
            if entry.rack_id != expected.rack_id
                || entry.node_id != expected.node_id
                || entry.disk_group_id != expected.group_id
                || !matching.is_some_and(|(_, value)| disk_matches(&entry.value, value))
            {
                return Err(HardwareBootstrapError::Invalid(
                    "Group 0 disk conflicts with profile",
                ));
            }
            found.disks.insert((entry.disk_id.high, entry.disk_id.low));
        }
        Ok(found)
    }
}

#[must_use]
pub fn hardware_step_names() -> Vec<String> {
    vec![STEP.to_owned()]
}

fn expected(profile: &DeploymentProfile) -> Result<ExpectedHardware, HardwareBootstrapError> {
    profile
        .validate()
        .map_err(|_| HardwareBootstrapError::Invalid("deployment profile is invalid"))?;
    let [node] = profile.nodes.as_slice() else {
        return Err(HardwareBootstrapError::Invalid(
            "preview requires one hardware node",
        ));
    };
    let Some(first_disk) = profile.disks.first() else {
        return Err(HardwareBootstrapError::Invalid("preview has no disks"));
    };
    if profile.disks.iter().any(|disk| {
        disk.node_id != node.node_id
            || disk.disk_group_id != first_disk.disk_group_id
            || disk.capacity_bytes != disk.zone_size_bytes
            || disk.capacity_bytes % UNIT_BYTES != 0
    }) {
        return Err(HardwareBootstrapError::Invalid(
            "preview disk layout is incompatible",
        ));
    }
    let mut disks = profile
        .disks
        .iter()
        .map(|disk| {
            let path = disk
                .path
                .to_str()
                .ok_or(HardwareBootstrapError::Invalid("disk path is not UTF-8"))?;
            Ok((
                parse_disk_id(&disk.disk_id)?,
                DiskValue {
                    disk_type: DiskType::BlockSsd as i32,
                    capacity_units: disk.capacity_bytes / UNIT_BYTES,
                    zone_size_units: disk.zone_size_bytes / UNIT_BYTES,
                    unit_size_bytes: u32::try_from(UNIT_BYTES).expect("1 MiB fits into u32"),
                    zone_count: 1,
                    status: HwStatus::Up as i32,
                    device_path: path.to_owned(),
                },
            ))
        })
        .collect::<Result<Vec<_>, HardwareBootstrapError>>()?;
    disks.sort_by_key(|(id, _)| (id.high, id.low));
    let disk_ids = disks.iter().map(|(id, _)| *id).collect();
    Ok(ExpectedHardware {
        rack_id: node.rack_id,
        node_id: node.node_id,
        group_id: first_disk.disk_group_id,
        rack: RackValue {
            status: HwStatus::Up as i32,
            node_ids: vec![node.node_id],
        },
        node: NodeValue {
            status: HwStatus::Up as i32,
            last_used_dg_id: 0,
            disk_group_ids: vec![first_disk.disk_group_id],
            status_changed_at_ms: 0,
            temp_failure_since_ms: None,
        },
        group: DiskGroupValue {
            status: HwStatus::Up as i32,
            disk_ids,
        },
        disks,
    })
}

fn rack_matches(actual: &RackValue, expected: &ExpectedHardware) -> bool {
    actual.node_ids == expected.rack.node_ids
}

fn node_matches(actual: &NodeValue, expected: &ExpectedHardware) -> bool {
    actual.disk_group_ids == expected.node.disk_group_ids
}

fn group_matches(actual: &DiskGroupValue, expected: &ExpectedHardware) -> bool {
    actual.disk_ids == expected.group.disk_ids
}

fn disk_matches(actual: &DiskValue, expected: &DiskValue) -> bool {
    actual.disk_type == expected.disk_type
        && actual.capacity_units == expected.capacity_units
        && actual.zone_size_units == expected.zone_size_units
        && actual.unit_size_bytes == expected.unit_size_bytes
        && actual.zone_count == expected.zone_count
        && actual.device_path == expected.device_path
}

fn verify_written(
    write: Result<(), crowdb_kv_client::Error>,
    visible: bool,
) -> Result<(), HardwareBootstrapError> {
    if visible {
        return Ok(());
    }
    write?;
    Err(HardwareBootstrapError::Invalid(
        "Group 0 hardware write is not visible",
    ))
}

async fn record(events: &mut MonitorLog, kind: MonitorEventKind) -> Result<(), MonitorLogError> {
    events
        .record(&MonitorEvent {
            kind,
            service: Some(STEP),
            pid: None,
            attempt: None,
        })
        .await
}

fn parse_disk_id(value: &str) -> Result<DiskId, HardwareBootstrapError> {
    if value.len() != 32 || !value.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(HardwareBootstrapError::Invalid("disk ID is not 32 hex digits"));
    }
    let high = u64::from_str_radix(&value[..16], 16)
        .map_err(|_| HardwareBootstrapError::Invalid("disk ID is invalid"))?;
    let low = u64::from_str_radix(&value[16..], 16)
        .map_err(|_| HardwareBootstrapError::Invalid("disk ID is invalid"))?;
    Ok(DiskId { high, low })
}
