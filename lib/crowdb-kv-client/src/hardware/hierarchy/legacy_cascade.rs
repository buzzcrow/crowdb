// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Compatibility cleanup is fenced against concurrent retained-media admission.

use std::collections::BTreeSet;

use crowdb_protocol::key::{
    BindMapKey, DiskGroupKey, DiskGroupUsageKey, DiskKey, NodeKey, OwnerMapKey, RackKey, TextKey,
};

use super::{mutation, HardwareClient};
use crate::{BatchOp, Error, Result};

pub(super) enum Scope {
    Rack,
    Node(u64),
    Group(u64, u64),
}

impl HardwareClient {
    pub(super) async fn legacy_cascade(&self, rack: u64, scope: Scope) -> Result<()> {
        let rack_path = RackKey { rack_id: rack }.to_path();
        let snapshot = mutation::read(&self.kv, &rack_path).await?;
        let (node, group) = match scope {
            Scope::Rack => (None, None),
            Scope::Node(node) => (Some(node), None),
            Scope::Group(node, group) => (Some(node), Some(group)),
        };
        if self.has_retained_disk_storage(rack, node, group).await? {
            return Err(Error::Server(
                "retained disks block parent cascade removal".into(),
            ));
        }
        let Some((rack_bytes, revision)) = snapshot else {
            return Err(Error::Server("hardware rack does not exist".into()));
        };
        let mut paths = BTreeSet::new();
        let nodes = if let Some(node) = node {
            vec![node]
        } else {
            self.list_nodes_in_rack(rack)
                .await?
                .into_iter()
                .map(|(node, _)| node)
                .collect()
        };
        for node in nodes {
            let groups = if let Some(group) = group {
                vec![group]
            } else {
                self.list_disk_groups_on_node(rack, node)
                    .await?
                    .into_iter()
                    .map(|group| group.dg_id)
                    .collect()
            };
            for group in groups {
                self.legacy_group_paths(rack, node, group, &mut paths).await?;
            }
            if group.is_none() {
                paths.insert(
                    NodeKey {
                        rack_id: rack,
                        node_id: node,
                    }
                    .to_path(),
                );
            }
        }
        let remove_rack = matches!(scope, Scope::Rack);
        if remove_rack {
            paths.insert(rack_path.clone());
        }
        if paths.len() > 4096 {
            return Err(Error::Server("legacy cascade exceeds mutation bound".into()));
        }
        let mut operations: Vec<_> = paths
            .into_iter()
            .map(|path| BatchOp::Delete {
                key: path.into_bytes().into(),
            })
            .collect();
        if !remove_rack {
            operations.push(BatchOp::Put {
                key: rack_path.as_bytes().to_vec().into(),
                value: rack_bytes,
            });
        }
        self.kv
            .batch_write_cas(0, 0, &operations, rack_path.as_bytes(), revision)
            .await?;
        Ok(())
    }

    async fn legacy_group_paths(
        &self,
        rack_id: u64,
        node_id: u64,
        disk_group_id: u64,
        paths: &mut BTreeSet<String>,
    ) -> Result<()> {
        for (disk_id, _) in self.list_disks_in_group(rack_id, node_id, disk_group_id).await? {
            paths.insert(
                DiskKey {
                    rack_id,
                    node_id,
                    disk_group_id,
                    disk_id,
                }
                .to_path(),
            );
        }
        paths.insert(
            DiskGroupKey {
                rack_id,
                node_id,
                disk_group_id,
            }
            .to_path(),
        );
        paths.insert(
            OwnerMapKey {
                rack_id,
                node_id,
                disk_group_id,
            }
            .to_path(),
        );
        paths.insert(
            BindMapKey {
                rack_id,
                node_id,
                disk_group_id,
            }
            .to_path(),
        );
        paths.insert(DiskGroupUsageKey { disk_group_id }.to_path());
        Ok(())
    }
}
