// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! A successful batch response means every membership was committed together.

use std::collections::HashSet;

use super::{
    authority, fenced, node_location, optional, pause, put, rack_revision, required, validate_disk_input,
    AddDiskInput, DiskEntry, DiskGroupKey, DiskGroupValue, DiskKey, DiskValue, Error, KvError, OpContext,
    Result, TextKey, RETRIES,
};

/// Publish a bounded batch under one hierarchy revision, with identical replay.
///
/// # Errors
/// Rejects invalid inputs, duplicate identities, missing parents, retained media
/// and conflicting records. No rejected batch creates partial membership.
pub async fn add_disks_to_group0(
    ctx: &OpContext,
    node_id: u64,
    group_id: u64,
    inputs: &[AddDiskInput],
) -> Result<Vec<DiskEntry>> {
    if inputs.is_empty() || inputs.len() > 256 {
        return Err(Error::Validation {
            field: "disks".into(),
            message: "disk batch requires 1 to 256 entries".into(),
        });
    }
    authority::ready(ctx).await?;
    let rack_id = node_location(&ctx.sysmd().list_nodes().await?, node_id)?;
    let mut seen = HashSet::new();
    let mut entries = Vec::with_capacity(inputs.len());
    for input in inputs {
        let entry = validate_disk_input(input, group_id, rack_id, node_id)?;
        if !seen.insert((entry.1.high, entry.1.low)) {
            return Err(Error::Conflict {
                kind: "duplicate disk identity in batch".into(),
                id: input.disk_id.clone(),
            });
        }
        entries.push(entry);
    }
    let group_path = DiskGroupKey {
        rack_id,
        node_id,
        disk_group_id: group_id,
    }
    .to_path();
    for (_, disk_id, _) in &entries {
        ctx.sysmd()
            .reserve_disk_id(&DiskKey {
                rack_id,
                node_id,
                disk_group_id: group_id,
                disk_id: *disk_id,
            })
            .await?;
    }
    for attempt in 0..RETRIES {
        let (rack_path, rack, revision) = rack_revision(ctx, rack_id).await?;
        let (mut group, _) =
            required::<DiskGroupValue>(ctx, &group_path, "disk_group", group_id.to_string()).await?;
        let mut operations = Vec::with_capacity(entries.len() + 1);
        for (entry, disk_id, value) in &entries {
            let key = DiskKey {
                rack_id,
                node_id,
                disk_group_id: group_id,
                disk_id: *disk_id,
            };
            let path = key.to_path();
            if ctx.sysmd().disk_storage(&key).await?.is_some() {
                return Err(Error::Conflict {
                    kind: "retained disk requires storage admission or recovery".into(),
                    id: entry.disk_id.clone(),
                });
            }
            match optional::<DiskValue>(ctx, &path).await? {
                Some(actual) if actual == *value && group.disk_ids.contains(disk_id) => continue,
                Some(_) => {
                    return Err(Error::Conflict {
                        kind: "disk".into(),
                        id: entry.disk_id.clone(),
                    })
                }
                None if group.disk_ids.contains(disk_id) => {
                    return Err(Error::Conflict {
                        kind: "disk membership".into(),
                        id: entry.disk_id.clone(),
                    })
                }
                None => {}
            }
            group.disk_ids.push(*disk_id);
            operations.push(put(&path, value)?);
        }
        if operations.is_empty() {
            return Ok(entries.into_iter().map(|(entry, _, _)| entry).collect());
        }
        group.disk_ids.sort_unstable_by_key(|id| (id.high, id.low));
        operations.push(put(&group_path, &group)?);
        if fenced(ctx, &rack_path, &rack, revision, operations).await? {
            return Ok(entries.into_iter().map(|(entry, _, _)| entry).collect());
        }
        pause(attempt).await;
    }
    Err(KvError::OutcomeUnknown.into())
}
