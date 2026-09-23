use serde_json::Value;

use crate::table::{TableLifecycle, TableMetadataDocument, TableMetadataError as Error};

mod definitions;
mod snapshots;

#[derive(Clone, Copy, Debug)]
pub struct TransitionLimits {
    pub entries: usize,
    pub upgrade_steps: usize,
}

/// Checks identity, explicit version transitions, high-water marks and retained definitions/snapshots.
/// `upgrades` is the evaluator's ordered list of applied upgrade operations, not inferred history.
/// This does not replace ordered schema/layout update checks, file validation or publication CAS.
/// # Errors
/// Rejects foreign/changed lifecycle identity, generation overflow, invalid upgrades,
/// decreasing high-water marks, changed retained snapshots and excessive work.
pub fn validate_metadata_transition(
    prior: &TableMetadataDocument,
    candidate: &TableMetadataDocument,
    upgrades: &[u8],
    limits: TransitionLimits,
) -> Result<(), Error> {
    if limits.entries == 0
        || limits.entries > 100_000
        || limits.upgrade_steps == 0
        || limits.upgrade_steps > 1000
        || upgrades.len() > limits.upgrade_steps
    {
        return Err(Error::Bounds);
    }
    identity(prior, candidate)?;
    let mut version = prior.selected_head().format_version;
    for target in upgrades {
        if *target < version || *target > version + 1 || *target > 3 {
            return Err(Error::Field("format-version"));
        }
        version = *target;
    }
    if version != candidate.selected_head().format_version {
        return Err(Error::Field("format-version"));
    }
    for field in [
        "last-column-id",
        "last-partition-id",
        "last-sequence-number",
        "next-row-id",
    ] {
        if number(candidate, field) < number(prior, field) {
            return Err(Error::Field(field));
        }
    }
    let mut work = limits.entries;
    definitions::validate(prior, candidate, &mut work)?;
    snapshots::validate(prior, candidate, &mut work)
}

fn identity(prior: &TableMetadataDocument, candidate: &TableMetadataDocument) -> Result<(), Error> {
    let before = prior.selected_head();
    let after = candidate.selected_head();
    if before.catalog != after.catalog
        || before.table != after.table
        || before.namespace != after.namespace
        || before.name != after.name
        || before.name_epoch != after.name_epoch
        || before.lifecycle != TableLifecycle::Ready
        || after.lifecycle != TableLifecycle::Ready
        || before.generation.checked_add(1) != Some(after.generation)
        || before
            .table_uuid
            .is_some_and(|uuid| after.table_uuid != Some(uuid))
        || before.metadata_file == after.metadata_file
        || before.metadata_location == after.metadata_location
        || after.operation_fence < before.operation_fence
    {
        return Err(Error::Binding);
    }
    Ok(())
}

pub(super) fn number(document: &TableMetadataDocument, name: &str) -> i64 {
    let version = document.selected_head().format_version;
    if (name == "last-sequence-number" && version == 1) || (name == "next-row-id" && version < 3) {
        return 0;
    }
    if name == "last-partition-id" && !document.fields().contains_key(name) {
        let fields = document.fields();
        let specs = fields.get("partition-specs").and_then(Value::as_array);
        return specs.map_or_else(
            || {
                fields
                    .get("partition-spec")
                    .and_then(Value::as_array)
                    .map_or(999, |fields| partition_max(fields))
            },
            |specs| {
                specs
                    .iter()
                    .filter_map(|spec| spec["fields"].as_array())
                    .map(|fields| partition_max(fields))
                    .max()
                    .unwrap_or(999)
            },
        );
    }
    document.fields().get(name).and_then(Value::as_i64).unwrap_or(0)
}

fn partition_max(fields: &[Value]) -> i64 {
    fields
        .iter()
        .enumerate()
        .map(|(index, field)| {
            field["field-id"]
                .as_i64()
                .unwrap_or(1000 + i64::try_from(index).unwrap_or(i64::MAX - 1000))
        })
        .max()
        .unwrap_or(999)
}

fn charge(work: &mut usize) -> Result<(), Error> {
    *work = work.checked_sub(1).ok_or(Error::Bounds)?;
    Ok(())
}
