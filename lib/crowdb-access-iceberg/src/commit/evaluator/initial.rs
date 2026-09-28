use std::collections::BTreeSet;

use serde_json::json;
use sha2::{Digest, Sha256};

use super::{
    raw, validate_limits, CommitRequest, EvaluatedMetadata, EvaluationError, EvaluationLimits, State,
    TableUpdate,
};
use crate::{
    commit::{validate_requirements, TableRequirement},
    table::{TableHead, TableLifecycle, TableMetadataDocument, TableMetadataError as Error},
};

/// Applies an assert-create update list to an empty builder, without reassigning staged field IDs.
/// The result is structural metadata, not a file proof, name reservation or publication authority.
/// # Errors
/// Rejects incomplete initialization, non-create requirements, foreign UUID/location and resource limits.
pub fn evaluate_table_create_commit(
    request: &CommitRequest,
    mut target: TableHead,
    timestamp_ms: i64,
    limits: EvaluationLimits,
) -> Result<EvaluatedMetadata, EvaluationError> {
    validate_limits(request, limits)?;
    if request.requirements.is_empty()
        || request
            .requirements
            .iter()
            .any(|requirement| *requirement != TableRequirement::AssertCreate)
    {
        return Err(Error::Field("create-requirements").into());
    }
    validate_requirements(&request.requirements, None, limits.requirements)?;
    target.validate().map_err(|_| Error::Binding)?;
    if target.generation != 1
        || target.operation_fence != 1
        || target.name_epoch != 1
        || target.pending_operation.is_none()
        || target.lifecycle != TableLifecycle::Ready
        || target.table_uuid.is_none()
        || timestamp_ms < 0
    {
        return Err(Error::Binding.into());
    }
    let mut state = empty(request, &target, timestamp_ms, limits)?;
    state.apply_all(request)?;
    state.raw.set("last-updated-ms", &timestamp_ms)?;
    state.snapshot_log()?;
    state.legacy()?;
    let bytes = state.raw.finish()?;
    target.format_version = state.raw.get("format-version")?;
    let uuid: String = state.raw.get("table-uuid")?;
    if uuid::Uuid::parse_str(&uuid).ok() != target.table_uuid {
        return Err(Error::Binding.into());
    }
    target.metadata_digest = Sha256::digest(&bytes).into();
    let document = TableMetadataDocument::parse(bytes, &target, limits.metadata)?;
    document.parquet_field_mapping(limits.metadata, limits.metadata.values)?;
    Ok(EvaluatedMetadata {
        document,
        head: target,
        upgrades: state.upgrades,
    })
}

fn empty(
    request: &CommitRequest,
    target: &TableHead,
    timestamp_ms: i64,
    limits: EvaluationLimits,
) -> Result<State, Error> {
    let version = request
        .updates
        .iter()
        .find_map(|update| match update {
            TableUpdate::UpgradeFormatVersion { format_version } => Some(*format_version),
            _ => None,
        })
        .unwrap_or(2);
    if !(1..=3).contains(&version) {
        return Err(Error::Field("format-version"));
    }
    let mut root = json!({
        "format-version":version,"last-updated-ms":timestamp_ms,"last-column-id":0,
        "schemas":[],"current-schema-id":-1,"partition-specs":[],"default-spec-id":-1,
        "last-partition-id":999,"sort-orders":[],"default-sort-order-id":-1,"properties":{},
        "current-snapshot-id":-1,"snapshots":[],"refs":{},"statistics":[],
        "partition-statistics":[],"snapshot-log":[],"metadata-log":[]
    });
    if version > 1 {
        root["last-sequence-number"] = json!(0);
    }
    if version == 3 {
        root["next-row-id"] = json!(0);
        root["current-snapshot-id"] = serde_json::Value::Null;
    }
    let encoded = raw::encode(&root, limits.metadata.bytes)?;
    Ok(State {
        raw: raw::Document::new(encoded.get().as_bytes(), limits.metadata.bytes, limits.work_bytes)?,
        limits: limits.metadata,
        upgrades: Vec::new(),
        last_schema: None,
        added_schemas: BTreeSet::new(),
        last_spec: None,
        added_specs: BTreeSet::new(),
        last_order: None,
        added_orders: BTreeSet::new(),
        added_snapshots: BTreeSet::new(),
        changed_main: BTreeSet::new(),
        removed_snapshots: false,
        timestamp_ms,
        source_head: target.clone(),
    })
}
