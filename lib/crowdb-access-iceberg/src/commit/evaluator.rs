use std::collections::BTreeSet;

use serde_json::{json, value::RawValue, Value};
use sha2::{Digest, Sha256};

use super::{
    validate_requirements, CommitRequest, MetadataObject, RequirementError, RequirementLimits, TableUpdate,
};
use crate::table::{TableHead, TableMetadataDocument, TableMetadataError, TableMetadataLimits};

mod auxiliary;
mod definitions;
mod layout;
mod raw;
mod scalar;
mod snapshots;

use raw::Document;

#[derive(Clone, Copy, Debug)]
pub struct EvaluationLimits {
    pub metadata: TableMetadataLimits,
    pub requirements: RequirementLimits,
    pub updates: usize,
    pub work_bytes: usize,
}

#[derive(Debug, thiserror::Error)]
pub enum EvaluationError {
    #[error(transparent)]
    Requirement(#[from] RequirementError),
    #[error(transparent)]
    Metadata(#[from] TableMetadataError),
    #[error("update at index {0} is not enabled for candidate evaluation")]
    Unsupported(usize),
}

/// A pure, structurally validated candidate, not a file-validation or publication proof.
#[derive(Debug)]
pub struct EvaluatedMetadata {
    pub document: TableMetadataDocument,
    pub head: TableHead,
    pub upgrades: Vec<u8>,
}

/// Applies enabled updates in request order against one selected input document.
/// Unsupported updates fail the entire evaluation without writing any storage.
/// # Errors
/// Rejects requirements, malformed updates, unsupported actions and independent resource limits.
pub fn evaluate_metadata_updates(
    prior: &TableMetadataDocument,
    request: &CommitRequest,
    mut target: TableHead,
    timestamp_ms: i64,
    limits: EvaluationLimits,
) -> Result<EvaluatedMetadata, EvaluationError> {
    limits.metadata.validate()?;
    if limits.updates == 0
        || limits.updates > 1000
        || request.updates.len() > limits.updates
        || limits.work_bytes == 0
        || limits.work_bytes > 256 * 1024 * 1024
    {
        return Err(TableMetadataError::Bounds.into());
    }
    validate_requirements(&request.requirements, Some(prior), limits.requirements)?;
    let mut state = State::new(prior, limits, timestamp_ms)?;
    for (index, update) in request.updates.iter().enumerate() {
        update.validate_parameters()?;
        if !state.apply(update)? {
            return Err(EvaluationError::Unsupported(index));
        }
        let bytes = state.raw.finish()?;
        crate::table::decode_bounded_json(&bytes, limits.metadata)?;
    }
    state.raw.set("last-updated-ms", &timestamp_ms)?;
    state.history(prior)?;
    state.snapshot_log()?;
    state.legacy()?;
    let bytes = state.raw.finish()?;
    target.metadata_digest = Sha256::digest(&bytes).into();
    target.format_version = state.raw.get("format-version")?;
    target.table_uuid = state
        .raw
        .fields
        .get("table-uuid")
        .map(|raw| {
            let value: Option<String> = serde_json::from_str(raw.get())?;
            value
                .as_deref()
                .map(uuid::Uuid::parse_str)
                .transpose()
                .map_err(|_| TableMetadataError::Field("table-uuid"))
        })
        .transpose()?
        .flatten();
    let document = TableMetadataDocument::parse(bytes, &target, limits.metadata)?;
    document.parquet_field_mapping(limits.metadata, limits.metadata.values)?;
    super::validate_metadata_transition(
        prior,
        &document,
        &state.upgrades,
        super::TransitionLimits {
            entries: limits.metadata.values.min(100_000),
            upgrade_steps: limits.updates,
        },
    )?;
    Ok(EvaluatedMetadata {
        document,
        head: target,
        upgrades: state.upgrades,
    })
}

struct State {
    raw: Document,
    limits: TableMetadataLimits,
    upgrades: Vec<u8>,
    last_schema: Option<i32>,
    added_schemas: BTreeSet<i32>,
    last_spec: Option<i32>,
    added_specs: BTreeSet<i32>,
    last_order: Option<i32>,
    added_orders: BTreeSet<i32>,
    added_snapshots: BTreeSet<i64>,
    changed_main: BTreeSet<i64>,
    removed_snapshots: bool,
    timestamp_ms: i64,
    source_head: TableHead,
}

impl State {
    fn new(
        prior: &TableMetadataDocument,
        limits: EvaluationLimits,
        timestamp_ms: i64,
    ) -> Result<Self, TableMetadataError> {
        let mut state = Self {
            raw: Document::new(prior.canonical(), limits.metadata.bytes, limits.work_bytes)?,
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
            source_head: prior.selected_head().clone(),
        };
        state.normalize(prior)?;
        Ok(state)
    }

    fn apply(&mut self, update: &TableUpdate) -> Result<bool, TableMetadataError> {
        match update {
            TableUpdate::AddSchema { schema, .. } => self.add_schema(schema)?,
            TableUpdate::SetCurrentSchema { schema_id } => self.select_schema(*schema_id)?,
            TableUpdate::RemoveSchemas { schema_ids } => self.remove_schemas(schema_ids)?,
            TableUpdate::AddSpec { spec } => self.add_layout(spec, true)?,
            TableUpdate::SetDefaultSpec { spec_id } => self.select_layout(*spec_id, true)?,
            TableUpdate::AddSortOrder { sort_order } => self.add_layout(sort_order, false)?,
            TableUpdate::SetDefaultSortOrder { sort_order_id } => {
                self.select_layout(*sort_order_id, false)?;
            }
            TableUpdate::RemovePartitionSpecs { spec_ids } => self.remove_specs(spec_ids)?,
            _ => return self.scalar(update),
        }
        Ok(true)
    }

    fn normalize(&mut self, prior: &TableMetadataDocument) -> Result<(), TableMetadataError> {
        if !self.raw.fields.contains_key("current-snapshot-id") {
            self.raw.set("current-snapshot-id", &prior.current_snapshot())?;
        }
        if !self.raw.fields.contains_key("schemas") {
            let mut schema: raw::Object = self.raw.get("schema")?;
            schema
                .entry("schema-id".into())
                .or_insert(raw::encode(&0, self.raw.limit)?);
            self.raw.set("current-schema-id", &schema["schema-id"])?;
            self.raw.set("schemas", &[schema])?;
        }
        if !self.raw.fields.contains_key("partition-specs") {
            let mut fields: Vec<raw::Object> = self.raw.get("partition-spec")?;
            for (index, field) in fields.iter_mut().enumerate() {
                field
                    .entry("field-id".into())
                    .or_insert(raw::encode(&(1000 + index), self.raw.limit)?);
            }
            let fields = raw::encode(&fields, self.raw.limit)?;
            let spec = raw::Object::from([
                ("spec-id".into(), raw::encode(&0, self.raw.limit)?),
                ("fields".into(), fields),
            ]);
            self.raw.set("partition-specs", &[spec])?;
            self.raw.set("default-spec-id", &0)?;
        }
        if !self.raw.fields.contains_key("sort-orders") {
            self.raw
                .set("sort-orders", &json!([{"order-id":0,"fields":[]}]))?;
            self.raw.set("default-sort-order-id", &0)?;
        }
        self.raw.set(
            "last-partition-id",
            &super::transition::number(prior, "last-partition-id"),
        )
    }

    fn legacy(&mut self) -> Result<(), TableMetadataError> {
        if self.raw.get::<u8>("format-version")? == 1 {
            let schema = self.current_schema()?;
            self.raw.set("schema", &schema)?;
            let selected: i32 = self.raw.get("default-spec-id")?;
            let specs = self.raw.array("partition-specs")?;
            for spec in specs {
                let value: raw::Object = serde_json::from_str(spec.get())?;
                if serde_json::from_str::<i32>(value["spec-id"].get())? == selected {
                    self.raw.set("partition-spec", &value["fields"])?;
                }
            }
        } else {
            self.raw.fields.remove("schema");
            self.raw.fields.remove("partition-spec");
        }
        Ok(())
    }

    fn payload(&mut self, object: &MetadataObject) -> Result<Box<RawValue>, TableMetadataError> {
        let raw = match object.canonical() {
            Some(text) => {
                if text.len() > self.raw.limit {
                    return Err(TableMetadataError::Bounds);
                }
                self.raw.charge(text.len())?;
                RawValue::from_string(text.to_owned())?
            }
            None => raw::encode(object.fields(), self.raw.limit)?,
        };
        self.raw.charge(raw.get().len())?;
        crate::table::decode_bounded_json(raw.get().as_bytes(), self.limits)?;
        Ok(raw)
    }

    fn history(&mut self, prior: &TableMetadataDocument) -> Result<(), TableMetadataError> {
        let properties: raw::Object = self.raw.object("properties")?;
        let keep = properties
            .get("write.metadata.previous-versions-max")
            .map(|value| serde_json::from_str::<String>(value.get()))
            .transpose()?
            .map(|value| value.parse::<i32>())
            .transpose()
            .map_err(|_| TableMetadataError::Field("write.metadata.previous-versions-max"))?
            .unwrap_or(100)
            .max(1);
        let keep = usize::try_from(keep).map_err(|_| TableMetadataError::Bounds)?;
        let mut log = self.raw.array("metadata-log")?;
        if log.len() >= keep {
            log.drain(..=(log.len() - keep));
        }
        log.push(raw::encode(
            &json!({
                "timestamp-ms": prior.fields()["last-updated-ms"],
                "metadata-file": prior.selected_head().metadata_location.to_string(),
            }),
            self.raw.limit,
        )?);
        self.raw.set("metadata-log", &log)
    }
}

fn integer(value: &Value, name: &'static str) -> Result<i32, TableMetadataError> {
    value[name]
        .as_i64()
        .and_then(|value| i32::try_from(value).ok())
        .filter(|value| *value >= 0)
        .ok_or(TableMetadataError::Field(name))
}
