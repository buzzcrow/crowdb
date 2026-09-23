use std::{collections::BTreeMap, sync::Arc};

use serde_json::Value;
use sha2::{Digest, Sha256};

use super::{SelectedTable, TableHead};
use crate::file::{ContentFormat, FileBlockStore, FileIoError, FileKind, FileReader};

mod auxiliary;
mod context;
mod defaults;
mod json;
mod layout;
mod name_mapping;
mod root;
mod schemas;
mod snapshots;

pub(crate) fn validate_schema_definition(
    schema: &Value,
    version: crate::manifest::ManifestVersion,
    mut work: usize,
) -> Result<(), TableMetadataError> {
    defaults::validate(schema, version, &mut work)
}

pub(crate) fn schema_default_identity(
    schema: &Value,
    value: &Value,
    version: crate::manifest::ManifestVersion,
) -> Result<String, TableMetadataError> {
    defaults::identity(schema, value, version, &mut 1_000_000)
}

pub(crate) fn validate_layout_definitions(
    root: &Value,
    schema: &crate::manifest::ManifestContext,
    limits: TableMetadataLimits,
) -> Result<(), TableMetadataError> {
    layout::validate(root, schema, limits)
}

pub(crate) fn validate_metadata_payloads(
    root: &Value,
    head: &TableHead,
    limits: TableMetadataLimits,
) -> Result<(), TableMetadataError> {
    let envelope = root::validate(root, head, limits)?;
    snapshots::parse(root, head, &envelope, limits)?;
    auxiliary::validate(root, head, limits)
}

pub(crate) fn validate_auxiliary_definition(
    root: &Value,
    head: &TableHead,
    limits: TableMetadataLimits,
) -> Result<(), TableMetadataError> {
    auxiliary::validate(root, head, limits)
}

pub use snapshots::TableSnapshot;

#[derive(Clone, Copy, Debug)]
pub struct TableMetadataLimits {
    pub bytes: usize,
    pub values: usize,
    pub depth: usize,
    pub string_bytes: usize,
    pub collection_entries: usize,
}

impl TableMetadataLimits {
    pub(crate) fn validate(self) -> Result<(), TableMetadataError> {
        if self.bytes == 0
            || self.bytes > 64 * 1024 * 1024
            || self.values == 0
            || self.values > 1_000_000
            || self.depth == 0
            || self.depth > 64
            || self.string_bytes == 0
            || self.string_bytes > self.bytes
            || self.collection_entries == 0
            || self.collection_entries > 100_000
        {
            return Err(TableMetadataError::Bounds);
        }
        Ok(())
    }
}

#[derive(Debug, thiserror::Error)]
pub enum TableMetadataError {
    #[error("table metadata resource limit exceeded")]
    Bounds,
    #[error("table metadata does not match its selected immutable authority")]
    Binding,
    #[error("invalid table metadata field: {0}")]
    Field(&'static str),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error(transparent)]
    Storage(#[from] FileIoError),
}

/// Canonical document with validated envelope, snapshot graph and reference linkage.
/// Cross-generation evolution and file semantics remain separate validation phases;
/// possession of this document is not a table publication or full metadata proof.
#[derive(Debug)]
pub struct TableMetadataDocument {
    head: TableHead,
    canonical: Vec<u8>,
    root: serde_json::Map<String, Value>,
    snapshots: BTreeMap<i64, TableSnapshot>,
    current_snapshot: Option<i64>,
}

impl TableMetadataDocument {
    /// # Errors
    /// Rejects mismatched head/digest, malformed or duplicate JSON, resource exhaustion,
    /// invalid version envelopes and inconsistent snapshot/ref/log relationships.
    pub fn parse(
        canonical: Vec<u8>,
        head: &TableHead,
        limits: TableMetadataLimits,
    ) -> Result<Self, TableMetadataError> {
        limits.validate()?;
        if canonical.len() > limits.bytes {
            return Err(TableMetadataError::Bounds);
        }
        if head.validate().is_err() || <[u8; 32]>::from(Sha256::digest(&canonical)) != head.metadata_digest {
            return Err(TableMetadataError::Binding);
        }
        let root = decode_bounded_json(&canonical, limits)?;
        let envelope = root::validate(&root, head, limits)?;
        let schema = schemas::validate(&root, head.format_version, limits)?;
        layout::validate(&root, &schema, limits)?;
        name_mapping::validate(&root, limits)?;
        let snapshots = snapshots::parse(&root, head, &envelope, limits)?;
        snapshots::references(&root, envelope.current_snapshot, &snapshots, limits)?;
        snapshots::logs(&root, head, &snapshots, limits)?;
        auxiliary::validate(&root, head, limits)?;
        let Value::Object(root) = root else {
            return Err(TableMetadataError::Field("metadata"));
        };
        Ok(Self {
            head: head.clone(),
            canonical,
            root,
            snapshots,
            current_snapshot: envelope.current_snapshot,
        })
    }

    #[must_use]
    pub fn canonical(&self) -> &[u8] {
        &self.canonical
    }

    #[must_use]
    pub fn fields(&self) -> &serde_json::Map<String, Value> {
        &self.root
    }

    #[must_use]
    pub fn snapshots(&self) -> &BTreeMap<i64, TableSnapshot> {
        &self.snapshots
    }

    #[must_use]
    pub fn current_snapshot(&self) -> Option<i64> {
        self.current_snapshot
    }

    pub(crate) fn selected_head(&self) -> &TableHead {
        &self.head
    }
}

pub(crate) fn decode_bounded_json(
    bytes: &[u8],
    limits: TableMetadataLimits,
) -> Result<Value, TableMetadataError> {
    limits.validate()?;
    if bytes.len() > limits.bytes {
        return Err(TableMetadataError::Bounds);
    }
    json::parse(bytes, limits)
}

/// Reads and verifies the entire selected immutable JSON, preserving original bytes.
/// Canonical fallback is independent of disposable projections. This is not a REST
/// load endpoint or a full table metadata validation proof.
/// # Errors
/// Rejects foreign selections, excessive length, corruption and invalid documents.
pub async fn read_table_metadata_document(
    store: Arc<dyn FileBlockStore>,
    selected: &SelectedTable,
    limits: TableMetadataLimits,
) -> Result<TableMetadataDocument, TableMetadataError> {
    limits.validate()?;
    let record = &selected.metadata;
    let head = &selected.head;
    if record.length > limits.bytes as u64 {
        return Err(TableMetadataError::Bounds);
    }
    if record.file != head.metadata_file
        || record.location != head.metadata_location
        || record.digest != head.metadata_digest
        || record.kind != FileKind::Metadata
        || record.format != ContentFormat::Json
    {
        return Err(TableMetadataError::Binding);
    }
    let mut reader = FileReader::new(store, record.clone(), None, 16 * 1024)?;
    let mut bytes = Vec::new();
    while let Some(frame) = reader.next().await? {
        if frame.len() > limits.bytes.saturating_sub(bytes.len()) {
            return Err(TableMetadataError::Bounds);
        }
        bytes.extend_from_slice(&frame);
    }
    TableMetadataDocument::parse(bytes, head, limits)
}

fn integer(value: &Value, field: &'static str) -> Result<i64, TableMetadataError> {
    value.as_i64().ok_or(TableMetadataError::Field(field))
}

fn nonnegative(value: &Value, field: &'static str) -> Result<i64, TableMetadataError> {
    integer(value, field).and_then(|value| {
        if value >= 0 {
            Ok(value)
        } else {
            Err(TableMetadataError::Field(field))
        }
    })
}

fn id(value: &Value, field: &'static str) -> Result<i32, TableMetadataError> {
    i32::try_from(nonnegative(value, field)?).map_err(|_| TableMetadataError::Field(field))
}

fn text<'value>(value: &'value Value, field: &'static str) -> Result<&'value str, TableMetadataError> {
    value.as_str().ok_or(TableMetadataError::Field(field))
}

fn array<'value>(
    value: &'value Value,
    field: &'static str,
    limits: TableMetadataLimits,
) -> Result<&'value [Value], TableMetadataError> {
    let values = value.as_array().ok_or(TableMetadataError::Field(field))?;
    if values.len() > limits.collection_entries {
        return Err(TableMetadataError::Bounds);
    }
    Ok(values)
}

fn optional_array<'value>(
    root: &'value Value,
    field: &'static str,
    limits: TableMetadataLimits,
) -> Result<&'value [Value], TableMetadataError> {
    root.get(field)
        .map_or(Ok(&[]), |value| array(value, field, limits))
}

fn strings(
    value: &Value,
    field: &'static str,
    limits: TableMetadataLimits,
) -> Result<(), TableMetadataError> {
    let values = value.as_object().ok_or(TableMetadataError::Field(field))?;
    if values.len() > limits.collection_entries {
        return Err(TableMetadataError::Bounds);
    }
    if values.values().any(|value| !value.is_string()) {
        return Err(TableMetadataError::Field(field));
    }
    Ok(())
}
