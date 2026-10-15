// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{
    super::{operation, snapshot, ApiError},
    predicate::Predicate,
    split_models::{decimal, required_option, revision, DraftSplit, Policy, Source},
};
use crowdb_access_dataset::{composition as native, FieldDefinition, SchemaRecord};
use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Schema {
    pub version: u32,
    pub fields: Vec<Field>,
}
#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Field {
    pub name: String,
    pub required: bool,
}
impl Schema {
    pub(super) fn native(self) -> Result<SchemaRecord, ApiError> {
        if self.fields.len() > 256 || self.fields.iter().any(|f| f.name.len() > 1024) {
            return Err(ApiError::too_large());
        }
        Ok(SchemaRecord {
            version: self.version,
            fields: self
                .fields
                .into_iter()
                .map(|f| FieldDefinition {
                    name: f.name,
                    required: f.required,
                })
                .collect(),
        })
    }
}
impl From<SchemaRecord> for Schema {
    fn from(value: SchemaRecord) -> Self {
        Self {
            version: value.version,
            fields: value
                .fields
                .into_iter()
                .map(|f| Field {
                    name: f.name,
                    required: f.required,
                })
                .collect(),
        }
    }
}
#[derive(Deserialize, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub(super) enum CompositionSource {
    All,
    Selection {
        predicate: Predicate,
    },
    Split {
        source: Source,
        selection: Option<Predicate>,
    },
}
impl CompositionSource {
    fn native(self) -> Result<native::CompositionSource, ApiError> {
        Ok(match self {
            Self::All => native::CompositionSource::All,
            Self::Selection { predicate } => native::CompositionSource::Selection(predicate.native()?),
            Self::Split { source, selection } => native::CompositionSource::Split {
                source: source.native()?,
                selection: selection.map(Predicate::native).transpose()?,
            },
        })
    }
}
impl From<native::CompositionSource> for CompositionSource {
    fn from(value: native::CompositionSource) -> Self {
        match value {
            native::CompositionSource::All => Self::All,
            native::CompositionSource::Selection(predicate) => Self::Selection {
                predicate: predicate.into(),
            },
            native::CompositionSource::Split { source, selection } => Self::Split {
                source: source.into(),
                selection: selection.map(Into::into),
            },
        }
    }
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Begin {
    pub operation: String,
    #[serde(deserialize_with = "required_option")]
    pub parent: Option<String>,
    pub source: CompositionSource,
    pub schema: Option<Schema>,
    pub inherit_parent_splits: bool,
    pub parent_split_revision: Option<String>,
    pub splits: Vec<DraftSplit>,
    pub policy: Option<Policy>,
}
impl Begin {
    pub(super) fn native(
        self,
    ) -> Result<(crowdb_access_dataset::OperationId, native::CompositionInput), ApiError> {
        Ok((
            operation(&self.operation)?,
            native::CompositionInput {
                parent: self.parent.as_deref().map(snapshot).transpose()?,
                source: self.source.native()?,
                schema: self.schema.map(Schema::native).transpose()?,
                inherit_parent_splits: self.inherit_parent_splits,
                parent_split_revision: self.parent_split_revision.as_deref().map(revision).transpose()?,
                splits: self
                    .splits
                    .into_iter()
                    .map(DraftSplit::native)
                    .collect::<Result<_, _>>()?,
                policy: self.policy.map(Into::into),
            },
        ))
    }
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct CompositionEdit {
    pub edit: String,
    pub expected_revision: String,
    pub schema: Option<Schema>,
    pub samples: Vec<super::composition_fields::SampleChange>,
    pub splits: Option<Vec<DraftSplit>>,
    pub policy: Option<Policy>,
}
impl CompositionEdit {
    pub(super) async fn native(
        self,
        identity: &crowdb_access_dataset::DatasetIdentity,
        uploads: &crowdb_access_dataset::DatasetUploads,
    ) -> Result<native::CompositionEdit, ApiError> {
        if self.samples.len() > 20 {
            return Err(ApiError::too_large());
        }
        let mut samples = Vec::new();
        for sample in self.samples {
            samples.push(sample.native(identity, uploads).await?);
        }
        Ok(native::CompositionEdit {
            id: operation(&self.edit)?,
            expected_revision: decimal(&self.expected_revision)?,
            schema: self.schema.map(Schema::native).transpose()?,
            samples,
            splits: self
                .splits
                .map(|splits| {
                    splits
                        .into_iter()
                        .map(DraftSplit::native)
                        .collect::<Result<_, _>>()
                })
                .transpose()?,
            policy: self.policy.map(Into::into),
        })
    }
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Publish {
    pub attempt: String,
    pub snapshot: String,
    #[serde(deserialize_with = "required_option")]
    pub expected_head: Option<String>,
}
