// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{super::ApiError, predicate::Predicate, split_models::Source};
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
    pub(super) fn native(self) -> Result<native::CompositionSource, ApiError> {
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
