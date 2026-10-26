// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
use super::{Predicate, SplitSource};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Schema {
    pub version: u32,
    pub fields: Vec<crate::FieldDefinition>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum CompositionSource {
    All,
    Selection {
        predicate: Predicate,
    },
    Split {
        source: SplitSource,
        selection: Option<Predicate>,
    },
}
