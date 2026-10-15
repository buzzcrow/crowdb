// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{super::ApiError, query::bytes};
use base64::{engine::general_purpose::STANDARD, Engine};
use crowdb_access_dataset::Selection;
use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub(super) enum Predicate {
    All,
    Explicit { ids: Vec<String> },
    Prefix { value: String },
    Keyword { value: String },
    Range { start: String, end: String },
    FieldMatch { field: String, value: String },
    And { items: Vec<Predicate> },
    Or { items: Vec<Predicate> },
    Not { item: Box<Predicate> },
}
impl Predicate {
    pub(super) fn native(self) -> Result<Selection, ApiError> {
        self.convert(0, &mut 1024)
    }
    fn convert(self, depth: usize, nodes: &mut usize) -> Result<Selection, ApiError> {
        if depth > 32 || *nodes == 0 {
            return Err(ApiError::too_large());
        }
        *nodes -= 1;
        Ok(match self {
            Self::All => Selection::Not(Box::new(Selection::Explicit(vec![]))),
            Self::Explicit { ids } => {
                if ids.len() > 10000 {
                    return Err(ApiError::too_large());
                }
                Selection::Explicit(
                    ids.into_iter()
                        .map(|id| bytes(&id, 16384, false))
                        .collect::<Result<_, _>>()?,
                )
            }
            Self::Prefix { value } => Selection::Prefix(bytes(&value, 16384, false)?),
            Self::Keyword { value } => Selection::Keyword(bytes(&value, 65536, false)?),
            Self::Range { start, end } => Selection::Range {
                start: bytes(&start, 16384, false)?,
                end: bytes(&end, 16384, false)?,
            },
            Self::FieldMatch { field, value } => Selection::FieldMatch {
                field,
                value: bytes(&value, 65536, true)?,
            },
            Self::And { items } => Selection::And(
                items
                    .into_iter()
                    .map(|item| item.convert(depth + 1, nodes))
                    .collect::<Result<_, _>>()?,
            ),
            Self::Or { items } => Selection::Or(
                items
                    .into_iter()
                    .map(|item| item.convert(depth + 1, nodes))
                    .collect::<Result<_, _>>()?,
            ),
            Self::Not { item } => Selection::Not(Box::new(item.convert(depth + 1, nodes)?)),
        })
    }
}
impl From<Selection> for Predicate {
    fn from(value: Selection) -> Self {
        match value {
            Selection::Explicit(ids) => Self::Explicit {
                ids: ids.into_iter().map(|id| STANDARD.encode(id)).collect(),
            },
            Selection::Prefix(value) => Self::Prefix {
                value: STANDARD.encode(value),
            },
            Selection::Keyword(value) => Self::Keyword {
                value: STANDARD.encode(value),
            },
            Selection::Range { start, end } => Self::Range {
                start: STANDARD.encode(start),
                end: STANDARD.encode(end),
            },
            Selection::FieldMatch { field, value } => Self::FieldMatch {
                field,
                value: STANDARD.encode(value),
            },
            Selection::And(items) => Self::And {
                items: items.into_iter().map(Self::from).collect(),
            },
            Selection::Or(items) => Self::Or {
                items: items.into_iter().map(Self::from).collect(),
            },
            Selection::Not(item) => Self::Not {
                item: Box::new(Self::from(*item)),
            },
        }
    }
}
