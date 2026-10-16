// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! V2 predicate semantics over effective inline fields; v1 persisted plans stay unchanged.
use crate::{DatasetError, FieldDefinition, FieldLocator, Selection};
use std::collections::{BTreeMap, BTreeSet};

pub(crate) struct EffectiveSelection<'a> {
    rule: &'a Selection,
    pub fields: Vec<String>,
    pub explicit_ids: BTreeSet<Vec<u8>>,
}
impl<'a> EffectiveSelection<'a> {
    pub(crate) fn new(rule: &'a Selection, schema: &[FieldDefinition]) -> Result<Self, DatasetError> {
        crate::split::definition::validate_selection(rule, 0).map_err(|_| DatasetError::InvalidManifest)?;
        if serde_json::to_vec(rule)
            .map_err(|_| DatasetError::InvalidManifest)?
            .len()
            > 64 * 1024
        {
            return Err(DatasetError::ReadLimitExceeded);
        }
        let names: BTreeSet<_> = schema.iter().map(|f| f.name.as_str()).collect();
        let mut fields = BTreeSet::new();
        let mut explicit_ids = BTreeSet::new();
        collect(rule, &names, &mut fields, &mut explicit_ids)?;
        Ok(Self {
            rule,
            fields: fields.into_iter().collect(),
            explicit_ids,
        })
    }
    pub(crate) fn matches(&self, sample_id: &[u8], fields: &BTreeMap<String, FieldLocator>) -> bool {
        matches(self.rule, sample_id, fields)
    }
}
fn collect(
    rule: &Selection,
    schema: &BTreeSet<&str>,
    fields: &mut BTreeSet<String>,
    ids: &mut BTreeSet<Vec<u8>>,
) -> Result<(), DatasetError> {
    match rule {
        Selection::FieldMatch { field, .. } => {
            if !schema.contains(field.as_str()) {
                return Err(DatasetError::InvalidManifest);
            }
            fields.insert(field.clone());
        }
        Selection::Keyword(_) => fields.extend(schema.iter().map(|s| (*s).to_owned())),
        Selection::Explicit(explicit) => {
            ids.extend(explicit.iter().cloned());
        }
        Selection::And(items) | Selection::Or(items) => {
            for item in items {
                collect(item, schema, fields, ids)?;
            }
        }
        Selection::Not(item) => collect(item, schema, fields, ids)?,
        Selection::Prefix(_) | Selection::Range { .. } => {}
    }
    Ok(())
}
fn matches(rule: &Selection, id: &[u8], fields: &BTreeMap<String, FieldLocator>) -> bool {
    match rule {
        Selection::Prefix(prefix) => id.starts_with(prefix),
        Selection::Range { start, end } => id >= start.as_slice() && id < end.as_slice(),
        Selection::Explicit(ids) => ids.iter().any(|candidate| candidate == id),
        Selection::FieldMatch { field, value } => matches!(fields.get(field), Some(FieldLocator::Inline { value: actual, .. }) if actual == value),
        Selection::Keyword(keyword) => fields.values().any(|field| matches!(field, FieldLocator::Inline { value, .. } if value.windows(keyword.len()).any(|window| window == keyword))),
        Selection::And(items) => items.iter().all(|item| matches(item, id, fields)),
        Selection::Or(items) => items.iter().any(|item| matches(item, id, fields)),
        Selection::Not(item) => !matches(item, id, fields),
    }
}
