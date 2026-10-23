// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Conditional request evaluation against one resolved object record.

use std::time::UNIX_EPOCH;

use crate::metadata::ObjectRecord;

#[derive(Default)]
pub struct ObjectConditions<'a> {
    pub if_match: Option<&'a str>,
    pub if_none_match: Option<&'a str>,
    pub if_modified_since: Option<&'a str>,
    pub if_unmodified_since: Option<&'a str>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConditionOutcome {
    Proceed,
    NotModified,
    PreconditionFailed,
}

/// Typed entity-tag selection for trusted callers without HTTP headers.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum EtagSelection {
    Any,
    Values(Vec<String>),
}

impl EtagSelection {
    fn matches(&self, etag: &str) -> bool {
        match self {
            Self::Any => true,
            Self::Values(values) => values.iter().any(|value| value == etag),
        }
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ReadPreconditions {
    pub if_match: Option<EtagSelection>,
    pub if_none_match: Option<EtagSelection>,
    pub if_modified_since_seconds: Option<u64>,
    pub if_unmodified_since_seconds: Option<u64>,
}

pub trait ReadCondition {
    fn evaluate(&self, record: &ObjectRecord) -> ConditionOutcome;
}

impl ReadCondition for ReadPreconditions {
    fn evaluate(&self, record: &ObjectRecord) -> ConditionOutcome {
        evaluate_values(
            record,
            self.if_match.as_ref().map(|value| value.matches(&record.etag)),
            self.if_none_match
                .as_ref()
                .map(|value| value.matches(&record.etag)),
            self.if_modified_since_seconds,
            self.if_unmodified_since_seconds,
        )
    }
}

impl ReadCondition for ObjectConditions<'_> {
    fn evaluate(&self, record: &ObjectRecord) -> ConditionOutcome {
        evaluate(record, self)
    }
}

/// Evaluates standard GET/HEAD conditions using fields from one KV value.
#[must_use]
pub fn evaluate(record: &ObjectRecord, conditions: &ObjectConditions<'_>) -> ConditionOutcome {
    evaluate_values(
        record,
        conditions
            .if_match
            .map(|value| etag_list_matches(value, &record.etag)),
        conditions
            .if_none_match
            .map(|value| etag_list_matches(value, &record.etag)),
        conditions.if_modified_since.and_then(parse_http_seconds),
        conditions.if_unmodified_since.and_then(parse_http_seconds),
    )
}

fn evaluate_values(
    record: &ObjectRecord,
    if_match: Option<bool>,
    if_none_match: Option<bool>,
    modified_since: Option<u64>,
    unmodified_since: Option<u64>,
) -> ConditionOutcome {
    if if_match == Some(false) {
        return ConditionOutcome::PreconditionFailed;
    }
    if if_match.is_none() && unmodified_since.is_some_and(|limit| record.modified_at_ms / 1000 > limit) {
        return ConditionOutcome::PreconditionFailed;
    }
    if if_none_match == Some(true) {
        return ConditionOutcome::NotModified;
    }
    if if_none_match.is_none() && modified_since.is_some_and(|limit| record.modified_at_ms / 1000 <= limit) {
        return ConditionOutcome::NotModified;
    }
    ConditionOutcome::Proceed
}

fn etag_list_matches(value: &str, etag: &str) -> bool {
    value.trim() == "*"
        || value
            .split(',')
            .map(str::trim)
            .any(|candidate| candidate.trim_start_matches("W/").trim_matches('"') == etag)
}

fn parse_http_seconds(value: &str) -> Option<u64> {
    httpdate::parse_http_date(value)
        .ok()?
        .duration_since(UNIX_EPOCH)
        .ok()
        .map(|duration| duration.as_secs())
}
