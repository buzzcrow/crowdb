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

/// Evaluates standard GET/HEAD conditions using fields from one KV value.
#[must_use]
pub fn evaluate(record: &ObjectRecord, conditions: &ObjectConditions<'_>) -> ConditionOutcome {
    if conditions
        .if_match
        .is_some_and(|value| !etag_list_matches(value, &record.etag))
    {
        return ConditionOutcome::PreconditionFailed;
    }
    if conditions.if_match.is_none()
        && conditions
            .if_unmodified_since
            .and_then(parse_http_seconds)
            .is_some_and(|limit| record.modified_at_ms / 1000 > limit)
    {
        return ConditionOutcome::PreconditionFailed;
    }
    if conditions
        .if_none_match
        .is_some_and(|value| etag_list_matches(value, &record.etag))
    {
        return ConditionOutcome::NotModified;
    }
    if conditions.if_none_match.is_none()
        && conditions
            .if_modified_since
            .and_then(parse_http_seconds)
            .is_some_and(|limit| record.modified_at_ms / 1000 <= limit)
    {
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
