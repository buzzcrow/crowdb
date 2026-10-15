// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::super::ApiError;
use base64::{engine::general_purpose::STANDARD, Engine};
use crowdb_access_dataset::MetadataScanBudget;
use std::{collections::BTreeMap, time::Duration};

pub(super) struct Query(BTreeMap<String, String>);
impl Query {
    pub(super) fn parse(query: Option<&str>, allowed: &[&str]) -> Result<Self, ApiError> {
        let mut values = BTreeMap::new();
        let query = query.unwrap_or("");
        if query.len() > 64 * 1024 {
            return Err(ApiError::too_large());
        }
        for part in query.split('&').flat_map(|pair| pair.split('=')) {
            let bytes = part.as_bytes();
            for (index, byte) in bytes.iter().enumerate() {
                if *byte == b'%'
                    && (index + 2 >= bytes.len()
                        || !bytes[index + 1].is_ascii_hexdigit()
                        || !bytes[index + 2].is_ascii_hexdigit())
                {
                    return Err(ApiError::invalid());
                }
            }
            percent_encoding::percent_decode_str(part)
                .decode_utf8()
                .map_err(|_| ApiError::invalid())?;
        }
        for (name, value) in url::form_urlencoded::parse(query.as_bytes()) {
            if !allowed.contains(&name.as_ref())
                || values.insert(name.into_owned(), value.into_owned()).is_some()
            {
                return Err(ApiError::invalid());
            }
        }
        Ok(Self(values))
    }
    pub(super) fn value(&self, name: &str) -> Option<&str> {
        self.0.get(name).map(String::as_str)
    }
    pub(super) fn after(&self) -> Result<Option<Vec<u8>>, ApiError> {
        self.value("after")
            .map(|value| bytes(value, 32 * 1024, false))
            .transpose()
    }
    pub(super) fn budget(&self) -> Result<MetadataScanBudget, ApiError> {
        let items = self.value("limit").map_or(Ok(20), |value| {
            if value.is_empty() || !value.bytes().all(|b| b.is_ascii_digit()) {
                return Err(ApiError::invalid());
            }
            value.parse::<usize>().map_err(|_| ApiError::invalid())
        })?;
        if !(1..=100).contains(&items) {
            return Err(ApiError::invalid());
        }
        Ok(MetadataScanBudget {
            items,
            bytes: 1024 * 1024,
            timeout: Duration::from_secs(2),
        })
    }
    pub(super) fn fields(&self) -> Result<Vec<String>, ApiError> {
        let fields: Vec<String> = serde_json::from_str(self.value("fields").ok_or_else(ApiError::invalid)?)
            .map_err(|_| ApiError::invalid())?;
        if fields.is_empty() || fields.len() > 12 {
            return Err(ApiError::invalid());
        }
        Ok(fields)
    }
}
pub(super) fn bytes(value: &str, max: usize, empty: bool) -> Result<Vec<u8>, ApiError> {
    if value.len() > max.div_ceil(3) * 4 {
        return Err(ApiError::too_large());
    }
    let bytes = STANDARD.decode(value).map_err(|_| ApiError::invalid())?;
    if bytes.len() > max || (!empty && bytes.is_empty()) || STANDARD.encode(&bytes) != value {
        return Err(ApiError::invalid());
    }
    Ok(bytes)
}
