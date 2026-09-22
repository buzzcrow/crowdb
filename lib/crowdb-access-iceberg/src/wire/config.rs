use serde::Serialize;
use std::collections::BTreeMap;

use crate::catalog::{Capabilities, FormatAction};

#[derive(Debug, Serialize)]
pub struct CatalogConfig {
    pub defaults: BTreeMap<String, String>,
    pub overrides: BTreeMap<String, String>,
    pub endpoints: Vec<String>,
    #[serde(rename = "idempotency-key-lifetime", skip_serializing_if = "Option::is_none")]
    pub idempotency_key_lifetime: Option<String>,
}

impl CatalogConfig {
    /// # Errors
    /// Returns the standard unknown-warehouse error for nonempty selectors.
    pub fn foundation(warehouse: Option<&str>) -> Result<Self, IcebergErrorResponse> {
        if warehouse.is_some_and(|value| !value.is_empty()) {
            return Err(IcebergErrorResponse::new(
                404,
                "NoSuchWarehouseException",
                "The given warehouse does not exist",
            ));
        }
        let mut overrides = BTreeMap::new();
        for (index, version) in Capabilities::default().versions.iter().enumerate() {
            for (name, action) in [
                ("parse", FormatAction::Parse),
                ("read", FormatAction::Read),
                ("create", FormatAction::Create),
                ("write", FormatAction::Write),
            ] {
                overrides.insert(
                    format!("crowdb.iceberg.v{}.{}", index + 1, name),
                    version.supports(action).to_string(),
                );
            }
        }
        overrides.insert("crowdb.iceberg.upgrade-v1-v2".into(), "false".into());
        overrides.insert("crowdb.iceberg.upgrade-v2-v3".into(), "false".into());
        Ok(Self {
            defaults: BTreeMap::new(),
            overrides,
            endpoints: Vec::new(),
            idempotency_key_lifetime: None,
        })
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct IcebergErrorResponse {
    pub error: IcebergError,
}

#[derive(Clone, Debug, Serialize)]
pub struct IcebergError {
    pub code: u16,
    #[serde(rename = "type")]
    pub kind: &'static str,
    pub message: &'static str,
}

impl IcebergErrorResponse {
    #[must_use]
    pub const fn new(code: u16, kind: &'static str, message: &'static str) -> Self {
        Self {
            error: IcebergError { code, kind, message },
        }
    }
}
