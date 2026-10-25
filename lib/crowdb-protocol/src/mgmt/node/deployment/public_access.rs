// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Explicit client origins, independent of internal service listeners.

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PublicAccessUris {
    pub s3: Option<String>,
    pub iceberg: Option<String>,
    pub dataset: Option<String>,
}

impl PublicAccessUris {
    /// # Errors
    /// Require HTTP origins without credentials, paths, queries or fragments.
    pub fn validate(&self) -> Result<(), &'static str> {
        for value in [&self.s3, &self.iceberg, &self.dataset].into_iter().flatten() {
            let uri = url::Url::parse(value).map_err(|_| "invalid public access URI")?;
            if value.len() > 2048
                || !(value.starts_with("http://") || value.starts_with("https://"))
                || value
                    .bytes()
                    .any(|byte| byte.is_ascii_whitespace() || byte == b'\\')
                || !matches!(uri.scheme(), "http" | "https")
                || uri.host_str().is_none()
                || !uri.username().is_empty()
                || uri.password().is_some()
                || uri.path() != "/"
                || uri.query().is_some()
                || uri.fragment().is_some()
            {
                return Err("public access URI must be an HTTP origin without credentials");
            }
        }
        Ok(())
    }

    pub(super) fn apply(&self, target: &mut Self) {
        for (value, destination) in [
            (&self.s3, &mut target.s3),
            (&self.iceberg, &mut target.iceberg),
            (&self.dataset, &mut target.dataset),
        ] {
            if value.is_some() {
                destination.clone_from(value);
            }
        }
    }

    /// Include only explicit choices; the service builder supplies internal defaults.
    #[must_use]
    pub fn environment(&self) -> std::collections::BTreeMap<String, String> {
        [
            ("CROWDB_S3_PUBLIC_URI", &self.s3),
            ("CROWDB_ICEBERG_PUBLIC_URI", &self.iceberg),
            ("CROWDB_DATASET_PUBLIC_URI", &self.dataset),
        ]
        .into_iter()
        .filter_map(|(key, value)| value.as_ref().map(|value| (key.into(), value.clone())))
        .collect()
    }
}
