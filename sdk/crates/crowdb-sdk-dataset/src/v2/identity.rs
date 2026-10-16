// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use base64::{engine::general_purpose::STANDARD, Engine};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::{fmt, str::FromStr};

/// An exact unsigned integer serialized as a canonical decimal JSON string.
#[derive(Clone, Copy, Debug, Default, Eq, Ord, PartialEq, PartialOrd)]
pub struct Decimal(pub u64);
impl Serialize for Decimal {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.0.to_string())
    }
}
impl<'de> Deserialize<'de> for Decimal {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let text = String::deserialize(deserializer)?;
        let value = text.parse::<u64>().map_err(serde::de::Error::custom)?;
        if value.to_string() != text {
            return Err(serde::de::Error::custom("noncanonical decimal"));
        }
        Ok(Self(value))
    }
}
impl From<u64> for Decimal {
    fn from(value: u64) -> Self {
        Self(value)
    }
}

/// A nonzero operation, Snapshot or Split revision identity.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(transparent)]
pub struct OpaqueId(String);
impl OpaqueId {
    /// Generates a caller-owned retry identity. Keep it for the entire operation.
    #[must_use]
    pub fn random() -> Self {
        Self(uuid::Uuid::new_v4().simple().to_string())
    }
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}
impl FromStr for OpaqueId {
    type Err = crate::Error;
    fn from_str(value: &str) -> Result<Self, Self::Err> {
        if !valid_hex(value, 32) || value.bytes().all(|b| b == b'0') {
            return Err(crate::Error::Invalid(
                "nonzero lower-hex identity required".into(),
            ));
        }
        Ok(Self(value.into()))
    }
}
impl fmt::Display for OpaqueId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}
impl<'de> Deserialize<'de> for OpaqueId {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        String::deserialize(deserializer)?
            .parse()
            .map_err(serde::de::Error::custom)
    }
}

/// Opaque head generation; use only as an exact compare-and-swap precondition.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(transparent)]
pub struct Generation(String);
impl Generation {
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}
impl FromStr for Generation {
    type Err = crate::Error;
    fn from_str(value: &str) -> Result<Self, Self::Err> {
        if !valid_hex(value, 64) {
            return Err(crate::Error::Invalid("lower-hex head generation required".into()));
        }
        Ok(Self(value.into()))
    }
}
impl<'de> Deserialize<'de> for Generation {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        String::deserialize(deserializer)?
            .parse()
            .map_err(serde::de::Error::custom)
    }
}
fn valid_hex(value: &str, length: usize) -> bool {
    value.len() == length
        && value
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

/// Exact binary Sample ID; JSON uses canonical padded base64 without UTF-8 loss.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct SampleId(Vec<u8>);
impl SampleId {
    /// # Errors
    /// Rejects empty IDs and IDs exceeding 16 KiB.
    pub fn new(bytes: Vec<u8>) -> Result<Self, crate::Error> {
        if bytes.is_empty() || bytes.len() > 16384 {
            return Err(crate::Error::Invalid("Sample ID length".into()));
        }
        Ok(Self(bytes))
    }
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
    #[must_use]
    pub fn base64(&self) -> String {
        STANDARD.encode(&self.0)
    }
}
impl Serialize for SampleId {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.base64())
    }
}
impl<'de> Deserialize<'de> for SampleId {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let value = String::deserialize(deserializer)?;
        if value.len() > 21848 {
            return Err(serde::de::Error::custom("Sample ID length"));
        }
        let bytes = STANDARD.decode(&value).map_err(serde::de::Error::custom)?;
        if STANDARD.encode(&bytes) != value {
            return Err(serde::de::Error::custom("noncanonical Sample ID"));
        }
        Self::new(bytes).map_err(serde::de::Error::custom)
    }
}
