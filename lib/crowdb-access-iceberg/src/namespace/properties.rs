use std::collections::{BTreeMap, BTreeSet};

use crate::error::ValidationError;

pub const MAX_PROPERTIES: usize = 256;
const MAX_KEY_BYTES: usize = 1024;
const MAX_VALUE_BYTES: usize = 8192;

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct NamespaceProperties(BTreeMap<String, String>);

#[derive(Clone, Debug, Default, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct PropertyChanges {
    pub removals: Vec<String>,
    pub updates: BTreeMap<String, String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PropertyUpdate {
    pub properties: NamespaceProperties,
    pub removed: Vec<String>,
    pub updated: Vec<String>,
    pub missing: Vec<String>,
}

impl NamespaceProperties {
    /// # Errors
    /// Rejects excessive entries, invalid text and excessive aggregate payload.
    /// The authority encoder additionally enforces the complete record limit.
    pub fn new(properties: BTreeMap<String, String>) -> Result<Self, ValidationError> {
        if properties.len() > MAX_PROPERTIES {
            return Err(ValidationError::RecordTooLarge);
        }
        let mut payload_bytes = 0_usize;
        for (key, value) in &properties {
            validate_text(key, MAX_KEY_BYTES)?;
            validate_text(value, MAX_VALUE_BYTES)?;
            payload_bytes = payload_bytes.saturating_add(key.len() + value.len());
            if payload_bytes > crate::record::MAX_RECORD_BYTES {
                return Err(ValidationError::RecordTooLarge);
            }
        }
        Ok(Self(properties))
    }

    #[must_use]
    pub fn entries(&self) -> &BTreeMap<String, String> {
        &self.0
    }

    /// # Errors
    /// Rejects invalid changes without modifying the original properties.
    pub fn apply(&self, changes: &PropertyChanges) -> Result<PropertyUpdate, ValidationError> {
        changes.validate()?;
        let mut properties = self.0.clone();
        let mut removed = Vec::new();
        let mut missing = Vec::new();
        for key in changes.removals.iter().collect::<BTreeSet<_>>() {
            if properties.remove(key).is_some() {
                removed.push(key.clone());
            } else {
                missing.push(key.clone());
            }
        }
        properties.extend(changes.updates.clone());
        Ok(PropertyUpdate {
            properties: Self::new(properties)?,
            removed,
            updated: changes.updates.keys().cloned().collect(),
            missing,
        })
    }
}

impl PropertyChanges {
    /// # Errors
    /// Distinguishes overlapping removal/update keys for the REST 422 response.
    pub fn validate(&self) -> Result<(), ValidationError> {
        if self.removals.len() > MAX_PROPERTIES {
            return Err(ValidationError::RecordTooLarge);
        }
        for key in &self.removals {
            validate_text(key, MAX_KEY_BYTES)?;
            if self.updates.contains_key(key) {
                return Err(ValidationError::PropertyOverlap);
            }
        }
        NamespaceProperties::new(self.updates.clone()).map(|_| ())
    }
}

fn validate_text(value: &str, max_bytes: usize) -> Result<(), ValidationError> {
    if value.len() > max_bytes || value.contains('\0') {
        return Err(ValidationError::Text);
    }
    Ok(())
}
