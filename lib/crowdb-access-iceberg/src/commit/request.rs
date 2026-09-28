use serde::Deserialize;

use super::{TableRequirement, TableUpdate};
use crate::{
    namespace::NamespaceIdentifier,
    table::{decode_bounded_json, TableMetadataError as Error, TableMetadataLimits},
};

#[derive(Clone, Copy, Debug)]
pub struct CommitRequestLimits {
    pub json: TableMetadataLimits,
    pub requirements: usize,
    pub updates: usize,
}

#[derive(Debug, Deserialize)]
pub struct CommitTableIdentifier {
    pub namespace: Vec<String>,
    pub name: String,
}

#[derive(Debug, Deserialize)]
pub struct CommitRequest {
    pub identifier: Option<CommitTableIdentifier>,
    pub requirements: Vec<TableRequirement>,
    pub updates: Vec<TableUpdate>,
}

#[derive(Deserialize)]
struct RawRequest<'request> {
    #[serde(borrow)]
    updates: Vec<&'request serde_json::value::RawValue>,
}

impl CommitRequest {
    #[must_use]
    pub fn create_version(&self) -> u8 {
        self.upgrade_targets().max().unwrap_or(2)
    }

    pub fn upgrade_targets(&self) -> impl Iterator<Item = u8> + '_ {
        self.updates.iter().filter_map(|update| match update {
            TableUpdate::UpgradeFormatVersion { format_version } => u8::try_from(*format_version).ok(),
            _ => None,
        })
    }

    /// Decodes the complete closed requirement/update union under independent JSON and count limits.
    /// Scalar parameters are checked, but nested payloads and selected-state semantics still need evaluation.
    /// # Errors
    /// Rejects unknown variants, duplicate keys, malformed fields and excessive work before returning a request.
    pub fn decode(bytes: &[u8], limits: CommitRequestLimits) -> Result<Self, Error> {
        if limits.requirements == 0
            || limits.requirements > 1000
            || limits.updates == 0
            || limits.updates > 1000
        {
            return Err(Error::Bounds);
        }
        let value = decode_bounded_json(bytes, limits.json)?;
        for (name, limit) in [("requirements", limits.requirements), ("updates", limits.updates)] {
            let items = value[name].as_array().ok_or(Error::Field(name))?;
            if items.len() > limit {
                return Err(Error::Bounds);
            }
        }
        let mut request: Self = serde_json::from_value(value)?;
        let raw: RawRequest<'_> = serde_json::from_slice(bytes)?;
        for (update, raw) in request.updates.iter_mut().zip(raw.updates) {
            update.validate_parameters()?;
            update.retain_payload(raw)?;
        }
        if let Some(identifier) = &request.identifier {
            NamespaceIdentifier::new(identifier.namespace.clone()).map_err(|_| Error::Field("identifier"))?;
            crate::key::NameSuffix {
                parent: None,
                name: &identifier.name,
            }
            .encode()
            .map_err(|_| Error::Field("identifier"))?;
        }
        Ok(request)
    }

    /// # Errors
    /// Rejects a body identifier that disagrees with the authenticated route.
    pub fn check_identifier(&self, namespace: &NamespaceIdentifier, name: &str) -> Result<(), Error> {
        if self.identifier.as_ref().is_some_and(|identifier| {
            identifier.namespace != namespace.components() || identifier.name != name
        }) {
            return Err(Error::Field("identifier"));
        }
        Ok(())
    }
}
