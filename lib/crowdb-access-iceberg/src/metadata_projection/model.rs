use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;
use sha2::{Digest, Sha256};

use crate::file::{FileRecord, TableLocation};
use crate::key::{CatalogScope, IcebergKey};

pub const PROJECTION_VERSION: u16 = 2;
pub const PROJECTION_PAGE_BYTES: usize = 32 * 1024;
pub const MAX_PROJECTION_BYTES: usize = 2 * 1024 * 1024;
pub(super) const MAX_CHILDREN: usize = 64;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct ProjectionIdentity {
    pub table: TableLocation,
    pub generation: u64,
    pub digest: [u8; 32],
    pub version: u16,
}

impl ProjectionIdentity {
    #[must_use]
    pub fn new(record: &FileRecord, generation: u64) -> Self {
        Self {
            table: record.location.table(),
            generation,
            digest: record.digest,
            version: PROJECTION_VERSION,
        }
    }

    pub(super) fn key(self, child: u16, page: u16) -> Option<Vec<u8>> {
        let mut suffix = self.table.table.as_bytes().to_vec();
        suffix.extend_from_slice(&self.generation.to_be_bytes());
        suffix.extend_from_slice(&self.digest);
        suffix.extend_from_slice(&self.version.to_be_bytes());
        suffix.extend_from_slice(&child.to_be_bytes());
        suffix.extend_from_slice(&page.to_be_bytes());
        IcebergKey::Catalog {
            catalog: self.table.catalog,
            scope: CatalogScope::MetadataProjection,
            suffix,
        }
        .encode()
        .ok()
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Root {
    catalog: [u8; 16],
    table: [u8; 16],
    generation: u64,
    digest: [u8; 32],
    version: u16,
    length: u64,
    pub children: BTreeMap<String, Child>,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Child {
    pub index: u16,
    pub length: usize,
    pub digest: [u8; 32],
}

impl Root {
    pub fn new(identity: ProjectionIdentity, length: u64, fields: &Fields<'_>) -> Option<Self> {
        let children = fields
            .0
            .iter()
            .enumerate()
            .map(|(index, (name, value))| {
                Some((
                    name.clone(),
                    Child {
                        index: u16::try_from(index + 1).ok()?,
                        length: value.get().len(),
                        digest: Sha256::digest(value.get().as_bytes()).into(),
                    },
                ))
            })
            .collect::<Option<_>>()?;
        Some(Self {
            catalog: *identity.table.catalog.as_bytes(),
            table: *identity.table.table.as_bytes(),
            generation: identity.generation,
            digest: identity.digest,
            version: identity.version,
            length,
            children,
        })
    }

    pub fn encode(&self) -> Option<Vec<u8>> {
        let body = serde_json::to_vec(self).ok()?;
        if body.len() + 32 > PROJECTION_PAGE_BYTES {
            return None;
        }
        let mut bytes = Sha256::digest(&body).to_vec();
        bytes.extend(body);
        Some(bytes)
    }

    pub fn decode(bytes: &[u8], identity: ProjectionIdentity, length: u64) -> Option<Self> {
        if bytes.len() > PROJECTION_PAGE_BYTES || bytes.len() < 32 {
            return None;
        }
        if Sha256::digest(&bytes[32..])[..] != bytes[..32] {
            return None;
        }
        let root: Self = serde_json::from_slice(&bytes[32..]).ok()?;
        if root.catalog != *identity.table.catalog.as_bytes()
            || root.table != *identity.table.table.as_bytes()
            || root.generation != identity.generation
            || root.digest != identity.digest
            || root.version != PROJECTION_VERSION
            || root.length != length
            || length > MAX_PROJECTION_BYTES as u64
            || root.children.len() > MAX_CHILDREN
        {
            return None;
        }
        let mut total = 0_usize;
        for (index, (name, child)) in root.children.iter().enumerate() {
            total = total.checked_add(child.length)?;
            if name.len() > 1024 || usize::from(child.index) != index + 1 || child.length == 0 {
                return None;
            }
        }
        (total <= MAX_PROJECTION_BYTES && total as u64 <= length).then_some(root)
    }
}

pub(super) struct Fields<'input>(pub BTreeMap<String, &'input RawValue>);

impl<'input> Deserialize<'input> for Fields<'input> {
    fn deserialize<Parser>(parser: Parser) -> Result<Self, Parser::Error>
    where
        Parser: serde::Deserializer<'input>,
    {
        struct FieldVisitor;
        impl<'input> serde::de::Visitor<'input> for FieldVisitor {
            type Value = Fields<'input>;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("a bounded metadata object with unique field names")
            }

            fn visit_map<Map>(self, mut map: Map) -> Result<Self::Value, Map::Error>
            where
                Map: serde::de::MapAccess<'input>,
            {
                let mut fields = BTreeMap::new();
                while let Some(name) = map.next_key::<String>()? {
                    if name.len() > 1024 || fields.len() == MAX_CHILDREN || fields.contains_key(&name) {
                        return Err(serde::de::Error::custom(
                            "projection field bounds or duplicate name",
                        ));
                    }
                    fields.insert(name, map.next_value::<&RawValue>()?);
                }
                Ok(Fields(fields))
            }
        }
        parser.deserialize_map(FieldVisitor)
    }
}
