// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Group-0 keys for monitor descriptors, catalogs, and serving grants.

use super::encoding::{
    check_path_exact, decode_path_u64, encode_path_header, encode_path_u64, KeyError, TextKey,
};

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct DomainMonitorKey {
    pub domain: String,
}

impl TextKey for DomainMonitorKey {
    const PATH_MAGIC: &'static str = "/monitor";
    const PATH_TYPE: &'static str = "domain";

    fn encode_to_path(&self, out: &mut String) {
        encode_path_header(out, Self::PATH_MAGIC, Self::PATH_TYPE);
        out.push('/');
        out.push_str(&self.domain);
    }

    fn decode_path(parts: &[&str]) -> Result<Self, KeyError> {
        check_path_exact(parts, 1)?;
        let domain = parts.first().ok_or(KeyError::ShortInput)?;
        if domain.is_empty() {
            return Err(KeyError::ShortInput);
        }
        Ok(Self {
            domain: (*domain).to_string(),
        })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct ChunkKvCatalogHeadKey;

impl TextKey for ChunkKvCatalogHeadKey {
    const PATH_MAGIC: &'static str = "/chunk-kv";
    const PATH_TYPE: &'static str = "catalog-head";

    fn encode_to_path(&self, out: &mut String) {
        encode_path_header(out, Self::PATH_MAGIC, Self::PATH_TYPE);
    }

    fn decode_path(parts: &[&str]) -> Result<Self, KeyError> {
        check_path_exact(parts, 0)?;
        Ok(Self)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct ChunkKvCatalogPageKey {
    pub generation: u64,
    pub page_index: u64,
}

impl TextKey for ChunkKvCatalogPageKey {
    const PATH_MAGIC: &'static str = "/chunk-kv";
    const PATH_TYPE: &'static str = "catalog-page";

    fn encode_to_path(&self, out: &mut String) {
        encode_path_header(out, Self::PATH_MAGIC, Self::PATH_TYPE);
        encode_path_u64(out, self.generation);
        encode_path_u64(out, self.page_index);
    }

    fn decode_path(parts: &[&str]) -> Result<Self, KeyError> {
        if parts.len() < 2 {
            return Err(KeyError::ShortInput);
        }
        let generation = decode_path_u64(parts[0])?;
        let page_index = decode_path_u64(parts[1])?;
        check_path_exact(parts, 2)?;
        Ok(Self {
            generation,
            page_index,
        })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct ServingGrantKey {
    pub instance_id: u64,
}

impl TextKey for ServingGrantKey {
    const PATH_MAGIC: &'static str = "/chunk-kv";
    const PATH_TYPE: &'static str = "serving-grant";

    fn encode_to_path(&self, out: &mut String) {
        encode_path_header(out, Self::PATH_MAGIC, Self::PATH_TYPE);
        encode_path_u64(out, self.instance_id);
    }

    fn decode_path(parts: &[&str]) -> Result<Self, KeyError> {
        let instance_id = decode_path_u64(parts.first().ok_or(KeyError::ShortInput)?)?;
        check_path_exact(parts, 1)?;
        Ok(Self { instance_id })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn group_zero_keys_round_trip() {
        let domain = DomainMonitorKey {
            domain: "chunk-kv".into(),
        };
        assert_eq!(DomainMonitorKey::from_path(&domain.to_path()).unwrap(), domain);
        assert_eq!(
            ChunkKvCatalogHeadKey::from_path(&ChunkKvCatalogHeadKey.to_path()).unwrap(),
            ChunkKvCatalogHeadKey
        );
        let page = ChunkKvCatalogPageKey {
            generation: 3,
            page_index: 4,
        };
        assert_eq!(ChunkKvCatalogPageKey::from_path(&page.to_path()).unwrap(), page);
        let grant = ServingGrantKey { instance_id: 5 };
        assert_eq!(ServingGrantKey::from_path(&grant.to_path()).unwrap(), grant);
    }
}
