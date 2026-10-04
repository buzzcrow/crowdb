// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::ChunkKvService;
use crowdb_protocol::chunk_kv::Id128;
use serde::Deserialize;
use serde_json::{json, Value};
use std::sync::Arc;

#[derive(Default, Deserialize)]
pub(crate) struct PageQuery {
    pub page_path: Option<String>,
    pub tree_version: Option<u64>,
    pub page_fingerprint: Option<u32>,
    #[serde(default)]
    pub entry_offset: usize,
}

impl ChunkKvService {
    pub(crate) fn observe_page(&self, id: Id128, query: &PageQuery) -> Result<Value, &'static str> {
        let raw = query.page_path.as_deref().ok_or("invalid_page_path")?;
        if raw.len() > 352
            || query.tree_version == Some(u64::MAX)
            || query.entry_offset > 131_072
            || query.entry_offset > 0 && (query.tree_version.is_none() || query.page_fingerprint.is_none())
        {
            return Err("invalid_page_cursor");
        }
        let path: Vec<u32> = if raw.is_empty() {
            Vec::new()
        } else {
            raw.split('.')
                .map(|part| part.parse().map_err(|_| "invalid_page_path"))
                .collect::<Result<_, _>>()?
        };
        if path.len() > 32 || !path.is_empty() && query.tree_version.is_none() {
            return Err("invalid_page_path");
        }
        let hosted = self.partitions.load_full();
        let partition = hosted.get(&id).ok_or("partition_not_hosted")?;
        let page = partition
            .inspect_page(&path, query.tree_version)
            .map_err(|error| match error {
                crowdb_chunk_kv::ChunkKvError::RequestConflict => "page_changed",
                crowdb_chunk_kv::ChunkKvError::TreeCorruption(_) => "page_corrupt",
                crowdb_chunk_kv::ChunkKvError::InvalidRequest(_) => "invalid_page_request",
                crowdb_chunk_kv::ChunkKvError::Overloaded => "page_bounds",
                _ => "page_io_unavailable",
            })?;
        let fingerprint = page.fingerprint();
        if query
            .page_fingerprint
            .is_some_and(|expected| expected != fingerprint)
            || query.entry_offset > page.len()
        {
            return Err("page_changed");
        }
        let end = page.len().min(query.entry_offset + 20);
        let rows = (query.entry_offset..end).map(|index| {
            let entry = page.entry(index).map_err(|_| "invalid_page_frame")?;
            let cell = entry.cell.map(cell).transpose()?;
            Ok(json!({ "index":index, "key":entry.key.map(preview), "child":entry.child.map(|v|v.to_string()), "cell":cell, "inline_delta":entry.inline_delta }))
        }).collect::<Result<Vec<_>, &'static str>>()?;
        if !Arc::ptr_eq(&hosted, &self.partitions.load_full()) {
            return Err("writer_changed");
        }
        Ok(
            json!({ "version":page.version.to_string(), "root":page.root.to_string(), "id":page.page.to_string(),
            "kind":if page.inner {"inner"} else {"leaf"}, "fingerprint":fingerprint,
            "path":raw, "frame_bytes":page.frame_bytes(), "pending_delta_pages":page.deltas,
            "base_frames_inspected":path.len() + 1, "overflow_values_read":0,
            "entries":page.len(), "offset":query.entry_offset, "next":(end < page.len()).then_some(end), "rows":rows }),
        )
    }
}

fn preview(bytes: &[u8]) -> Value {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let prefix = &bytes[..bytes.len().min(256)];
    let mut hex = String::with_capacity(prefix.len() * 2);
    for byte in prefix {
        hex.push(char::from(HEX[usize::from(byte >> 4)]));
        hex.push(char::from(HEX[usize::from(byte & 15)]));
    }
    json!({ "hex":hex, "bytes":bytes.len(), "truncated":prefix.len() < bytes.len(),
        "text":std::str::from_utf8(prefix).ok() })
}

fn cell(bytes: &[u8]) -> Result<Value, &'static str> {
    if bytes.len() < 9 {
        return Err("invalid_page_cell");
    }
    let sequence = u64::from_le_bytes(bytes[..8].try_into().map_err(|_| "invalid_page_cell")?);
    let overflow = bytes[8] & 2 != 0;
    if overflow && bytes.len() != 25 {
        return Err("invalid_page_cell");
    }
    let word = |offset| {
        u64::from_le_bytes(
            bytes[offset..offset + 8]
                .try_into()
                .expect("validated cell length"),
        )
        .to_string()
    };
    Ok(
        json!({ "sequence":sequence.to_string(), "tombstone":bytes[8] & 1 != 0,
        "overflow_page":overflow.then(||word(9)), "overflow_bytes":overflow.then(||word(17)),
        "value":(!overflow).then(||preview(&bytes[9..])) }),
    )
}
