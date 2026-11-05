// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::LegacyRetirementPending;
use crowdb_protocol::chunkdb::rpc::Chunk;
use crowdb_tree_ffi::CtError;
use serde::{Deserialize, Serialize};

pub(crate) const NAMESPACE_KEY: &[u8] = b"\0crowdb/chunk-kv/legacy-retirement/v1/state";
pub(super) const ROOT_PREFIX: &[u8] = b"\0crowdb/chunk-kv/root/v1/";

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(super) struct Namespace {
    pub version: u32,
    pub phase: Phase,
    pub after: Vec<u8>,
    pub pack_index: usize,
    pub proof_after: Vec<u8>,
    pub pins_done: bool,
    pub owners_done: bool,
    pub pending: Option<LegacyRetirementPending>,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(super) enum Phase {
    Proving,
    Retiring,
    Complete,
}
impl Default for Namespace {
    fn default() -> Self {
        Self {
            version: 1,
            phase: Phase::Proving,
            after: Vec::new(),
            pack_index: 0,
            proof_after: Vec::new(),
            pins_done: false,
            owners_done: false,
            pending: None,
        }
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub(super) struct AllocationReceipt {
    pub version: u32,
    pub allocation: Chunk,
    pub physical_bytes: u64,
    pub complete: bool,
}

pub(crate) fn is_legacy(bytes: &[u8]) -> bool {
    bytes.get(..4) == Some(&7_u32.to_le_bytes())
}
pub(super) fn key(tree: u64, kind: &[u8], generation: u64) -> Vec<u8> {
    let mut key = ROOT_PREFIX.to_vec();
    key.extend_from_slice(&tree.to_be_bytes());
    key.push(b'/');
    key.extend_from_slice(kind);
    key.push(b'/');
    key.extend_from_slice(&generation.to_be_bytes());
    key
}
pub(super) fn parse_key(bytes: &[u8]) -> Result<(u64, &[u8], u64), CtError> {
    let tail = bytes.strip_prefix(ROOT_PREFIX).ok_or(CtError::Corruption)?;
    if tail.len() < 18 || tail[8] != b'/' {
        return Err(CtError::Corruption);
    }
    let end = tail[9..]
        .iter()
        .position(|byte| *byte == b'/')
        .ok_or(CtError::Corruption)?
        + 9;
    if tail.len() < end + 9 {
        return Err(CtError::Corruption);
    }
    Ok((
        u64::from_be_bytes(tail[..8].try_into().map_err(|_| CtError::Corruption)?),
        &tail[9..end],
        u64::from_be_bytes(
            tail[end + 1..end + 9]
                .try_into()
                .map_err(|_| CtError::Corruption)?,
        ),
    ))
}
pub(super) fn allocation_key(high: u64, low: u64) -> Vec<u8> {
    let mut key = b"\0crowdb/chunk-kv/legacy-retirement/v1/allocation/".to_vec();
    key.extend_from_slice(&high.to_be_bytes());
    key.extend_from_slice(&low.to_be_bytes());
    key
}
pub(super) fn encode<T: Serialize>(value: &T) -> Result<Vec<u8>, CtError> {
    serde_json::to_vec(value).map_err(|_| CtError::Internal)
}
pub(super) fn decode<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> Result<T, CtError> {
    serde_json::from_slice(bytes).map_err(|_| CtError::Corruption)
}

// This checks the immutable format envelope. The production recovery decoder
// additionally validates every directory entry before opening a typed root.
pub(super) fn typed_root(bytes: &[u8], tree: u64, generation: u64) -> Result<(), CtError> {
    const HEADER: usize = 128;
    const ENTRY: usize = 96;
    if bytes.len() < HEADER + 4 {
        return Err(CtError::Corruption);
    }
    let word = |index: usize| {
        u64::from_le_bytes(
            bytes[index * 8..index * 8 + 8]
                .try_into()
                .expect("header checked"),
        )
    };
    let crc = u32::from_le_bytes(
        bytes[bytes.len() - 4..]
            .try_into()
            .map_err(|_| CtError::Corruption)?,
    );
    let entries = usize::try_from(word(13)).map_err(|_| CtError::Corruption)?;
    if word(0) != 0x3354_4F4F_524B_4843
        || word(1) != 1
        || word(2) != tree
        || word(3) <= generation
        || entries != (bytes.len() - HEADER - 4) / ENTRY
        || (bytes.len() - HEADER - 4) % ENTRY != 0
        || crowdb_tree_ffi::crc32c(&bytes[..bytes.len() - 4]) != crc
    {
        return Err(CtError::Corruption);
    }
    Ok(())
}
