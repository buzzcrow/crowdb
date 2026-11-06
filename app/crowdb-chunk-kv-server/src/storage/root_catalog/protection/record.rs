// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_tree_ffi::chunk::protection::{CandidateState, CandidateStatus, MAX_CANDIDATE_REFERENCES};
use crowdb_tree_ffi::chunk::stage::StageId;
use crowdb_tree_ffi::{crc32c, CtError};

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct CandidateRecord {
    pub status: CandidateStatus,
    pub expected_checksum: u32,
    pub prepared_checksum: u32,
    pub manifest_checksum: u32,
    pub manifest_bytes: u64,
    pub last_stage: StageId,
}

impl CandidateRecord {
    pub fn encode(&self) -> Vec<u8> {
        encode_words(&[
            1,
            self.status.state as u64,
            self.status.generation,
            self.status.reference_count,
            self.status.prepared_references,
            self.status.prepared_windows,
            self.status.released_windows,
            self.expected_checksum.into(),
            self.prepared_checksum.into(),
            self.manifest_checksum.into(),
            self.manifest_bytes,
            self.last_stage.owner_epoch,
            self.last_stage.operation_high,
            self.last_stage.operation_low,
        ])
    }
    pub fn decode(id: StageId, bytes: &[u8]) -> Result<Self, CtError> {
        let words = decode_words::<14>(bytes)?;
        let state = match words[1] {
            1 => CandidateState::Preparing,
            2 => CandidateState::Ready,
            3 => CandidateState::Committed,
            4 => CandidateState::Aborted,
            5 => CandidateState::Reclaimed,
            _ => return Err(CtError::Corruption),
        };
        if words[0] != 1
            || words[2] == 0
            || words[3] > MAX_CANDIDATE_REFERENCES
            || words[4] > words[3]
            || words[5] > words[4]
            || words[6] > words[5]
        {
            return Err(CtError::Corruption);
        }
        Ok(Self {
            status: CandidateStatus {
                id,
                state,
                generation: words[2],
                reference_count: words[3],
                prepared_references: words[4],
                prepared_windows: words[5],
                released_windows: words[6],
            },
            expected_checksum: u32::try_from(words[7]).map_err(|_| CtError::Corruption)?,
            prepared_checksum: u32::try_from(words[8]).map_err(|_| CtError::Corruption)?,
            manifest_checksum: u32::try_from(words[9]).map_err(|_| CtError::Corruption)?,
            manifest_bytes: words[10],
            last_stage: StageId {
                owner_epoch: words[11],
                operation_high: words[12],
                operation_low: words[13],
            },
        })
    }
}

pub(super) fn encode_words(words: &[u64]) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(8 * words.len() + 4);
    for value in words {
        bytes.extend_from_slice(&value.to_le_bytes());
    }
    bytes.extend_from_slice(&crc32c(&bytes).to_le_bytes());
    bytes
}

pub(super) fn decode_words<const N: usize>(bytes: &[u8]) -> Result<[u64; N], CtError> {
    if bytes.len() != N * 8 + 4 {
        return Err(CtError::Corruption);
    }
    let checksum = u32::from_le_bytes(bytes[N * 8..].try_into().map_err(|_| CtError::Corruption)?);
    if crc32c(&bytes[..N * 8]) != checksum {
        return Err(CtError::Corruption);
    }
    Ok(std::array::from_fn(|index| {
        u64::from_le_bytes(
            bytes[index * 8..index * 8 + 8]
                .try_into()
                .expect("fixed record bounds"),
        )
    }))
}

pub(super) fn operation_key(tree: u64, kind: &[u8], id: StageId) -> Vec<u8> {
    let mut key = super::catalog_key(tree, kind, id.owner_epoch);
    key.extend_from_slice(&id.operation_high.to_be_bytes());
    key.extend_from_slice(&id.operation_low.to_be_bytes());
    key
}

pub(super) fn window_key(tree: u64, id: StageId, window: u64) -> Vec<u8> {
    let mut key = operation_key(tree, b"candidate-window", id);
    key.extend_from_slice(&window.to_be_bytes());
    key
}

pub(super) fn encode_references(refs: &[StageId]) -> Vec<u8> {
    let words = refs
        .iter()
        .flat_map(|id| [id.owner_epoch, id.operation_high, id.operation_low])
        .collect::<Vec<_>>();
    encode_words(&words)
}

pub(super) fn decode_references(bytes: &[u8]) -> Result<Vec<StageId>, CtError> {
    if bytes.len() < 28 || (bytes.len() - 4) % 24 != 0 || bytes.len() > 64 * 24 + 4 {
        return Err(CtError::Corruption);
    }
    let payload = &bytes[..bytes.len() - 4];
    if crc32c(payload)
        != u32::from_le_bytes(
            bytes[bytes.len() - 4..]
                .try_into()
                .map_err(|_| CtError::Corruption)?,
        )
    {
        return Err(CtError::Corruption);
    }
    let refs = payload
        .chunks_exact(24)
        .map(|chunk| {
            let at =
                |offset| u64::from_le_bytes(chunk[offset..offset + 8].try_into().expect("reference bounds"));
            StageId {
                owner_epoch: at(0),
                operation_high: at(8),
                operation_low: at(16),
            }
        })
        .collect::<Vec<_>>();
    if refs.windows(2).any(|pair| pair[0] >= pair[1]) {
        return Err(CtError::Corruption);
    }
    Ok(refs)
}
