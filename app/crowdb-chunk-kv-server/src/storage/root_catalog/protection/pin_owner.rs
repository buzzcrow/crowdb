// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::record::{decode_words, encode_words};
use crowdb_protocol::chunk_stream::{StreamProcessIdentity, StreamPurpose};
use crowdb_tree_ffi::CtError;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct PinOwner {
    pub live: bool,
    pub process: Option<StreamProcessIdentity>,
}
impl PinOwner {
    pub fn encode(self) -> Vec<u8> {
        let mut words = [1, u64::from(self.live), 0, 0, 0, 0, 0, 0];
        if let Some(identity) = self.process {
            words[2] = 1;
            words[3] = identity.instance_id;
            words[4] = identity.process_epoch;
            words[5] = u64::from_le_bytes(identity.boot_nonce[..8].try_into().expect("nonce bounds"));
            words[6] = u64::from_le_bytes(identity.boot_nonce[8..].try_into().expect("nonce bounds"));
        }
        encode_words(&words)
    }
    pub fn decode(bytes: &[u8]) -> Result<Self, CtError> {
        if bytes.len() == 12 {
            let [live] = decode_words::<1>(bytes)?;
            if live > 1 {
                return Err(CtError::Corruption);
            }
            return Ok(Self {
                live: live != 0,
                process: None,
            });
        }
        let words = decode_words::<8>(bytes)?;
        if words[0] != 1 || words[1] > 1 || words[2] > 1 || words[7] != 0 {
            return Err(CtError::Corruption);
        }
        let process = if words[2] == 0 {
            if words[3..7] != [0, 0, 0, 0] {
                return Err(CtError::Corruption);
            }
            None
        } else {
            if words[3] == 0 || words[4] == 0 || words[5] == 0 && words[6] == 0 {
                return Err(CtError::Corruption);
            }
            let mut boot_nonce = [0; 16];
            boot_nonce[..8].copy_from_slice(&words[5].to_le_bytes());
            boot_nonce[8..].copy_from_slice(&words[6].to_le_bytes());
            Some(StreamProcessIdentity {
                purpose: StreamPurpose::Wal,
                instance_id: words[3],
                process_epoch: words[4],
                boot_nonce,
            })
        };
        Ok(Self {
            live: words[1] != 0,
            process,
        })
    }
}
