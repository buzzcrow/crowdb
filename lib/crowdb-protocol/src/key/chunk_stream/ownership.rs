// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Fixed Group-0 owner keys; process epochs remain in conditional values.

use super::super::encoding::{check_path_exact, decode_path_u64, encode_path_header, encode_path_u64};
use crate::{
    chunk_stream::StreamPurpose,
    key::{KeyError, TextKey},
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StreamProcessLeaseKey {
    pub purpose: StreamPurpose,
    pub instance_id: u64,
}

impl TextKey for StreamProcessLeaseKey {
    const PATH_MAGIC: &'static str = "/stream-owner";
    const PATH_TYPE: &'static str = "process";

    fn encode_to_path(&self, out: &mut String) {
        encode_path_header(out, Self::PATH_MAGIC, Self::PATH_TYPE);
        encode_path_u64(out, u64::from(self.purpose == StreamPurpose::Wal));
        encode_path_u64(out, self.instance_id);
    }

    fn decode_path(parts: &[&str]) -> Result<Self, KeyError> {
        check_path_exact(parts, 2)?;
        let purpose = match decode_path_u64(parts[0])? {
            0 => StreamPurpose::Stream,
            1 => StreamPurpose::Wal,
            _ => return Err(KeyError::BadTag),
        };
        Ok(Self {
            purpose,
            instance_id: decode_path_u64(parts[1])?,
        })
    }
}
