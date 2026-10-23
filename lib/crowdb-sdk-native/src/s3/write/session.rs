// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crate::{Error, Result};
use crowdb_access_s3::execution::{ObjectWriter, PreparedPart, PreparedPut};
use crowdb_access_s3::metadata::MultipartPartRecord;
use crowdb_access_s3::object::SelectedObject;

pub(super) enum UploadSession {
    Object(PreparedPut),
    Part(PreparedPart),
}

pub(super) enum UploadResult {
    Object(SelectedObject),
    Part(MultipartPartRecord),
}

impl UploadSession {
    pub fn writer_mut(&mut self) -> &mut ObjectWriter {
        match self {
            Self::Object(session) => session.writer_mut(),
            Self::Part(session) => session.writer_mut(),
        }
    }

    pub fn is_large(&self) -> bool {
        match self {
            Self::Object(session) => session.is_large(),
            Self::Part(session) => session.is_large(),
        }
    }

    pub async fn abort(&mut self) {
        match self {
            Self::Object(session) => session.abort().await,
            Self::Part(session) => session.abort().await,
        }
    }

    pub async fn finish(self, etag: String, checksum: Vec<u8>, request_id: u64) -> Result<UploadResult> {
        match self {
            Self::Object(session) => Box::pin(session.finish(etag, checksum))
                .await
                .map(UploadResult::Object)
                .map_err(|outcome| super::map_outcome(outcome, request_id)),
            Self::Part(session) => Box::pin(session.finish(checksum))
                .await
                .map(UploadResult::Part)
                .map_err(|error| crate::s3::multipart::map_error(error, request_id)),
        }
    }
}

impl UploadResult {
    pub fn object(self) -> Result<SelectedObject> {
        match self {
            Self::Object(object) => Ok(object),
            Self::Part(_) => Err(Error::Configuration("part writer used as object writer".into())),
        }
    }

    pub fn part(self) -> Result<MultipartPartRecord> {
        match self {
            Self::Part(part) => Ok(part),
            Self::Object(_) => Err(Error::Configuration("object writer used as part writer".into())),
        }
    }
}
