// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_access_s3::retrieval::{prepare_get_range, RetrievalError};
use crowdb_access_s3::S3ErrorCode;

use super::{ByteRange, ObjectBody, ObjectMetadata, ReadPreconditions, ResolvedObject, S3Client};
use crate::runtime::RequestLease;
use crate::{Error, OperationContext, Result};

#[derive(Clone, Debug, Default)]
pub struct GetOptions {
    pub range: Option<ByteRange>,
    pub conditions: ReadPreconditions,
}

pub struct GetObject {
    pub metadata: ObjectMetadata,
    pub range: ByteRange,
    pub body: ObjectBody,
}

impl S3Client {
    /// Reads metadata and conditions without requesting payload storage.
    /// # Errors
    /// Returns admission, missing object, conditional or metadata failures.
    pub async fn head_object(
        &self,
        context: OperationContext,
        bucket: &[u8],
        key: &[u8],
        conditions: &ReadPreconditions,
    ) -> Result<ObjectMetadata> {
        let request = self.runtime.resources.admit(context)?;
        request
            .run(async {
                let selected = self.select_object(bucket, key).await?;
                self.execution
                    .prepare_head(&selected.record, conditions)
                    .map_err(|error| retrieval_error(&error))?;
                Ok(selected.metadata().clone())
            })
            .await
    }

    /// Selects one published generation and returns a lazy verified body.
    /// # Errors
    /// Returns admission, namespace, conditional or range failures before delivery.
    pub async fn get_object(
        &self,
        context: OperationContext,
        bucket: &[u8],
        key: &[u8],
        options: &GetOptions,
    ) -> Result<GetObject> {
        let request = self.runtime.resources.admit(context)?;
        let selected = request.run(self.select_object(bucket, key)).await?;
        self.prepare_read(request, &selected, options)
    }

    /// Reads exactly the selected record, even after its name is overwritten.
    /// # Errors
    /// Also rejects selections from a different tenant or storage authority.
    pub fn get_resolved(
        &self,
        context: OperationContext,
        selected: &ResolvedObject,
        options: &GetOptions,
    ) -> Result<GetObject> {
        let request = self.runtime.resources.admit(context)?;
        self.prepare_read(request, selected, options)
    }

    fn prepare_read(
        &self,
        request: RequestLease,
        selected: &ResolvedObject,
        options: &GetOptions,
    ) -> Result<GetObject> {
        self.validate_selection(selected)?;
        request.context().check()?;
        let prepared = prepare_get_range(
            self.execution.chunks(),
            &selected.record,
            options.range,
            &options.conditions,
        )
        .map_err(|error| retrieval_error(&error))?;
        let range = options.range.unwrap_or(ByteRange {
            start: 0,
            end: selected.record.logical_length,
        });
        Ok(GetObject {
            metadata: selected.metadata().clone(),
            range,
            body: ObjectBody::new(prepared.stream, request, self.runtime.resources.clone(), range),
        })
    }
}

fn retrieval_error(error: &RetrievalError) -> Error {
    match error {
        RetrievalError::NotModified => Error::NotModified,
        RetrievalError::PreconditionFailed => Error::S3(S3ErrorCode::PreconditionFailed),
        RetrievalError::Range(_) => Error::S3(S3ErrorCode::InvalidRange),
        RetrievalError::DataReference | RetrievalError::ChunkRead => Error::S3(S3ErrorCode::InternalError),
    }
}
