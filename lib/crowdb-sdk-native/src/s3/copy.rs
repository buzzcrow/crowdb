// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{CopyMetadata, ObjectMetadata, ReadPreconditions, ResolvedObject, S3Client};
use crate::{Error, OperationContext, Result};
use crowdb_access_s3::condition::{ConditionOutcome, ReadCondition};
use crowdb_access_s3::execution::CopyFailure;
use crowdb_access_s3::S3ErrorCode;

#[derive(Default)]
pub struct CopyOptions {
    pub source_conditions: ReadPreconditions,
    /// None preserves source metadata; Some replaces it and allows self-copy.
    pub metadata: Option<CopyMetadata>,
}

impl S3Client {
    /// Selects one source generation, then copies it through the shared engine.
    /// # Errors
    /// Returns namespace, condition, storage or uncertain publication failure.
    pub async fn copy_object(
        &self,
        context: OperationContext,
        source_bucket: &[u8],
        source_key: &[u8],
        destination_bucket: &[u8],
        destination_key: &[u8],
        options: CopyOptions,
    ) -> Result<ObjectMetadata> {
        let source = self
            .resolve_object(context.clone(), source_bucket, source_key)
            .await?;
        Box::pin(self.copy_resolved(context, &source, destination_bucket, destination_key, options)).await
    }

    /// Copies the retained source generation even after its mutable name changes.
    /// # Errors
    /// Returns authority/condition, storage or uncertain publication failure.
    pub async fn copy_resolved(
        &self,
        context: OperationContext,
        source: &ResolvedObject,
        destination_bucket: &[u8],
        destination_key: &[u8],
        options: CopyOptions,
    ) -> Result<ObjectMetadata> {
        self.validate_selection(source)?;
        if options.source_conditions.evaluate(&source.record) != ConditionOutcome::Proceed {
            return Err(Error::S3(S3ErrorCode::PreconditionFailed));
        }
        let id = context.request_id.get();
        let request = self.runtime.resources.admit(context)?;
        let destination = request
            .run(async {
                self.execution
                    .resolve_bucket(destination_bucket)
                    .await
                    .map_err(Error::S3)
            })
            .await?;
        let selected = request
            .run_mutation(async {
                self.execution
                    .copy_object_record(
                        &source.record,
                        destination,
                        destination_key,
                        options.metadata,
                        &self.write_policy,
                    )
                    .await
                    .map_err(|error| match error {
                        CopyFailure::S3(code) => Error::S3(code),
                        CopyFailure::Publication(outcome) => super::write::map_outcome(outcome, id),
                    })
            })
            .await?;
        Ok(self.bind_object(selected)?.metadata().clone())
    }
}
