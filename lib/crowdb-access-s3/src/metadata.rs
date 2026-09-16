// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Durable S3 namespace metadata.

mod key;
mod namespace;
mod record;
mod store;

mod generated {
    #![allow(
        unsafe_code,
        clippy::all,
        clippy::pedantic,
        dead_code,
        non_camel_case_types,
        non_snake_case
    )]
    include!(concat!(env!("OUT_DIR"), "/s3_metadata_generated.rs"));
}

pub use key::{BucketId, MetadataKey, MetadataKeyError, TenantId};
pub use namespace::{BucketDeleteOutcome, BucketNamespace, BucketNamespaceError};
pub use record::{BucketNameRecord, MetadataRecordError, ObjectRecord};
pub use store::{ChunkKvMetadataStore, MetadataStoreError, PutIfAbsentOutcome};
