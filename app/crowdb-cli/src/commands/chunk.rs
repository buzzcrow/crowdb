// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! `chunk` domain — chunk-disk allocation and chunk stub service
//! management.

pub mod diskdb;
pub mod stub;

pub(crate) use diskdb::{run_chunk_diskdb_verb, ChunkDiskdbVerb};
pub(crate) use stub::{run_chunk_stub_verb, ChunkStubVerb};
