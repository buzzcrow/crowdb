// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Project-local test directories and retained process ownership.
//!
//! Formal server lifecycle and fixed container endpoints live in the root E2E
//! runner. Component tests share only runtime directories and cleanup guards.

#![allow(
    clippy::must_use_candidate,
    clippy::doc_markdown,
    clippy::missing_panics_doc,
    clippy::missing_errors_doc
)]

pub mod test_dirs;
