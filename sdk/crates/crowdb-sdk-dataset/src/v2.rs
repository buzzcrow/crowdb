// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Typed Dataset control v2. All integer wire values preserve their exact bits.

mod client;
mod composition;
mod identity;
mod inspection;
mod lifecycle;
mod paging;
mod read;
mod requests;
mod split;
mod state;
mod transfer;
mod upload;

pub use client::ControlClient;
pub use composition::*;
pub use identity::{Decimal, Generation, OpaqueId, SampleId};
pub use inspection::*;
pub use paging::{PageCursor, PageKind};
pub use read::*;
pub use split::*;
pub use state::*;
pub use upload::*;

fn required_option<'de, D: serde::Deserializer<'de>, T: serde::Deserialize<'de>>(
    d: D,
) -> Result<Option<T>, D::Error> {
    <Option<T> as serde::Deserialize>::deserialize(d)
}
