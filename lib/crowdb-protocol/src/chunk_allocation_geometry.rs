// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Physical allocation geometry supported by the shared Strip unit field.

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("Chunk placement requires a uniform allocation unit; found {first_bytes} and {other_bytes} bytes")]
pub struct MixedAllocationUnits {
    pub first_bytes: u32,
    pub other_bytes: u32,
}

/// Return the common physical unit, or reject incompatible Strip geometry.
/// Empty topology has no known unit.
///
/// # Errors
/// Returns the conflicting sizes when physical allocation units differ.
pub fn uniform_allocation_unit(
    units: impl IntoIterator<Item = u32>,
) -> Result<Option<u32>, MixedAllocationUnits> {
    let mut units = units.into_iter();
    let Some(first_bytes) = units.next() else {
        return Ok(None);
    };
    for other_bytes in units {
        if other_bytes != first_bytes {
            return Err(MixedAllocationUnits {
                first_bytes,
                other_bytes,
            });
        }
    }
    Ok(Some(first_bytes))
}
