// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_protocol::chunk_allocation_geometry::uniform_allocation_unit;

#[test]
fn strip_geometry_requires_equal_physical_units() {
    assert_eq!(uniform_allocation_unit([]), Ok(None));
    assert_eq!(uniform_allocation_unit([131_072; 3]), Ok(Some(131_072)));
    for units in [[131_072, 1_048_576], [1_048_576, 131_072]] {
        let error = uniform_allocation_unit(units).unwrap_err();
        assert_eq!((error.first_bytes, error.other_bytes), (units[0], units[1]));
    }
}
