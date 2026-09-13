// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_diskio_client::{DiskId, DiskioError, SegmentTarget};

#[test]
fn segment_target_rejects_invalid_arithmetic_before_io() {
    let disk = DiskId::new(1, 2);
    assert!(matches!(
        SegmentTarget::new(disk, 0, 0, 1, 0),
        Err(DiskioError::InvalidInput(_))
    ));
    assert!(matches!(
        SegmentTarget::new(disk, 0, 0, 0, 4096),
        Err(DiskioError::InvalidInput(_))
    ));
    assert!(matches!(
        SegmentTarget::new(disk, 0, u64::MAX, 1, 4096),
        Err(DiskioError::InvalidInput(_))
    ));

    let target = SegmentTarget::new(disk, 3, 8, 2, 4096).expect("valid target");
    assert!(target.validate_range(4096, 4096).is_ok());
    assert!(matches!(
        target.validate_range(8192, 1),
        Err(DiskioError::InvalidInput(_))
    ));
    assert!(matches!(
        target.validate_range(u64::MAX, 2),
        Err(DiskioError::InvalidInput(_))
    ));
}
