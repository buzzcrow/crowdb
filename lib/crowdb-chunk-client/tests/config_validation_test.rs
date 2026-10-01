// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_chunk_client::ChunkClientConfig;

#[test]
fn large_mirror_copy_count_is_bounded_to_five() {
    let mut config = ChunkClientConfig {
        large_mirror_copies: Some(5),
        ..ChunkClientConfig::default()
    };
    assert!(config.validate().is_ok());

    config.large_mirror_copies = Some(6);
    assert!(config.validate().is_err());

    config.large_mirror_copies = Some(0);
    assert!(config.validate().is_err());
}
