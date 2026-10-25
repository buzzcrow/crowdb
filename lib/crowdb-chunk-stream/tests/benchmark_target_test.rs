// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[path = "../benches/production_bounds/config.rs"]
mod config;

#[test]
fn production_measurement_requires_an_explicit_existing_target() {
    let error = config::BenchConfig::parse(std::iter::empty()).unwrap_err();
    assert!(error.contains("--management-seed is required"), "{error}");
    let configured = config::BenchConfig::parse(
        ["--management-seed", "http://127.0.0.1:19347"]
            .into_iter()
            .map(str::to_owned),
    )
    .unwrap();
    assert_eq!(configured.management_seed, "http://127.0.0.1:19347");
}
