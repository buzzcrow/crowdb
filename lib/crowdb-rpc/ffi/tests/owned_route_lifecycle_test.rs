// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[path = "common/owned_route.rs"]
mod owned_route;

use owned_route::{check_evicted_route, TestReply};

#[test]
fn evicted_route_releases_last_owners_after_response_callback() {
    check_evicted_route(TestReply::Success);
}

#[test]
fn evicted_route_releases_last_owners_after_close_callback() {
    check_evicted_route(TestReply::Close);
}

#[test]
fn evicted_route_releases_last_owners_after_timeout_callback() {
    check_evicted_route(TestReply::Timeout);
}
