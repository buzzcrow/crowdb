// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
use super::{ct_resource_usage_v1, ct_root_catalog, ct_runtime};
use std::ffi::c_void;
#[repr(C)]
pub struct ct_chunk_pin_service {
    _private: [u8; 0],
}
#[repr(C)]
#[derive(Clone, Copy, Default)]
pub struct ct_chunk_pin_service_step {
    pub completed: usize,
    pub failed: usize,
    pub more: u8,
    pub drained: u8,
}
unsafe extern "C" {
    pub fn ct_chunk_pin_service_create(
        runtime: *mut ct_runtime,
        capacity: usize,
        wake: unsafe extern "C" fn(*mut c_void),
        drop_context: unsafe extern "C" fn(*mut c_void),
        context: *mut c_void,
        out: *mut *mut ct_chunk_pin_service,
    ) -> i32;
    pub fn ct_root_catalog_bind_pin_service(
        catalog: *mut ct_root_catalog,
        service: *mut ct_chunk_pin_service,
    ) -> i32;
    pub fn ct_chunk_pin_service_pump(
        service: *mut ct_chunk_pin_service,
        limit: usize,
    ) -> ct_chunk_pin_service_step;
    pub fn ct_chunk_pin_service_stop(service: *mut ct_chunk_pin_service);
    pub fn ct_chunk_pin_service_resource_usage(
        service: *const ct_chunk_pin_service,
        out: *mut ct_resource_usage_v1,
    ) -> i32;
    pub fn ct_chunk_pin_service_free(service: *mut ct_chunk_pin_service);
}
