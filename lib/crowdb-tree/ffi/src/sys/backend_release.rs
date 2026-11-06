// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
use super::ct_tree;
use std::ffi::c_void;
#[repr(C)]
pub struct ct_backend_release_service {
    _private: [u8; 0],
}
#[repr(C)]
#[derive(Clone, Copy, Default)]
pub struct ct_backend_release_step {
    pub visited: usize,
    pub completed: usize,
    pub more: u8,
    pub drained: u8,
}
unsafe extern "C" {
    pub fn ct_tree_backend_release_service(
        tree: *mut ct_tree,
        wake: unsafe extern "C" fn(*mut c_void),
        drop_context: unsafe extern "C" fn(*mut c_void),
        context: *mut c_void,
        out: *mut *mut ct_backend_release_service,
    ) -> i32;
    pub fn ct_backend_release_service_pump(
        service: *mut ct_backend_release_service,
        limit: usize,
    ) -> ct_backend_release_step;
    pub fn ct_backend_release_service_stop(service: *mut ct_backend_release_service);
    pub fn ct_backend_release_service_free(service: *mut ct_backend_release_service);
}
