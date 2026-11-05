// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once
#include "crowdb-tree/chunk_protection_c_api.h"
#include "crowdb-tree/runtime_c_api.h"
#ifdef __cplusplus
extern "C" {
#endif
typedef struct ct_chunk_pin_service ct_chunk_pin_service;

struct ct_chunk_pin_service_step
{
    size_t  completed;
    size_t  failed;
    uint8_t more;
    uint8_t drained;
};

// Context ownership transfers on success. Wake must not block or allocate.
ct_status ct_chunk_pin_service_create(ct_runtime *, size_t capacity, void (*wake)(void *), void (*drop_context)(void *),
                                      void *, ct_chunk_pin_service **);
ct_status ct_root_catalog_bind_pin_service(ct_root_catalog *, ct_chunk_pin_service *);
// Call pump on the host blocking-backend executor, never a Tree CPU worker.
ct_chunk_pin_service_step ct_chunk_pin_service_pump(ct_chunk_pin_service *, size_t limit);
// Non-draining host queue counters: used/allocated count all registered pins,
// waiters count pending final releases, and denied_host counts capacity denials.
ct_status ct_chunk_pin_service_resource_usage(const ct_chunk_pin_service *, ct_resource_usage_v1 *out);
void      ct_chunk_pin_service_stop(ct_chunk_pin_service *);
void      ct_chunk_pin_service_free(ct_chunk_pin_service *);
#ifdef __cplusplus
}
#endif
