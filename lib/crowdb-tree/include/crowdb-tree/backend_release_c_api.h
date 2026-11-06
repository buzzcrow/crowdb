// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once
#include "crowdb-tree/c_api.h"
#ifdef __cplusplus
extern "C" {
#endif
typedef struct ct_backend_release_service ct_backend_release_service;

typedef struct ct_backend_release_step
{
    size_t  visited;
    size_t  completed;
    uint8_t more;
    uint8_t drained;
} ct_backend_release_step;

// Bind once before publishing the opened Tree to other callers. Returns
// NotFound for backends without deferred file release. Context transfers only
// on success. Wake must neither block nor allocate. This service owns neither
// the Tree, its runtime, nor the backend; the host retains it through drained.
ct_status ct_tree_backend_release_service(ct_tree *tree, void (*wake)(void *), void (*drop_context)(void *),
                                          void *context, ct_backend_release_service **out);
// Pump only on a host blocking-backend executor. Limit is in [1,64].
ct_backend_release_step ct_backend_release_service_pump(ct_backend_release_service *service, size_t limit);
// Stop new backend files only after host ingress is closed. Existing readers
// remain valid and may enqueue their final release after this call.
void ct_backend_release_service_stop(ct_backend_release_service *service);
void ct_backend_release_service_free(ct_backend_release_service *service);
#ifdef __cplusplus
}
#endif
