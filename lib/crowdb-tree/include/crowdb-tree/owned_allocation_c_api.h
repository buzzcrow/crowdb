// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once

#include "crowdb-tree/c_api.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct ct_owned_allocation ct_owned_allocation;

// Transfer a complete immutable allocation. Capacity includes spare bytes;
// length is the initialized prefix. The context is consumed on every return,
// including failure, and drop_context runs exactly once. drop_context must
// be nonnull. control_bytes is the
// foreign owner/control allocation size, independently charged as metadata.
// Requires a runtime Tree. Retained owners do not retain its CPU executor.
ct_status ct_owned_allocation_create(ct_tree *tree, const uint8_t *data, size_t length, size_t capacity,
                                     void (*drop_context)(void *), void *context, size_t control_bytes,
                                     ct_owned_allocation **out);
void      ct_owned_allocation_retain(ct_owned_allocation *owner);
void      ct_owned_allocation_free(ct_owned_allocation *owner);

typedef struct ct_owned_kv_ref
{
    const uint8_t       *key;
    size_t               key_len;
    ct_owned_allocation *owner;
    size_t               offset;
    size_t               length;
    uint8_t              kind; // 0 put, 1 delete
} ct_owned_kv_ref;

// Borrow owners for the synchronous call. Same-host values retain the shared
// allocation identity; different hosts copy into target-admitted owned cells.
// Validation/admission failure never publishes a partial batch.
ct_status ct_apply_batch_owned(ct_tree *tree, uint64_t slot, const ct_owned_kv_ref *operations, size_t count);

// The callback borrows stable immutable descriptors until return. It is called
// synchronously, may be called repeatedly for an index, and must not throw.
// This avoids an unaccounted temporary descriptor array in language adapters.
typedef ct_owned_kv_ref (*ct_owned_read_fn)(const void *context, size_t index);
ct_status ct_apply_batch_owned_reader(ct_tree *tree, uint64_t slot, size_t count, ct_owned_read_fn read,
                                      const void *context);

#ifdef __cplusplus
}
#endif
