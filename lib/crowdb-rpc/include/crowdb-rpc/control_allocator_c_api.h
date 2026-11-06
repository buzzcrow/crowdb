// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once
#ifdef __cplusplus
#    include <cstddef>
#    include <cstdint>
#else
#    include <stddef.h>
#    include <stdint.h>
#endif

#ifdef __cplusplus
extern "C" {
#endif

typedef struct crowdb_rpc_buffer_s *crowdb_rpc_buffer_t;

// Callbacks never throw and are safe on any final-owner thread. The external
// owner keeps context alive through every control deallocation; free_cb runs
// only after all native Buffer, refcount, and C handle storage has been freed.
// claim precedes allocation; record(acquired=0) and release follow actual free.
typedef struct crowdb_rpc_control_allocator_v1
{
    uint32_t version;
    void    *context;
    int32_t (*claim)(void *, size_t, size_t);
    void (*record)(void *, size_t, size_t, int32_t);
    void (*release)(void *, size_t, size_t);
} crowdb_rpc_control_allocator_v1;

// On failure no ownership transfers and free_cb is not called. All partial
// native allocations and their claims have been released before returning.
crowdb_rpc_buffer_t crowdb_rpc_buffer_create_external_accounted(const uint8_t *data, uint32_t  len,
                                                                void (*free_cb)(void *), void *free_ctx,
                                                                const crowdb_rpc_control_allocator_v1 *callbacks);

#ifdef __cplusplus
}
#endif
