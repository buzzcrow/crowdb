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
typedef struct ct_owned_budget   ct_owned_budget;
typedef struct ct_future         ct_future;
typedef struct ct_scan_result    ct_scan_result;
typedef struct ct_snapshot_chunk ct_snapshot_chunk;

// Retains the paid control-allocation window of an immutable returned owner.
// Legacy owners return success with *out=nullptr (caller-owned allocation).
int32_t ct_future_owned_budget(const ct_future *future, ct_owned_budget **out);
int32_t ct_scan_result_owned_budget(const ct_scan_result *result, ct_owned_budget **out);
int32_t ct_snapshot_chunk_owned_budget(const ct_snapshot_chunk *chunk, ct_owned_budget **out);

// Claim before allocation, record after successful allocation, then record
// freed and release only after actual deallocation. Admission refusal acquires
// nothing. The owner must pair every successful claim with one release.
int32_t ct_owned_budget_claim(ct_owned_budget *budget, size_t bytes);
void    ct_owned_budget_record(ct_owned_budget *budget, size_t bytes, int32_t acquired);
void    ct_owned_budget_release(ct_owned_budget *budget, size_t bytes);
void    ct_owned_budget_free(ct_owned_budget *budget);
#ifdef __cplusplus
}
#endif
