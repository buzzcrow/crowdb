// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once

#include "crowdb-tree/btree/read_result.h"
#include "crowdb-tree/btree/scan_packed.h"
#include "crowdb-tree/c_api.h"
#include "crowdb-tree/chunk_runtime_c_api.h"
#include "crowdb-tree/maintenance_c_api.h"
#include "crowdb-tree/status.h"

#include <atomic>
#include <memory>
#include <variant>

struct ct_future_impl
{
    enum class Kind : std::uint8_t {
        kGet,
        kFlush,
        kSnapshot,
        kScan,
        kApply,
        kFlushReport,
        kCheckpointReport,
        kCompaction,
        kChunkCollection
    };
    Kind                                            kind = Kind::kGet;
    std::shared_ptr<crowdb::tree::AllocationBudget> metadata;
    std::shared_ptr<crowdb::tree::AllocationBudget> result_controls;
    // Written once by the completion callback (release), read once by the
    // first ct_future_poll that observes it (acquire) -- that acquire/
    // release pair is the only synchronization the fields below need,
    // since there is exactly one writer-then-reader handoff per future.
    std::atomic<bool> done{false};
    ct_status         status = static_cast<ct_status>(crowdb::tree::Code::kOk);
    // kGet only. Retains the immutable allocation backing the borrowed bytes.
    // ct_future_poll deliberately does *not* delete the handle for a kGet
    // future (see its updated doc comment in c_api.h); the caller must
    // always follow up with ct_future_free once done reading out_value.
    crowdb::tree::GetView get_result;
    uint64_t              slot = 0; // kSnapshot only (last_applied_slot)
    // kScan only: packed record buffer (same format ct_scan produces),
    // received directly from the engine via ScanPackedBuf (R57: zero-copy
    // staging — no re-pack loop, no make_buf). Ownership is transferred
    // to the caller via release() in ct_future_poll.
    crowdb::tree::ScanPackedBuf scan_packed;
    uint64_t                    scan_count     = 0;
    bool                        scan_truncated = false;
    std::variant<std::monostate, ct_flush_result_v1, ct_checkpoint_result_v1, ct_chunk_collection_result_v1,
                 ct_compaction_result_v1>
        maintenance;
};

using ct_future_handle = std::shared_ptr<ct_future_impl>;

inline std::shared_ptr<ct_future_impl> make_ct_future(std::shared_ptr<crowdb::tree::AllocationBudget> metadata)
{
    auto value      = std::allocate_shared<ct_future_impl>(crowdb::tree::MetadataAllocator<ct_future_impl>(metadata));
    value->metadata = std::move(metadata);
    return value;
}

inline ct_future_handle *make_ct_future_handle(std::shared_ptr<ct_future_impl> value)
{
    crowdb::tree::MetadataAllocator<ct_future_handle> allocator(value->metadata);
    auto                                             *handle = allocator.allocate(1);
    std::construct_at(handle, std::move(value));
    return handle;
}

inline void free_ct_future_handle(ct_future_handle *handle) noexcept
{
    crowdb::tree::MetadataAllocator<ct_future_handle> allocator((*handle)->metadata);
    std::destroy_at(handle);
    allocator.deallocate(handle, 1);
}
