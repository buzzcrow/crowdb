// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/tree_core.h"

namespace crowdb::tree
{
Status Crowdbtree::scan(Slice prefix, Slice start_after, Slice end_key, size_t limit, size_t byte_budget,
                        bool keys_only, uint64_t deadline_ms, std::vector<scan_entry> *out, bool *truncated,
                        bool include_tombstones, ScanPackedBuf *out_packed, size_t *out_count, bool has_start_bound,
                        bool start_inclusive, bool strict_byte_budget) const
{
    CoreScanOptions options;
    options.prefix             = prefix;
    options.start              = start_after;
    options.end                = end_key;
    options.limit              = limit;
    options.byte_budget        = byte_budget;
    options.deadline_ms        = deadline_ms;
    options.keys_only          = keys_only;
    options.include_tombstones = include_tombstones;
    options.has_start_bound    = has_start_bound || !start_after.empty();
    options.start_inclusive    = start_inclusive;
    options.strict_byte_budget = strict_byte_budget;
    return core_->scan(options, out, truncated, out_packed, out_count);
}

Status Crowdbtree::seek_reverse(Slice start_key, bool inclusive, Slice begin_key, scan_entry *out, bool *found) const
{
    if (out == nullptr || found == nullptr) {
        return Status::invalid_argument("seek_reverse output is null");
    }
    CoreScanOptions options;
    options.start           = start_key;
    options.begin           = begin_key;
    options.reverse         = true;
    options.has_start_bound = true;
    options.start_inclusive = inclusive;
    options.limit           = 1;
    std::vector<scan_entry> entries;
    bool                    truncated = false;
    const auto              status    = core_->scan(options, &entries, &truncated);
    *found                            = status.ok() && !entries.empty();
    if (*found) {
        *out = std::move(entries.front());
    }
    return status;
}

Status Crowdbtree::scan_reverse(Slice start_key, bool has_start_bound, bool start_inclusive, Slice begin_key,
                                size_t limit, size_t byte_budget, std::vector<scan_entry> *out, bool *truncated) const
{
    CoreScanOptions options;
    options.start           = start_key;
    options.begin           = begin_key;
    options.reverse         = true;
    options.has_start_bound = has_start_bound;
    options.start_inclusive = start_inclusive;
    options.limit           = limit;
    options.byte_budget     = byte_budget;
    return core_->scan(options, out, truncated);
}

void Crowdbtree::scan_async(Slice prefix, Slice start_after, Slice end_key, size_t limit, size_t byte_budget,
                            bool keys_only, uint64_t deadline_ms,
                            std::function<void(Status, ScanPackedBuf, bool)> on_done) const
{
    CoreScanOptions options;
    options.prefix          = prefix;
    options.start           = start_after;
    options.end             = end_key;
    options.limit           = limit;
    options.byte_budget     = byte_budget;
    options.deadline_ms     = deadline_ms;
    options.keys_only       = keys_only;
    options.has_start_bound = !start_after.empty();
    core_->scan_async(options, std::move(on_done));
}

void Crowdbtree::scan_directional_async(Slice prefix, Slice start_after, Slice end_key, size_t limit,
                                        size_t byte_budget, bool keys_only, uint64_t deadline_ms, bool reverse,
                                        std::function<void(Status, ScanPackedBuf, bool)> on_done) const
{
    CoreScanOptions options;
    options.prefix          = prefix;
    options.start           = start_after;
    options.end             = end_key;
    options.limit           = limit;
    options.byte_budget     = byte_budget;
    options.deadline_ms     = deadline_ms;
    options.keys_only       = keys_only;
    options.reverse         = reverse;
    options.has_start_bound = !start_after.empty();
    core_->scan_async(options, std::move(on_done));
}
} // namespace crowdb::tree
