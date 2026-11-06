// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/tree_core.h"
#include "snapshot/published_native_collect.h"
#include "snapshot/published_snapshot_view.h"

namespace crowdb::tree
{
Status Crowdbtree::install_snapshot(std::vector<leaf_entry> sorted_entries, uint64_t at_slot)
{
    return core_->install_portable(std::move(sorted_entries), at_slot);
}

Status Crowdbtree::collect_native_frames(std::vector<NativeFrame> *out, uint64_t *out_root_page_id,
                                         uint64_t *out_at_slot, uint64_t *out_next_page_id, const KeyRange *filter,
                                         uint64_t *out_subtrees_skipped)
{
    if (filter != nullptr) {
        auto status = filter->validate();
        if (!status.ok()) {
            return status;
        }
    }
    NativeStreamInfo info;
    auto status = collect_published_frames(core_, core_->binding()->options().limits.routing_bytes, out, &info);
    if (!status.ok()) {
        return status;
    }
    if (out_root_page_id != nullptr) {
        *out_root_page_id = info.root_page_id;
    }
    if (out_at_slot != nullptr) {
        *out_at_slot = info.at_slot;
    }
    if (out_next_page_id != nullptr) {
        *out_next_page_id = info.next_page_id;
    }
    if (out_subtrees_skipped != nullptr) {
        *out_subtrees_skipped = 0;
    }
    return Status::Ok();
}

std::shared_ptr<Snapshot> Crowdbtree::snapshot_view()
{
    std::shared_ptr<Snapshot> result;
    auto                      status = create_published_snapshot(core_, &result);
    if (!status.ok()) {
        throw SnapshotReadError(status);
    }
    return result;
}
} // namespace crowdb::tree
