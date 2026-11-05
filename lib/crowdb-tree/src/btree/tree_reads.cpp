// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/tree_core.h"

namespace crowdb::tree
{
GetView Crowdbtree::get_view(Slice key) const
{
    GetView result;
    (void)get_view(key, &result);
    return result;
}

Status Crowdbtree::get_view(Slice key, GetView *out) const
{
    const auto status = core_->get(key, out);
    if (status.code() == Code::kIoError || status.code() == Code::kCorruption) {
        io_failed_.store(true, std::memory_order_release);
    }
    return status;
}

void Crowdbtree::get_async(Slice key, std::function<void(Status, GetView)> on_done) const
{
    core_->get_async(key, std::move(on_done));
}

bool Crowdbtree::get(Slice key, uint64_t *out_slot, std::string *out_value) const
{
    GetView v = get_view(key);
    if (!v.found()) {
        return false;
    }
    if (out_slot != nullptr) {
        *out_slot = v.slot();
    }
    if (out_value != nullptr) {
        *out_value = v.value().to_string(); // Clone into caller-owned storage.
    }
    return true;
}

std::vector<get_result> Crowdbtree::multi_get(const std::vector<Slice> &keys) const
{
    std::vector<get_result> results;
    results.reserve(keys.size());
    for (const Slice &k : keys) {
        get_result g;
        g.found = get(k, &g.slot, &g.value);
        results.push_back(std::move(g));
    }
    return results;
}
} // namespace crowdb::tree
