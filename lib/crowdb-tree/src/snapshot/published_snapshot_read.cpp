// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/allocation_bound.h"
#include "btree/read_value_bound.h"
#include "snapshot/operation_wait.h"
#include "snapshot/published_snapshot.h"

namespace crowdb::tree
{
Status PublishedSnapshotContext::get(Slice key, GetView *out) const
try {
    if (out == nullptr) {
        return Status::invalid_argument("snapshot point read needs output");
    }
    *out = {};
    if (!range().contains(key)) {
        return Status::Ok();
    }
    auto       limits = read_limits();
    const auto root   = view()->mapping.get(view()->root_page_id);
    if (!root || root->level >= limits.max_route_depth) {
        return Status::corruption("snapshot has no bounded published root");
    }
    limits.max_route_depth           = root->level + 1ULL;
    const auto loads                 = load_limits();
    const auto workers               = binding()->cpu()->worker_count();
    const auto leaves                = view()->mapping.copy_bounds().max_leaf_bases;
    const auto values                = overflow_value_bytes_bound(*view(), loads.max_value_bytes);
    auto       bounded_loads         = loads;
    bounded_loads.max_overflow_pages = std::min<uint64_t>(loads.max_overflow_pages, values);
    const auto loader =
        values == 0 ? AsyncStorePageLoader::primary_reserve_bound(loads, root->level, workers, leaves)
                    : AsyncStorePageLoader::read_reserve_bound(bounded_loads, root->level, workers, leaves, 1, values);
    const auto bound =
        detail::AllocationBound().add(PublishedGet::allocation_bound(limits, workers)).add(loader).value();
    if (!bound) {
        return Status::resource_exhausted("snapshot point read bound overflow");
    }
    SnapshotReadWindow window;
    auto               status = prepare_window(*bound, &window);
    if (!status.ok()) {
        return status;
    }
    limits.metadata = window.budget.bytes;
    std::shared_ptr<PublishedGet> read;
    status = PublishedGet::create(view(), window.loader, key, limits, &read);
    if (status.ok()) {
        status = detail::wait_snapshot_operation(read);
    }
    if (status.ok() && read->result().found) {
        const auto &cell = read->result().cell;
        *out             = GetView::from_owner(cell.slot, cell.value, cell.owner);
    }
    window.seal();
    return status;
}
catch (const std::bad_alloc &) {
    return Status::resource_exhausted("snapshot point read allocation failed");
}

Status SnapshotPageLoader::begin(std::shared_ptr<PageLoader> loader)
{
    if (active_ || !loader) {
        return Status::unavailable("snapshot loader already has an active window");
    }
    active_ = std::move(loader);
    return Status::Ok();
}

void SnapshotPageLoader::end() noexcept
{
    active_.reset();
}

void SnapshotPageLoader::load_leaf(BaseRef base, LeafComplete complete)
{
    if (auto loader = active_) {
        loader->load_leaf(std::move(base), std::move(complete));
    }
    else {
        complete(Status::unavailable("snapshot loader has no active window"), {});
    }
}

void SnapshotPageLoader::load_inner(std::shared_ptr<const BackingRef> backing, InnerComplete complete)
{
    if (auto loader = active_) {
        loader->load_inner(std::move(backing), std::move(complete));
    }
    else {
        complete(Status::unavailable("snapshot loader has no active window"), {});
    }
}

void SnapshotPageLoader::load_overflow(std::shared_ptr<const BackingRef> primary, uint64_t head, uint64_t length,
                                       ValueComplete complete)
{
    if (auto loader = active_) {
        loader->load_overflow(std::move(primary), head, length, std::move(complete));
    }
    else {
        complete(Status::unavailable("snapshot loader has no active window"), {});
    }
}
} // namespace crowdb::tree
