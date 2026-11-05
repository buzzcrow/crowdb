// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "snapshot/published_snapshot_entries.h"

#include "btree/allocation_bound.h"
#include "btree/read_value_bound.h"
#include "snapshot/operation_wait.h"
#include "snapshot/published_snapshot_budget.h"

namespace crowdb::tree
{
struct PublishedSnapshotEntries::State
{
    ~State()
    {
        if (metadata) {
            metadata->seal();
        }
    }

    std::shared_ptr<PublishedSnapshotContext>      snapshot;
    std::shared_ptr<detail::AllocationReservation> metadata;
    std::shared_ptr<SnapshotPageLoader>            loader;
    std::shared_ptr<PublishedScan>                 scan;
    size_t                                         read_bound = 0;
    std::atomic<bool>                              busy{false};
    Status                                         failure;
    bool                                           complete = false;
};

Status PublishedSnapshotEntries::create(std::shared_ptr<PublishedSnapshotContext>  snapshot,
                                        std::shared_ptr<PublishedSnapshotEntries> *out, const KeyRange &range)
try {
    if (!snapshot || out == nullptr || *out) {
        return Status::invalid_argument("snapshot entry cursor requires fixed inputs and empty output");
    }
    auto status = range.validate();
    if (!status.ok()) {
        return status;
    }
    auto limits = snapshot->read_limits();
    if ((range.start() && range.start()->size() > limits.max_key_bytes) ||
        (range.end() && range.end()->size() > limits.max_key_bytes)) {
        return Status::invalid_argument("snapshot range exceeds key budget");
    }
    limits.max_entries = 1;
    // Snapshot capture removes L0. Its fixed mapping cannot observe future
    // source writes, so neither source cursors nor their key copies are needed.
    limits.max_memtables   = 1;
    const auto root        = snapshot->view()->mapping.get(snapshot->view()->root_page_id);
    limits.max_route_depth = root ? root->level + 1ULL : 1;
    if (limits.max_route_depth > 64) {
        return Status::resource_exhausted("snapshot entry cursor exceeds route depth bound");
    }
    const auto  workers = snapshot->binding()->cpu()->worker_count();
    const auto  loads   = snapshot->load_limits();
    const auto &view    = *snapshot->view();
    const auto  leaves  = snapshot_leaf_window(view, limits.max_entries);
    if (!leaves) {
        return Status::resource_exhausted("snapshot entry frame window overflow");
    }
    const auto value_bytes   = overflow_value_bytes_bound(view, loads.max_value_bytes);
    auto       bounded_loads = loads;
    // Each accepted overflow frame supplies at least one value byte. Keep the
    // loader's validation limits unchanged; only this fixed window is smaller.
    bounded_loads.max_overflow_pages = std::min<uint64_t>(loads.max_overflow_pages, value_bytes);
    const auto read_bound =
        value_bytes == 0 ? AsyncStorePageLoader::primary_reserve_bound(loads, limits.max_route_depth, workers, *leaves)
                         : AsyncStorePageLoader::read_reserve_bound(bounded_loads, limits.max_route_depth, workers,
                                                                    *leaves, 1, value_bytes);
    using detail::shared_allocation_bound;
    const auto bound =
        detail::AllocationBound(shared_allocation_bound<State>() + shared_allocation_bound<PublishedSnapshotEntries>() +
                                shared_allocation_bound<SnapshotPageLoader>() +
                                shared_allocation_bound<detail::AllocationReservation>())
            .add(PublishedScan::allocation_bound(limits, workers))
            .add_product(2, OwnedKey::allocation_overhead() + limits.max_key_bytes + 1)
            .value();
    if (!bound || !read_bound) {
        return Status::resource_exhausted("snapshot entry cursor bound overflow");
    }
    auto credit =
        detail::ResourceBudget::try_reserve(snapshot->binding()->account(), detail::BudgetClass::kRouting, *bound);
    if (!credit) {
        return Status::resource_exhausted("snapshot entry control reserve unavailable");
    }
    auto metadata     = detail::AllocationReservation::create(std::move(*credit));
    auto state        = detail::reserved_shared<State>(metadata);
    state->snapshot   = std::move(snapshot);
    state->metadata   = metadata;
    state->loader     = detail::reserved_shared<SnapshotPageLoader>(metadata);
    state->read_bound = *read_bound;
    limits.metadata   = metadata;
    PageKeyRange selection;
    if (range.start()) {
        selection.lower = OwnedKey(Slice(*range.start()), metadata);
    }
    if (range.end()) {
        selection.upper = OwnedKey(Slice(*range.end()), metadata);
    }
    const auto &domain = state->snapshot->range();
    if (domain.lower && (!selection.lower || *domain.lower > *selection.lower)) {
        selection.lower = domain.lower;
    }
    if (domain.upper && (!selection.upper || *domain.upper < *selection.upper)) {
        selection.upper = domain.upper;
    }
    status =
        PublishedScan::create(state->snapshot->view(), state->loader, std::move(selection), false, limits, &state->scan,
                              {.start = {}, .include_tombstones = true, .allow_oversized_first = true});
    if (!status.ok()) {
        return status;
    }
    *out = detail::reserved_construct<PublishedSnapshotEntries>(metadata, [&](auto *storage) {
        ::new (static_cast<void *>(storage)) PublishedSnapshotEntries(std::move(state));
    });
    return Status::Ok();
}
catch (const std::bad_alloc &) {
    return Status::resource_exhausted("snapshot entry cursor allocation failed");
}

Status PublishedSnapshotEntries::next(std::optional<PublishedScanEntry> *out, bool *complete)
{
    if (out == nullptr || complete == nullptr) {
        return Status::invalid_argument("snapshot entry cursor needs result outputs");
    }
    out->reset();
    *complete = false;
    if (state_->busy.exchange(true, std::memory_order_acq_rel)) {
        return Status::unavailable("snapshot entry cursor already has an active window");
    }

    struct Leave
    {
        State &state;

        ~Leave()
        {
            state.busy.store(false, std::memory_order_release);
        }
    } leave{*state_};

    if (!state_->failure.ok()) {
        return state_->failure;
    }
    if (state_->complete) {
        *complete = true;
        return Status::Ok();
    }
    try {
        SnapshotReadWindow window;

        struct ActiveWindow
        {
            SnapshotPageLoader &loader;

            ~ActiveWindow()
            {
                loader.end();
            }
        } active{*state_->loader};

        auto status = state_->snapshot->prepare_window(state_->read_bound, &window);
        if (status.ok()) {
            status = state_->loader->begin(window.loader);
        }
        if (!status.ok()) {
            state_->failure = status.without_message();
            return status;
        }
        detail::SnapshotWaitCompletion completion;
        auto                           connected =
            stdexec::connect(state_->scan->next_sender(), detail::SnapshotWaitReceiver{.completion = &completion});
        stdexec::start(connected);
        completion.done.wait();
        const auto &result = state_->scan->result();
        if (!result.status.ok()) {
            state_->failure = result.status.without_message();
            return result.status;
        }
        if (!result.entries.empty()) {
            *out = result.entries.front();
        }
        state_->complete = result.done;
        *complete        = result.done;
        state_->loader->end();
        window.seal();
        return Status::Ok();
    }
    catch (const std::bad_alloc &) {
        state_->failure = Status::resource_exhausted();
        return state_->failure;
    }
}

uint64_t PublishedSnapshotEntries::skipped_subtrees() const noexcept
{
    return state_->scan->skipped_subtrees();
}
} // namespace crowdb::tree
