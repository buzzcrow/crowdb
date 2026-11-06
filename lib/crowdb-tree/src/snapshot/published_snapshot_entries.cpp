// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "snapshot/published_snapshot_entries.h"

#include "snapshot/operation_wait.h"

namespace crowdb::tree
{
struct PublishedSnapshotEntries::State
{
    std::shared_ptr<PublishedSnapshotContext> snapshot;
    std::shared_ptr<AllocationBudget>         metadata;
    std::shared_ptr<SnapshotPageLoader>       loader;
    std::shared_ptr<PublishedScan>            scan;
    std::atomic<bool>                         busy{false};
    Status                                    failure;
    bool                                      complete = false;
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
    auto metadata   = limits.metadata;
    auto state      = detail::reserved_shared<State>(metadata);
    state->snapshot = std::move(snapshot);
    state->metadata = metadata;
    state->loader   = detail::reserved_shared<SnapshotPageLoader>(metadata);
    limits.metadata = metadata;
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

        auto status = state_->snapshot->prepare_window(&window);
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
