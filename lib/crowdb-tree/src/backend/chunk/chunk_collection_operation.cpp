// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "chunk_collection_operation.h"

#include "btree/allocation_bound.h"
#include "chunk_backend_state.h"

namespace crowdb::tree
{
struct ChunkCollectionOperation::State
{
    std::shared_ptr<ChunkBackendState> backend;
    PageLoadBudget                     budget;
    detail::ChunkCollectionProgress    progress;
    size_t                             steps     = 0;
    bool                               owns_gate = false;

    ~State()
    {
        release_gate();
        if (budget.reservation()) {
            budget.reservation()->seal();
        }
        if (budget.io) {
            budget.io->seal();
        }
    }

    void release_gate() noexcept
    {
        if (owns_gate) {
            owns_gate = false;
            backend->collection_active.store(false, std::memory_order_release);
            backend.reset();
        }
    }

    Status step(bool stopped)
    {
        if (stopped) {
            release_gate();
            return Status::unavailable();
        }
        const auto &domain = *backend->domain;
        auto        status = backend->collector.step(*domain.catalog, *domain.transport, domain.tree, domain.epoch,
                                                     budget.reservation(), &progress, domain.physical_registry);
        ++steps;
        if (!status.ok() || done()) {
            release_gate();
        }
        return status;
    }

    [[nodiscard]] bool done() const noexcept
    {
        return steps == 8 || !progress.more;
    }
};

size_t ChunkCollectionOperation::allocation_bound() noexcept
{
    return detail::shared_allocation_bound<State>() + detail::shared_allocation_bound<ChunkCollectionOperation>() +
           ChunkProtocolOperation::allocation_bound() + detail::ChunkCollector::allocation_bound();
}

Status ChunkCollectionOperation::create(std::shared_ptr<ChunkBackendState>          backend,
                                        std::shared_ptr<AsyncPageStore>             store,
                                        std::shared_ptr<detail::FlushRuntime>       runtime,
                                        std::shared_ptr<detail::FlushRuntime::Lane> lane, PageLoadBudget budget,
                                        std::shared_ptr<ChunkCollectionOperation> *out)
try {
    if (!backend || !store || !budget.reservation() || !budget.io || out == nullptr || *out) {
        return Status::invalid_argument();
    }
    auto state     = detail::reserved_shared<State>(budget.reservation());
    state->backend = std::move(backend);
    state->budget  = budget;
    if (state->backend->needs_recovery.load(std::memory_order_acquire) ||
        state->backend->collection_active.exchange(true, std::memory_order_acq_rel)) {
        return Status::unavailable();
    }
    state->owns_gate = true;
    auto operation   = detail::reserved_construct<ChunkCollectionOperation>(budget.reservation(), [&](auto *storage) {
        ::new (static_cast<void *>(storage)) ChunkCollectionOperation(state);
    });
    auto status      = ChunkProtocolOperation::create(
        std::move(store), std::move(runtime), std::move(lane), budget, state,
        [state](bool stopped) { return state->step(stopped); }, [state] { return state->done(); },
        &operation->protocol_);
    if (status.ok()) {
        *out = std::move(operation);
    }
    return status;
}
catch (const std::bad_alloc &) {
    return Status::resource_exhausted();
}

detail::CallbackSender ChunkCollectionOperation::sender() noexcept
{
    return protocol_->sender();
}

const detail::ChunkCollectionProgress &ChunkCollectionOperation::result() const noexcept
{
    return state_->progress;
}

void ChunkCollectionOperation::request_stop() noexcept
{
    protocol_->request_stop();
}
} // namespace crowdb::tree
