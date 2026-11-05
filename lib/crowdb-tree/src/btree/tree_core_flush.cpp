// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "backend/checkpoint_backend.h"
#include "btree/allocation_bound.h"
#include "crowdb-common/log.h"
#include "tree_core_internal.h"

namespace crowdb::tree
{
#ifdef CROWDB_TREE_TEST_UTIL
void TreeCore::set_flush_handoff_wait_for_tests(std::function<bool(uint64_t, std::function<void()>)> callback)
{
    state_->flush->set_handoff_wait_for_tests(std::move(callback));
}
#endif

void TreeCore::resume_capacity() noexcept
{
    if ((!local_backend() && !chunk_backend()) ||
        (durable_coverage() >= publisher().capture()->coverage &&
         (!chunk_backend() || !chunk_backend()->needs_recovery.load(std::memory_order_acquire)))) {
        state_->source_rotation->resume_capacity();
    }
}

Status TreeCore::prepare_flush(std::shared_ptr<FlushOperation> *out)
{
    return prepare_flush(out, {});
}

Status TreeCore::prepare_flush(std::shared_ptr<FlushOperation>               *out,
                               std::shared_ptr<TreeOperationLifetime::Ticket> accepted)
try {
    State::Ingress admission(*state_);
    if (admission.owner == nullptr) {
        return Status::unavailable("Tree ingress closed");
    }
    auto ticket = std::move(accepted);
    if (!ticket) {
        auto status = state_->lifetime->acquire(binding()->lifecycle_budget(), &ticket);
        if (!status.ok()) {
            return status;
        }
    }
    auto preparation  = state_->preparation;
    preparation.store = ticket->backend(async_backend());
    return state_->flush->prepare(preparation, out);
}
catch (const std::bad_alloc &) {
    return Status::resource_exhausted("Tree flush preparation allocation failed");
}

Status TreeCore::flush()
try {
    if (detail::FlushRuntime::on_worker_thread()) {
        return Status::unavailable("a runtime worker must use asynchronous flush");
    }
    State::Ingress admission(*state_);
    if (admission.owner == nullptr) {
        return Status::unavailable("Tree ingress closed");
    }
    std::shared_ptr<TreeOperationLifetime::Ticket> ticket;
    auto status = state_->lifetime->acquire(binding()->lifecycle_budget(), &ticket);
    if (!status.ok()) {
        return status;
    }
    auto preparation     = state_->preparation;
    preparation.store    = ticket->backend(async_backend());
    const auto requested = state_->frontier->contiguous().load(std::memory_order_acquire);
    for (;;) {
        const auto                      observed = state_->apply_progress->sequence();
        bool                            blocked  = false;
        std::shared_ptr<FlushOperation> operation;
        status = state_->flush->prepare(preparation, &operation, &blocked);
        if (status.ok()) {
            status = operation->wait();
            if (!status.ok() && operation->result().admission_stale) {
                continue;
            }
            if (status.ok() && operation->result().published_coverage < requested) {
                continue;
            }
            if (status.ok()) {
                resume_capacity();
            }
            return status;
        }
        if (!blocked) {
            return status;
        }
        status = state_->apply_progress->wait(observed);
        if (!status.ok()) {
            return status;
        }
    }
}
catch (const std::bad_alloc &) {
    return Status::resource_exhausted("Tree flush preparation allocation failed");
}
} // namespace crowdb::tree
