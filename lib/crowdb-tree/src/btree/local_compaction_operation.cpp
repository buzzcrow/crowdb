// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "backend/local/block_account.h"
#include "btree/allocation_bound.h"
#include "btree/local_compaction_internal.h"

#include <cmath>

namespace crowdb::tree
{
std::optional<size_t> LocalCompactionOperation::allocation_bound() noexcept
{
    using namespace detail;
    return AllocationBound(local_block_gc_metadata_bound({}).value_or(SIZE_MAX))
        .add(shared_allocation_bound<State>())
        .add(shared_allocation_bound<LocalCompactionOperation>())
        .add(shared_allocation_bound<AllocationReservation>())
        .add(TreeOperationLifetime::ticket_allocation_bound())
        .value();
}
Status LocalCompactionOperation::create(TreeCore &core, LocalCompactionOptions options,
                                        std::shared_ptr<ApplyDispatcher>               cpu,
                                        std::shared_ptr<detail::AllocationReservation> credit,
                                        std::shared_ptr<TreeOperationLifetime::Ticket> ticket,
                                        std::shared_ptr<LocalCompactionOperation>     *out)
try {
    if (out == nullptr || *out || !credit || !ticket || !cpu) {
        return Status::invalid_argument();
    }
    auto state              = detail::reserved_shared<State>(credit);
    state->credit           = credit;
    state->ticket           = std::move(ticket);
    state->core             = &core;
    state->options          = options;
    state->local            = core.local_backend();
    state->store            = dynamic_cast<BlockPageStore *>(core.config().page_store);
    state->cpu_progress     = std::move(cpu);
    state->backend_progress = core.binding()->backend_progress();
    Status status;
    if (state->store != nullptr && state->store->block_size() != 0 && options.max_relocation_bytes != 0) {
        if (!state->local || !state->local->ledger || !std::isfinite(options.free_threshold) ||
            options.free_threshold <= 0.0 || options.free_threshold >= 1.0) {
            return Status::invalid_argument();
        }
        status = detail::reserve_backend_operation(state->store, core.binding()->account(),
                                                   {.workers = core.binding()->backend_cpu()->worker_count()},
                                                   &state->backend);
        if (!status.ok()) {
            return status;
        }
        if (state->local->gc_active.exchange(true, std::memory_order_acq_rel)) {
            return Status::unavailable("another block cleanup is active");
        }
        state->gate_held = true;
    }
    else {
        state->store = nullptr;
    }
    *out =
        detail::reserved_construct<LocalCompactionOperation>(credit, [state = std::move(state)](void *where) mutable {
            return ::new (where) LocalCompactionOperation(std::move(state));
        });
    return Status::Ok();
}
catch (const std::bad_alloc &) {
    return Status::resource_exhausted();
}

LocalCompactionOperation::State::~State()
{
    if (gate_held) {
        local->gc_active.store(false, std::memory_order_release);
    }
    detail::seal_backend_budget(backend);
    if (credit) {
        credit->seal();
    }
}

detail::CallbackSender LocalCompactionOperation::sender() noexcept
{
    return {state_.get(), [](void *source, detail::CallbackComplete done, void *context) {
                static_cast<State *>(source)->start(done, context);
            }};
}

const MergeGcStats &LocalCompactionOperation::result() const noexcept
{
    return state_->report;
}

void LocalCompactionOperation::observe_completion(
    detail::InlineFunction<void(const MergeGcStats &), 32> observer) noexcept
{
    state_->observer = std::move(observer);
}

void LocalCompactionOperation::request_stop() noexcept
{
    state_->stopped.store(true, std::memory_order_release);
    if (state_->callback_ready.load(std::memory_order_acquire)) {
        state_->wake();
    }
}

void LocalCompactionOperation::State::start(detail::CallbackComplete callback, void *argument) noexcept
{
    if (started.exchange(true, std::memory_order_acq_rel)) {
        callback(argument, detail::CallbackSignal::kError, Status::unavailable());
        return;
    }
    complete = callback;
    context  = argument;
    callback_ready.store(true, std::memory_order_release);
    wake();
}

void LocalCompactionOperation::State::wake() noexcept
{
    cpu_progress->request(cpu_node, weak_from_this().lock());
}

void LocalCompactionOperation::State::cpu_run(void *context) noexcept
{
    auto &state = *static_cast<State *>(context);
    try {
        state.pump();
    }
    catch (const std::bad_alloc &) {
        state.finish(Status::resource_exhausted());
    }
    catch (...) {
        state.finish(Status::io_error());
    }
}

void LocalCompactionOperation::State::backend_run(void *context) noexcept
{
    auto &state = *static_cast<State *>(context);
    try {
        state.backend_step();
    }
    catch (const std::bad_alloc &) {
        state.child_status = Status::resource_exhausted();
    }
    catch (...) {
        state.child_status = Status::io_error();
    }
    state.child_ready.store(true, std::memory_order_release);
    state.wake();
}

void LocalCompactionOperation::State::finish(Status status) noexcept
{
    if (phase == Phase::kDone) {
        return;
    }
    phase = Phase::kDone;
    attempt.reset();
    child.reset();
    candidate.exclusion = {};
    if (gate_held) {
        local->gc_active.store(false, std::memory_order_release);
        gate_held = false;
    }
    detail::seal_backend_budget(backend);
    credit->seal();
    if (observer) {
        try {
            observer(report);
        }
        catch (...) {
            // Reporting cannot change the already completed storage outcome.
            observer.reset();
        }
        observer.reset();
    }
    const auto signal = status.ok() ? detail::CallbackSignal::kValue : detail::CallbackSignal::kError;
    complete(context, signal, std::move(status));
}
} // namespace crowdb::tree
