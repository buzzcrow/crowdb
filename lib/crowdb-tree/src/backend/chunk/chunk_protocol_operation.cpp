// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "backend/chunk/chunk_protocol_operation.h"

#include "btree/allocation_bound.h"
#include "btree/apply_dispatcher.h"

#include <cassert>

namespace crowdb::tree
{
struct ChunkProtocolOperation::State : std::enable_shared_from_this<State>
{
    explicit State(PageLoadBudget credit) : budget(std::move(credit)), io(budget.io, 1)
    {
    }

    PageLoadBudget                                  budget;
    AllocationLease                                 io;
    std::shared_ptr<AsyncPageStore>                 store;
    std::shared_ptr<detail::FlushRuntime::Progress> progress;
    std::shared_ptr<ApplyDispatcher>                dispatcher;
    ApplyDispatcher::Node                           node{&State::run, this};
    std::shared_ptr<const void>                     owner;
    Step                                            step;
    Done                                            done;
    std::atomic<uint8_t>                            lifecycle{0};
    std::atomic<bool>                               stopped{false};
    std::atomic<bool>                               ready{false};
    bool                                            waiting = false;
    Status                                          event_status;
    detail::CallbackComplete                        complete = nullptr;
    void                                           *context  = nullptr;

    void wake() noexcept
    {
        if (lifecycle.load(std::memory_order_acquire) != 2) {
            return;
        }
        auto self = weak_from_this().lock();
        assert(self);
        if (dispatcher) {
            dispatcher->request(node, std::move(self));
        }
        else {
            progress->request(std::move(self));
        }
    }

    static void completed(void *context, Status status)
    {
        auto *self         = static_cast<State *>(context);
        self->event_status = std::move(status);
        self->ready.store(true, std::memory_order_release);
        self->wake();
    }

    static void run(void *context) noexcept
    {
        auto *self = static_cast<State *>(context);
        if (self->lifecycle.load(std::memory_order_acquire) != 2) {
            return;
        }
        try {
            self->pump();
        }
        catch (const std::bad_alloc &) {
            self->finish(Status::resource_exhausted());
        }
        catch (...) {
            self->finish(Status::internal_error());
        }
    }

    void pump()
    {
        if (waiting) {
            if (!ready.load(std::memory_order_acquire)) {
                return;
            }
            waiting = false;
            if (!event_status.ok()) {
                finish(std::move(event_status));
                return;
            }
            if (done()) {
                finish(Status::Ok());
                return;
            }
        }
        auto  self      = shared_from_this();
        auto *scheduled = static_cast<ScheduledPageStore *>(store.get());
        waiting         = true;
        ready.store(false, std::memory_order_release);
        auto status = scheduled->submit_step(
            [self] { return self->step(self->stopped.load(std::memory_order_acquire)); },
            {.context = this, .complete_fn = &State::completed, .owner = self, .allocations = budget.reservation()});
        if (!status.ok()) {
            waiting = false;
            finish(std::move(status));
        }
    }

    void finish(Status status) noexcept
    {
        if (lifecycle.exchange(3, std::memory_order_acq_rel) == 3) {
            return;
        }
        io   = {};
        step = {};
        done = {};
        owner.reset();
        store.reset();
        complete(context, status.ok() ? detail::CallbackSignal::kValue : detail::CallbackSignal::kError,
                 status.without_message());
    }
};

size_t ChunkProtocolOperation::allocation_bound() noexcept
{
    return detail::shared_allocation_bound<State>() + detail::shared_allocation_bound<ChunkProtocolOperation>() +
           detail::FlushRuntime::progress_allocation_bound();
}

Status ChunkProtocolOperation::create(std::shared_ptr<AsyncPageStore>             store,
                                      std::shared_ptr<detail::FlushRuntime>       runtime,
                                      std::shared_ptr<detail::FlushRuntime::Lane> lane, PageLoadBudget budget,
                                      std::shared_ptr<const void> owner, Step step, Done done,
                                      std::shared_ptr<ChunkProtocolOperation> *out)
try {
    if (!store || dynamic_cast<ScheduledPageStore *>(store.get()) == nullptr || !runtime || !lane ||
        !budget.reservation() || !budget.io || !step || !done || out == nullptr || *out ||
        budget.reservation()->account() != budget.io->account() || budget.io->category() != detail::BudgetClass::kIo) {
        return Status::invalid_argument();
    }
    auto state        = detail::reserved_shared<State>(budget.reservation(), budget);
    state->store      = std::move(store);
    state->owner      = std::move(owner);
    state->step       = std::move(step);
    state->done       = std::move(done);
    state->dispatcher = budget.cpu_progress;
    if (!state->dispatcher) {
        auto status = runtime->reserve_progress(lane, &State::run, state.get(), budget.reservation(), &state->progress);
        if (!status.ok()) {
            return status;
        }
    }
    *out = detail::reserved_construct<ChunkProtocolOperation>(budget.reservation(), [&](auto *storage) {
        ::new (static_cast<void *>(storage)) ChunkProtocolOperation(std::move(state));
    });
    return Status::Ok();
}
catch (const std::bad_alloc &) {
    return Status::resource_exhausted();
}

detail::CallbackSender ChunkProtocolOperation::sender() noexcept
{
    return {this, &ChunkProtocolOperation::submit};
}

void ChunkProtocolOperation::request_stop() noexcept
{
    state_->stopped.store(true, std::memory_order_release);
    state_->wake();
}

void ChunkProtocolOperation::submit(void *source, detail::CallbackComplete complete, void *context)
{
    auto    state = static_cast<ChunkProtocolOperation *>(source)->state_;
    uint8_t idle  = 0;
    if (!state->lifecycle.compare_exchange_strong(idle, 1, std::memory_order_acq_rel)) {
        complete(context, detail::CallbackSignal::kError, Status::unavailable());
        return;
    }
    state->complete = complete;
    state->context  = context;
    state->lifecycle.store(2, std::memory_order_release);
    state->wake();
}
} // namespace crowdb::tree
