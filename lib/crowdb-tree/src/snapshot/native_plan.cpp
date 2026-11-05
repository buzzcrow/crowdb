// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "snapshot/native_plan.h"

#include "btree/allocation_bound.h"

namespace crowdb::tree
{
struct NativePlanOperation::State : std::enable_shared_from_this<State>
{
    struct Receiver
    {
        using receiver_concept = stdexec::receiver_tag;
        std::shared_ptr<State> owner;

        void set_error(Status status) noexcept
        {
            auto self          = std::move(owner);
            self->child_status = std::move(status);
            self->ready.store(true, std::memory_order_release);
            self->wake();
        }

        void set_value() noexcept
        {
            set_error(Status::Ok());
        }

        void set_stopped() noexcept
        {
            set_error(Status::unavailable());
        }
    };

    using Connected = stdexec::connect_result_t<detail::CallbackSender, Receiver>;

    struct Attempt
    {
        Attempt(detail::CallbackSender sender, std::shared_ptr<State> owner)
            : connected(stdexec::connect(sender, Receiver{.owner = std::move(owner)}))
        {
        }

        Connected connected;
    };

    std::shared_ptr<CapturedFrameStream>            stream;
    std::shared_ptr<NativeLayoutPlanner>            planner;
    std::shared_ptr<detail::FlushRuntime::Progress> progress;
    std::optional<Attempt>                          attempt;
    std::atomic<uint8_t>                            lifecycle{0};
    std::atomic<bool>                               stopped{false};
    std::atomic<bool>                               ready{false};
    bool                                            waiting   = false;
    bool                                            last_page = false;
    Status                                          child_status;
    Status                                          outcome;
    detail::CallbackComplete                        complete = nullptr;
    void                                           *context  = nullptr;

    void wake() noexcept
    {
        if (lifecycle.load(std::memory_order_acquire) == 2) {
            progress->request(weak_from_this().lock());
        }
    }

    void finish(Status status) noexcept
    {
        lifecycle.store(3, std::memory_order_release);
        outcome = std::move(status);
        stream.reset();
        planner.reset();
        complete(context, outcome.ok() ? detail::CallbackSignal::kValue : detail::CallbackSignal::kError,
                 outcome.without_message());
    }

    void begin()
    {
        waiting = true;
        ready.store(false, std::memory_order_release);
        attempt.emplace(stream->sender(), shared_from_this());
        stdexec::start(attempt->connected);
    }

    void pump()
    {
        if (waiting) {
            if (stopped.load(std::memory_order_acquire)) {
                stream->request_stop();
            }
            if (!ready.load(std::memory_order_acquire)) {
                return;
            }
            waiting = false;
            attempt.reset();
            if (!child_status.ok()) {
                finish(std::move(child_status));
                return;
            }
            const auto &result = stream->result();
            last_page          = result.complete;
            if (result.plan) {
                auto status = planner->accept(*result.plan);
                if (!status.ok()) {
                    finish(std::move(status));
                    return;
                }
            }
            else if (!last_page) {
                finish(Status::corruption("native planning stream returned no page"));
                return;
            }
        }
        if (stopped.load(std::memory_order_acquire)) {
            finish(Status::unavailable());
            return;
        }
        if (planner->pending()) {
            auto status = planner->step();
            if (!status.ok()) {
                finish(std::move(status));
                return;
            }
            wake();
            return;
        }
        if (last_page) {
            finish(planner->finish());
            return;
        }
        begin();
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
};

Status NativePlanOperation::create(std::shared_ptr<CapturedFrameStream>                  stream,
                                   std::shared_ptr<NativeLayoutPlanner>                  planner,
                                   std::shared_ptr<detail::FlushRuntime>                 runtime,
                                   std::shared_ptr<detail::FlushRuntime::Lane>           lane,
                                   const std::shared_ptr<detail::AllocationReservation> &metadata,
                                   std::shared_ptr<NativePlanOperation>                 *out)
try {
    if (!stream || !planner || !runtime || !lane || !metadata || out == nullptr || *out) {
        return Status::invalid_argument("native planning requires admitted bounded inputs");
    }
    auto state     = detail::reserved_shared<State>(metadata);
    state->stream  = std::move(stream);
    state->planner = std::move(planner);
    auto status    = runtime->reserve_progress(lane, &State::run, state.get(), metadata, &state->progress);
    if (!status.ok()) {
        return status;
    }
    *out = detail::reserved_construct<NativePlanOperation>(
        metadata, [&](auto *storage) { ::new (static_cast<void *>(storage)) NativePlanOperation(std::move(state)); });
    return Status::Ok();
}
catch (const std::bad_alloc &) {
    return Status::resource_exhausted("native planning allocation failed");
}

size_t NativePlanOperation::allocation_bound() noexcept
{
    return detail::shared_allocation_bound<State>() + detail::shared_allocation_bound<NativePlanOperation>() +
           detail::FlushRuntime::progress_allocation_bound();
}

detail::CallbackSender NativePlanOperation::sender() noexcept
{
    return {this, &NativePlanOperation::submit};
}

const Status &NativePlanOperation::result() const noexcept
{
    return state_->outcome;
}

void NativePlanOperation::request_stop() noexcept
{
    state_->stopped.store(true, std::memory_order_release);
    state_->wake();
}

void NativePlanOperation::submit(void *source, detail::CallbackComplete complete, void *context)
{
    auto    self = static_cast<NativePlanOperation *>(source)->state_;
    uint8_t idle = 0;
    if (!self->lifecycle.compare_exchange_strong(idle, 1, std::memory_order_acq_rel)) {
        complete(context, detail::CallbackSignal::kError, Status::unavailable());
        return;
    }
    self->complete = complete;
    self->context  = context;
    self->lifecycle.store(2, std::memory_order_release);
    self->wake();
}
} // namespace crowdb::tree
