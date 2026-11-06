// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/allocation_bound.h"
#include "crowdb-common/log.h"
#include "tree_core_internal.h"

namespace crowdb::tree
{
namespace
{
struct FlushCall : std::enable_shared_from_this<FlushCall>
{
    struct Receiver
    {
        using receiver_concept = stdexec::receiver_tag;
        FlushCall *owner;

        void set_value() const noexcept
        {
            owner->resumed = Status::Ok();
            owner->schedule();
        }

        void set_value(FlushResult result) const noexcept
        {
            owner->report = std::move(result);
            owner->resumed =
                owner->report.handoff_status.ok() ? owner->report.maintenance_status : owner->report.handoff_status;
            owner->schedule();
        }

        void set_error(Status status) const noexcept
        {
            owner->rejected = true;
            owner->resumed  = std::move(status);
            owner->schedule();
        }

        void set_stopped() const noexcept
        {
            set_error(Status::unavailable("Tree flush stopped"));
        }
    };

    struct Attempt
    {
        Attempt(FlushCall &owner, std::shared_ptr<FlushOperation> value)
            : operation(std::move(value)),
              connected(stdexec::connect(operation->report_sender(), Receiver{&owner}))
        {
        }

        std::shared_ptr<FlushOperation>                        operation;
        stdexec::connect_result_t<FlushReportSender, Receiver> connected;
    };

    struct Wait
    {
        Wait(FlushCall &owner, uint64_t sequence)
            : request(*owner.progress, sequence),
              connected(stdexec::connect(request.sender(), Receiver{&owner}))
        {
        }

        ApplyProgress::Request                                      request;
        stdexec::connect_result_t<detail::CallbackSender, Receiver> connected;
    };

    FlushCall(std::shared_ptr<TreeCore> value, std::shared_ptr<TreeOperationLifetime::Ticket> lifetime,
              FlushController &controller, FlushPreparationOptions options, std::shared_ptr<ApplyDispatcher> dispatcher,
              std::shared_ptr<ApplyProgress> progress)
        : core(std::move(value)),
          ticket(std::move(lifetime)),
          controller(controller),
          preparation(std::move(options)),
          dispatcher(std::move(dispatcher)),
          progress(std::move(progress)),
          requested(core->contiguous_slot())
    {
    }

    void schedule() noexcept
    {
        dispatcher->request(node, self);
    }

    static void run(void *context) noexcept
    {
        auto &owner = *static_cast<FlushCall *>(context);
        try {
            owner.step();
        }
        catch (const std::bad_alloc &) {
            owner.finish(Status::resource_exhausted("Tree flush continuation allocation failed"));
        }
        catch (...) {
            owner.finish(Status::internal_error("Tree flush continuation failed"));
        }
    }

    void step()
    {
        if (attempt) {
            attempt.reset();
            if (!resumed.ok() && !report.admission_stale) {
                if (rejected && resumed.code() == Code::kUnavailable) {
                    wait.emplace(*this, observed);
                    stdexec::start(wait->connected);
                }
                else {
                    finish(std::move(resumed));
                }
                return;
            }
            if (resumed.ok() && report.published_coverage >= requested) {
                core->resume_capacity();
                finish(Status::Ok());
                return;
            }
        }
        if (wait) {
            wait.reset();
            if (!resumed.ok()) {
                finish(std::move(resumed));
                return;
            }
        }
        observed                                = progress->sequence();
        bool                            blocked = false;
        std::shared_ptr<FlushOperation> operation;
        auto                            status = controller.prepare(preparation, &operation, &blocked);
        if (!status.ok()) {
            if (blocked) {
                wait.emplace(*this, observed);
                stdexec::start(wait->connected);
            }
            else {
                finish(std::move(status));
            }
            return;
        }
        observed = progress->sequence();
        report   = {};
        rejected = false;
        attempt.emplace(*this, std::move(operation));
        stdexec::start(attempt->connected);
    }

    void finish(Status status) noexcept
    {
        auto retained = std::move(self);
        try {
            callback(std::move(status));
        }
        catch (...) {
            CRB_LOG_ERROR("Tree flush receiver threw after terminal");
        }
    }

    std::shared_ptr<TreeCore>                      core;
    std::shared_ptr<TreeOperationLifetime::Ticket> ticket;
    FlushController                               &controller;
    FlushPreparationOptions                        preparation;
    std::shared_ptr<ApplyDispatcher>               dispatcher;
    std::shared_ptr<ApplyProgress>                 progress;
    const uint64_t                                 requested;
    uint64_t                                       observed = 0;
    ApplyDispatcher::Node                          node{&FlushCall::run, this};
    FlushResult                                    report;
    Status                                         resumed;
    bool                                           rejected = false;
    std::optional<Attempt>                         attempt;
    std::optional<Wait>                            wait;
    std::function<void(Status)>                    callback;
    std::shared_ptr<FlushCall>                     self;
};
} // namespace

void TreeCore::flush_async(std::function<void(Status)> complete)
{
    if (!complete) {
        return;
    }
    std::shared_ptr<FlushCall> call;
    Status                     status;
    try {
        State::Ingress admission(*state_);
        constexpr auto bytes = detail::shared_allocation_bound<FlushCall>() +
                               detail::shared_allocation_bound<detail::AllocationReservation>();
        auto reserved = detail::ResourceBudget::try_reserve(binding()->account(), detail::BudgetClass::kHandoff,
                                                            bytes + TreeOperationLifetime::ticket_allocation_bound());
        if (admission.owner == nullptr) {
            status = Status::unavailable("Tree ingress closed");
        }
        else if (!reserved) {
            status = Status::unavailable("Tree flush callback credit unavailable");
        }
        else {
            auto credit = detail::AllocationReservation::create(std::move(*reserved));
            std::shared_ptr<TreeOperationLifetime::Ticket> ticket;
            status = state_->lifetime->acquire(credit, &ticket);
            if (status.ok()) {
                auto preparation  = state_->preparation;
                preparation.store = ticket->backend(async_backend());
                call = detail::reserved_shared<FlushCall>(credit, shared_from_this(), std::move(ticket), *state_->flush,
                                                          std::move(preparation), state_->apply_dispatcher,
                                                          state_->apply_progress);
                call->callback.swap(complete);
                credit->seal();
            }
        }
    }
    catch (const std::bad_alloc &) {
        status = Status::resource_exhausted("Tree flush callback allocation failed");
    }
    if (call) {
        call->self = call;
        FlushCall::run(call.get());
        return;
    }
    complete(std::move(status));
}
} // namespace crowdb::tree
