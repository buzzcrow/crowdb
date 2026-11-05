// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/allocation_bound.h"
#include "crowdb-common/log.h"
#include "tree_core_internal.h"

namespace crowdb::tree
{
namespace
{
struct FlushCall;

struct FlushReceiver
{
    using receiver_concept = stdexec::receiver_tag;
    FlushCall *owner;
    void       set_value(FlushResult result) const noexcept;
    void       set_error(Status status) const noexcept;
    void       set_stopped() const noexcept;
};

struct FlushCall : std::enable_shared_from_this<FlushCall>
{
    using Connected = stdexec::connect_result_t<FlushReportSender, FlushReceiver>;

    FlushCall(std::shared_ptr<TreeOperationLifetime::Ticket> lifetime, std::shared_ptr<FlushOperation> operation)
        : ticket(std::move(lifetime)),
          operation(std::move(operation)),
          connected(stdexec::connect(this->operation->report_sender(), FlushReceiver{this}))
    {
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

    void start()
    {
        self = shared_from_this();
        stdexec::start(connected);
    }

    std::shared_ptr<TreeOperationLifetime::Ticket> ticket;
    std::shared_ptr<FlushOperation>                operation;
    std::function<void(Status)>                    callback;
    Connected                                      connected;
    std::shared_ptr<FlushCall>                     self;
};

void FlushReceiver::set_value(FlushResult result) const noexcept
{
    owner->finish(result.handoff_status.ok() ? result.maintenance_status : result.handoff_status);
}

void FlushReceiver::set_error(Status status) const noexcept
{
    owner->finish(std::move(status));
}

void FlushReceiver::set_stopped() const noexcept
{
    owner->finish(Status::unavailable("Tree flush stopped"));
}
} // namespace

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
            std::shared_ptr<FlushOperation> operation;
            if (status.ok()) {
                status = prepare_flush(&operation);
            }
            if (status.ok()) {
                call = detail::reserved_shared<FlushCall>(credit, std::move(ticket), std::move(operation));
                call->callback.swap(complete);
                credit->seal();
            }
        }
    }
    catch (const std::bad_alloc &) {
        status = Status::resource_exhausted("Tree flush callback allocation failed");
    }
    if (call) {
        call->start();
        return;
    }
    complete(std::move(status));
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
