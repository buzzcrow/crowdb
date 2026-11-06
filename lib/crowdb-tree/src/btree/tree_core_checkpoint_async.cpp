// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/allocation_bound.h"
#include "btree/tree_core_checkpoint_internal.h"
#include "btree/tree_core_internal.h"
#include "crowdb-common/log.h"

namespace crowdb::tree
{
namespace
{
struct CheckpointCall;

struct CheckpointReceiver
{
    using receiver_concept = stdexec::receiver_tag;
    CheckpointCall *owner;
    void            set_value() const noexcept;
    void            set_error(Status status) const noexcept;
    void            set_stopped() const noexcept;
};

struct CheckpointCall : std::enable_shared_from_this<CheckpointCall>
{
    using Connected = stdexec::connect_result_t<detail::CallbackSender, CheckpointReceiver>;

    explicit CheckpointCall(std::shared_ptr<CheckpointOperation> child)
        : operation(std::move(child)),
          connected(stdexec::connect(operation->sender(), CheckpointReceiver{this}))
    {
    }

    void start()
    {
        self = shared_from_this();
        stdexec::start(connected);
    }

    void finish(Status status) noexcept
    {
        auto retained = std::move(self);
        try {
            callback(std::move(status), operation->result().durable_coverage);
        }
        catch (...) {
            CRB_LOG_ERROR("Tree checkpoint receiver threw after terminal");
        }
    }

    std::shared_ptr<CheckpointOperation>  operation;
    std::function<void(Status, uint64_t)> callback;
    Connected                             connected;
    std::shared_ptr<CheckpointCall>       self;
};

void CheckpointReceiver::set_value() const noexcept
{
    owner->finish(Status::Ok());
}

void CheckpointReceiver::set_error(Status status) const noexcept
{
    owner->finish(std::move(status));
}

void CheckpointReceiver::set_stopped() const noexcept
{
    owner->finish(Status::unavailable());
}
} // namespace

void TreeCore::checkpoint_async(std::function<void(Status, uint64_t)> complete)
{
    if (!complete) {
        return;
    }
    Status                          status;
    uint64_t                        fallback_coverage = 0;
    std::shared_ptr<CheckpointCall> call;
    try {
        State::Ingress admission(*state_);
        if (admission.owner == nullptr) {
            status = Status::unavailable("Tree ingress closed");
        }
        else {
            fallback_coverage = durable_coverage();
            using namespace detail;
            constexpr auto bytes =
                shared_allocation_bound<CheckpointCall>() + shared_allocation_bound<AllocationReservation>();
            auto reserved = ResourceBudget::try_reserve(binding()->account(), BudgetClass::kReclaim, bytes);
            if (!reserved) {
                status = Status::resource_exhausted("checkpoint callback credit unavailable");
            }
            else {
                auto                                 credit = AllocationReservation::create(std::move(*reserved));
                std::shared_ptr<CheckpointOperation> operation;
                status = prepare_checkpoint(checkpoint_detail::defaults(*this), &operation);
                if (status.ok()) {
                    call = reserved_shared<CheckpointCall>(credit, std::move(operation));
                    call->callback.swap(complete);
                    credit->seal();
                }
            }
        }
    }
    catch (const std::bad_alloc &) {
        status = Status::resource_exhausted();
    }
    if (call) {
        call->start();
        return;
    }
    complete(std::move(status), fallback_coverage);
}
} // namespace crowdb::tree
