// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once

#include "c_api_future_internal.h"
#include "c_api_internal.h"
#include "runtime/tree_operation_lifetime.h"
#include "snapshot/checkpoint_operation.h"
#include "stdexec_adapter.h"

#include <type_traits>

namespace crowdb::tree::detail
{
struct ChunkCollectionProgress;
}

namespace crowdb::tree::c_api_detail
{
void store_report(ct_future_impl &result, const detail::ChunkCollectionProgress &report,
                  const Status &terminal) noexcept;
void store_report(ct_future_impl &result, const MergeGcStats &report, const Status &terminal) noexcept;
void store_report(ct_future_impl &result, const FlushResult &report, const Status &terminal) noexcept;
void store_report(ct_future_impl &result, const CheckpointResult &report, const Status &terminal) noexcept;

template <class Operation> struct MaintenanceCall : std::enable_shared_from_this<MaintenanceCall<Operation>>
{
    struct Receiver
    {
        using receiver_concept = stdexec::receiver_tag;
        MaintenanceCall *owner;

        void set_value() const noexcept
        {
            owner->finish(Status::Ok());
        }

        void set_value(FlushResult report) const noexcept
        {
            owner->finish_report(Status::Ok(), std::move(report));
        }

        void set_error(Status status) const noexcept
        {
            owner->finish(std::move(status));
        }

        void set_stopped() const noexcept
        {
            owner->finish(Status::unavailable("maintenance stopped"));
        }
    };

    using Sender =
        std::conditional_t<std::is_same_v<Operation, FlushOperation>, FlushReportSender, detail::CallbackSender>;
    using Connected = stdexec::connect_result_t<Sender, Receiver>;

    static Sender sender(Operation &operation) noexcept
    {
        if constexpr (std::is_same_v<Operation, FlushOperation>) {
            return operation.report_sender();
        }
        else {
            return operation.sender();
        }
    }

    MaintenanceCall(std::shared_ptr<TreeOperationLifetime::Ticket> lifetime, std::shared_ptr<Operation> operation,
                    ct_future_handle result, std::shared_ptr<CompletionSignal> completion)
        : ticket(std::move(lifetime)),
          operation(std::move(operation)),
          result(std::move(result)),
          signal(std::move(completion)),
          connected(stdexec::connect(sender(*this->operation), Receiver{this}))
    {
    }

    void start() noexcept
    {
        self = this->shared_from_this();
        stdexec::start(connected);
    }

    template <class Report> void finish_report(Status status, const Report &report) noexcept
    {
        auto retained = std::move(self);
        store_report(*result, report, status);
        result->done.store(true, std::memory_order_release);
        c_api_notify_completion(signal);
    }

    void finish(Status status) noexcept
    {
        if constexpr (std::is_same_v<Operation, FlushOperation>) {
            // A rejected concurrent connection owns no progress and must not
            // observe the report being mutated by the accepted attempt.
            FlushResult rejected;
            rejected.handoff_status = status.without_message();
            finish_report(std::move(status), std::move(rejected));
        }
        else {
            finish_report(std::move(status), operation->result());
        }
    }

    std::shared_ptr<TreeOperationLifetime::Ticket> ticket;
    std::shared_ptr<Operation>                     operation;
    ct_future_handle                               result;
    std::shared_ptr<CompletionSignal>              signal;
    Connected                                      connected;
    std::shared_ptr<MaintenanceCall>               self;
};
} // namespace crowdb::tree::c_api_detail
