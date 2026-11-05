// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "flush_operation_internal.h"
#include "host_completion.h"

namespace crowdb::tree
{
Status FlushOperation::wait()
{
    FlushResult report;
    auto        status = wait_report(false, &report);
    if (!status.ok()) {
        return status;
    }
    return report.handoff_status.ok() ? std::move(report.maintenance_status) : std::move(report.handoff_status);
}

Status FlushOperation::wait_capture(FlushResult *out)
{
    return wait_report(true, out);
}

Status FlushOperation::wait_report(bool capture_only, FlushResult *out)
{
    if (out == nullptr) {
        return Status::invalid_argument("flush host wait requires a report output");
    }
    if (detail::FlushRuntime::on_worker_thread()) {
        return Status::unavailable("a runtime worker cannot wait for a flush");
    }

    struct Completion
    {
        detail::HostCompletion done;
        Status                 status;
        FlushResult            report;
    } completion;

    auto callback = [](void *context, detail::CallbackSignal signal, Status status, FlushResult report) {
        auto &result = *static_cast<Completion *>(context);
        if (signal == detail::CallbackSignal::kValue) {
            result.report = std::move(report);
        }
        else {
            result.status =
                signal == detail::CallbackSignal::kStopped ? Status::unavailable("flush stopped") : std::move(status);
        }
        result.done.signal();
    };
    auto state = state_;
    while (!state->start(nullptr, &completion, callback, capture_only, false)) {
        auto observed = state->lifecycle.load(std::memory_order_acquire);
        while ((observed & State::kRunning) != 0) {
            state->lifecycle.wait(observed, std::memory_order_acquire);
            observed = state->lifecycle.load(std::memory_order_acquire);
        }
    }
    completion.done.wait();
    *out = std::move(completion.report);
    return completion.status;
}
} // namespace crowdb::tree
