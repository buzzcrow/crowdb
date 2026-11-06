// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once

#include "host_completion.h"
#include "stdexec_adapter.h"

namespace crowdb::tree::detail
{
struct SnapshotWaitCompletion
{
    HostCompletion done;
    Status         status;
};

struct SnapshotWaitReceiver
{
    using receiver_concept = stdexec::receiver_tag;
    SnapshotWaitCompletion *completion;

    void set_error(Status status) const noexcept
    {
        completion->status = std::move(status);
        completion->done.signal();
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

// Synchronous public facades call this only on their host thread. All CPU and
// backend work still runs through the operation's admitted asynchronous path.
template <class Operation> Status wait_snapshot_operation(const std::shared_ptr<Operation> &operation)
{
    SnapshotWaitCompletion completion;
    auto connected = stdexec::connect(operation->sender(), SnapshotWaitReceiver{.completion = &completion});
    stdexec::start(connected);
    completion.done.wait();
    return completion.status;
}
} // namespace crowdb::tree::detail
