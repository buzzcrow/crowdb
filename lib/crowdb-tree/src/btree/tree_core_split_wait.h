// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once

#include "btree/flush_operation.h"
#include "host_completion.h"

namespace crowdb::tree::detail
{
class SplitGate
{
  public:
    explicit SplitGate(std::atomic_flag &flag) : flag_(flag), held_(!flag.test_and_set(std::memory_order_acquire))
    {
    }

    ~SplitGate()
    {
        if (held_) {
            flag_.clear(std::memory_order_release);
        }
    }

    [[nodiscard]] bool held() const noexcept
    {
        return held_;
    }

  private:
    std::atomic_flag &flag_;
    bool              held_;
};

struct SplitWait
{
    HostCompletion done;
    Status         status;
    FlushResult    result;

    void finish(Status value) noexcept
    {
        status = std::move(value);
        done.signal();
    }

    void wait() const noexcept
    {
        done.wait();
    }
};

struct SplitReportReceiver
{
    using receiver_concept = stdexec::receiver_tag;
    SplitWait *completion;

    void set_value(FlushResult result) const noexcept
    {
        completion->result = std::move(result);
        completion->finish(Status::Ok());
    }

    void set_error(Status status) const noexcept
    {
        completion->finish(std::move(status));
    }

    void set_stopped() const noexcept
    {
        completion->finish(Status::unavailable("split capture stopped"));
    }
};

inline Status wait_split_report(FlushReportSender sender, FlushResult *out)
{
    SplitWait completion;
    auto      connected = stdexec::connect(std::move(sender), SplitReportReceiver{&completion});
    stdexec::start(connected);
    completion.wait();
    *out = std::move(completion.result);
    return completion.status;
}
} // namespace crowdb::tree::detail
