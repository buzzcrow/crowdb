// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "stdexec_adapter.h"

#include <gtest/gtest.h>

#include <atomic>
#include <memory>
#include <thread>

namespace crowdb::tree::detail
{
namespace
{

struct Result
{
    std::atomic<int> values{0};
    std::atomic<int> errors{0};
    std::atomic<int> stopped{0};
};

struct Receiver
{
    using receiver_concept = stdexec::receiver_tag;

    std::shared_ptr<Result> result;

    void set_value() noexcept
    {
        result->values.fetch_add(1);
    }

    void set_error(Status /*unused*/) noexcept
    {
        result->errors.fetch_add(1);
    }

    void set_stopped() noexcept
    {
        result->stopped.fetch_add(1);
    }
};

struct Source
{
    CallbackSignal signal    = CallbackSignal::kValue;
    bool           delayed   = false;
    bool           duplicate = false;
    std::thread    worker;

    ~Source()
    {
        if (worker.joinable()) {
            worker.join();
        }
    }
};

void submit(void *context, CallbackComplete complete, void *operation)
{
    auto *source = static_cast<Source *>(context);
    auto  run    = [source, complete, operation] {
        complete(operation, source->signal, Status::io_error("injected"));
        if (source->duplicate) {
            complete(operation, source->signal, Status::io_error("duplicate"));
        }
    };
    if (source->delayed) {
        source->worker = std::thread(std::move(run));
    }
    else {
        run();
    }
}

void expect_signal(CallbackSignal signal, bool delayed)
{
    Source source{.signal = signal, .delayed = delayed, .duplicate = true, .worker = {}};
    auto   result    = std::make_shared<Result>();
    auto   operation = stdexec::connect(CallbackSender(&source, &submit), Receiver{result});
    stdexec::start(operation);
    if (source.worker.joinable()) {
        source.worker.join();
    }
    EXPECT_EQ(result->values.load(), signal == CallbackSignal::kValue ? 1 : 0);
    EXPECT_EQ(result->errors.load(), signal == CallbackSignal::kError ? 1 : 0);
    EXPECT_EQ(result->stopped.load(), signal == CallbackSignal::kStopped ? 1 : 0);
}

TEST(StdexecAdapter, ImmediateAndDelayedSignalsCompleteExactlyOnce)
{
    for (CallbackSignal signal : {CallbackSignal::kValue, CallbackSignal::kError, CallbackSignal::kStopped}) {
        expect_signal(signal, false);
        expect_signal(signal, true);
    }
}

TEST(StdexecAdapter, SubmissionStartsOnlyWhenOperationStarts)
{
    Source source;
    auto   result    = std::make_shared<Result>();
    auto   operation = stdexec::connect(CallbackSender(&source, &submit), Receiver{result});
    EXPECT_EQ(result->values.load(), 0);
    stdexec::start(operation);
    EXPECT_EQ(result->values.load(), 1);
}

} // namespace
} // namespace crowdb::tree::detail
