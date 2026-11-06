// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "host_completion.h"

#include <gtest/gtest.h>

#include <memory>
#include <thread>

using crowdb::tree::detail::HostCompletion;

TEST(HostCompletion, SignalBeforeWaitPublishesTheResult)
{
    HostCompletion completion;
    completion.signal();
    completion.wait();
}

TEST(HostCompletion, WaiterCanReleaseStorageBeforeJoiningTheNotifier)
{
    for (size_t attempt = 0; attempt < 256; ++attempt) {
        auto        completion = std::make_unique<HostCompletion>();
        int         result     = 0;
        std::thread notifier([signal = completion.get(), &result] {
            result = 42;
            signal->signal();
        });
        completion->wait();
        EXPECT_EQ(result, 42);
        completion.reset();
        notifier.join();
    }
}

#ifdef CROWDB_TREE_TEST_UTIL
TEST(HostCompletion, ReadyWaiterStillRetainsStorageDuringNotification)
{
    struct TestNotification
    {
        std::atomic<bool> entered{false};
        std::atomic<bool> release{false};
    } notification;

    HostCompletion    completion;
    std::atomic<bool> waiting{false};
    std::atomic<bool> returned{false};
    std::thread       notifier([&] {
        completion.signal_for_tests(
            [](void *context) {
                auto &state = *static_cast<TestNotification *>(context);
                state.entered.store(true, std::memory_order_release);
                state.entered.notify_one();
                state.release.wait(false, std::memory_order_acquire);
            },
            &notification);
    });
    notification.entered.wait(false, std::memory_order_acquire);
    std::thread waiter([&] {
        waiting.store(true, std::memory_order_release);
        waiting.notify_one();
        completion.wait();
        returned.store(true, std::memory_order_release);
    });
    waiting.wait(false, std::memory_order_acquire);
    for (size_t attempt = 0; attempt < 1'000; ++attempt) {
        std::this_thread::yield();
    }
    EXPECT_FALSE(returned.load(std::memory_order_acquire));
    notification.release.store(true, std::memory_order_release);
    notification.release.notify_one();
    waiter.join();
    notifier.join();
    EXPECT_TRUE(returned.load(std::memory_order_acquire));
}
#endif
