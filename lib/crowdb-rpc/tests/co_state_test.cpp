// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "common/test_co_state.h"

#include <gtest/gtest.h>

#include <thread>

using namespace crowdb::rpc;

TEST(CoroutineCompletion, ResponseBeforeSuspendPublishesOnce)
{
    TestCoroutine coroutine;
    coroutine.complete();
    EXPECT_EQ(coroutine.state.co_state.load(), CO_RESPONSE_READY);
    coroutine.task.handle.resume();
    EXPECT_EQ(coroutine.finished.load(), 1U);
    EXPECT_EQ(coroutine.state.total_ops, 1U);
    EXPECT_EQ(coroutine.state.active_callbacks.load(), 0U);
}

TEST(CoroutineCompletion, ResponseAfterSuspendResumesOnce)
{
    TestCoroutine coroutine;
    coroutine.task.handle.resume();
    EXPECT_EQ(coroutine.state.co_state.load(), CO_SUSPENDED);
    coroutine.complete();
    EXPECT_EQ(coroutine.finished.load(), 1U);
    EXPECT_EQ(coroutine.state.total_ops, 1U);
    EXPECT_EQ(coroutine.state.active_callbacks.load(), 0U);
    coroutine.complete();
    EXPECT_EQ(coroutine.finished.load(), 1U);
    EXPECT_EQ(coroutine.state.co_state.load(), CO_IDLE);
}

TEST(CoroutineCompletion, ConcurrentPublicationCannotLoseResume)
{
    for (int iteration = 0; iteration < 2000; ++iteration) {
        TestCoroutine coroutine;
        std::thread   completing([&coroutine] { coroutine.complete(); });
        coroutine.task.handle.resume();
        completing.join();
        ASSERT_EQ(coroutine.finished.load(std::memory_order_acquire), 1U) << iteration;
        EXPECT_EQ(coroutine.state.total_ops, 1U);
        EXPECT_EQ(coroutine.state.active_callbacks.load(), 0U);
    }
}
