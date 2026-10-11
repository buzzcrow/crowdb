// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "group0/ffi_result.h"

#include <gtest/gtest.h>

#include <thread>

TEST(Group0FfiResult, LateCompletionRetainsAndThenReleasesAbandonedResult)
{
    auto                                     result   = std::make_shared<crowdb::diskio::FfiResult>();
    std::weak_ptr<crowdb::diskio::FfiResult> lifetime = result;
    auto                                    *callback = crowdb::diskio::retain_ffi_result(result);
    result.reset();
    ASSERT_FALSE(lifetime.expired());
    std::thread completion([callback] { crowdb::diskio::complete_ffi_result(0, "late disk listing", callback); });
    completion.join();
    EXPECT_TRUE(lifetime.expired());
}

TEST(Group0FfiResult, CompletionPublishesOwnedResponseAndErrorStatus)
{
    auto        result   = std::make_shared<crowdb::diskio::FfiResult>();
    auto       *callback = crowdb::diskio::retain_ffi_result(result);
    std::thread completion([callback] {
        std::string response = "request failed";
        crowdb::diskio::complete_ffi_result(7, response.c_str(), callback);
    });
    completion.join();
    ASSERT_TRUE(result->done.load(std::memory_order_acquire));
    EXPECT_EQ(result->status.load(std::memory_order_relaxed), 7);
    EXPECT_EQ(result->json_result, "request failed");
    EXPECT_EQ(result.use_count(), 1);
}
