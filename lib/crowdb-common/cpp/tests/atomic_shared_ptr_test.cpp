// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "crowdb-common/atomic_shared_ptr.h"

#include <gtest/gtest.h>

#include <type_traits>

// Platforms with the specialization must never instantiate the deprecated
// free-function implementation, even when warnings are treated as errors.
#if defined(__cpp_lib_atomic_shared_ptr) && __cpp_lib_atomic_shared_ptr >= 201711L
static_assert(std::is_same_v<crowdb::common::AtomicSharedPtr<const int>, std::atomic<std::shared_ptr<const int>>>);
#endif

TEST(AtomicSharedPtrTest, FailedPublicationUpdatesExpectedAndPreservesSnapshot)
{
    const auto                                 first  = std::make_shared<const int>(1);
    const auto                                 second = std::make_shared<const int>(2);
    const auto                                 third  = std::make_shared<const int>(3);
    crowdb::common::AtomicSharedPtr<const int> snapshot(first);
    auto                                       retained = snapshot.load(std::memory_order_acquire);
    snapshot.store(second, std::memory_order_release);

    auto expected = first;
    EXPECT_FALSE(snapshot.compare_exchange_weak(expected, third, std::memory_order_release, std::memory_order_acquire));
    EXPECT_EQ(expected, second);
    EXPECT_EQ(snapshot.load(std::memory_order_acquire), second);
    EXPECT_EQ(*retained, 1);

    while (!snapshot.compare_exchange_weak(expected, third, std::memory_order_release, std::memory_order_acquire)) {
    }
    EXPECT_EQ(snapshot.load(std::memory_order_acquire), third);
    EXPECT_EQ(*retained, 1);
}
