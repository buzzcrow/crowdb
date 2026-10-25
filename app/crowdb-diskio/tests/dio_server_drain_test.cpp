// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "common/synchronized_write.h"

#include <gtest/gtest.h>

#include <chrono>
#include <cstdint>
#include <limits>
#include <thread>

namespace
{
namespace dproto       = crowdb::diskio::proto;
using DiskioWriteDrain = crowdb::diskio::test::TestSynchronizedWrite;

TEST_F(DiskioWriteDrain, EmptySynchronizationWaitsForPriorByteWritesAndItsFlush)
{
    server.handle_write(request(false), connection.get());
    server.handle_write(request(true, 0, 42, 0), connection.get());
    EXPECT_EQ(engine.writes.size(), 1);
    EXPECT_TRUE(engine.flushes.empty());
    EXPECT_TRUE(transport.replies.empty());
    engine.write_done(32);
    EXPECT_TRUE(engine.writes.empty());
    EXPECT_EQ(engine.flushes.size(), 1);
    EXPECT_EQ(transport.replies.size(), 1);
    engine.flush_done(0);
    ASSERT_EQ(transport.replies.size(), 2);
    EXPECT_EQ(transport.reply(1)->id(), 42);
    EXPECT_EQ(transport.reply(1)->ret_code(), dproto::FBDiskIoRetCode_Success);
    EXPECT_TRUE(transport.reply(1)->sync_completed());
    EXPECT_EQ(released, 1);
}

TEST_F(DiskioWriteDrain, MissingStaleAndUnboundedFutureTimesAreRejectedBeforeIo)
{
    for (const uint64_t created_ms : {uint64_t{0}, uint64_t{1}, std::numeric_limits<uint64_t>::max()}) {
        server.handle_write(request(true, 0, 41, 32, created_ms), connection.get());
    }
    EXPECT_TRUE(engine.writes.empty());
    EXPECT_TRUE(engine.flushes.empty());
    ASSERT_EQ(transport.replies.size(), 3);
    for (size_t index = 0; index < 3; ++index) {
        EXPECT_EQ(transport.reply(index)->ret_code(), dproto::FBDiskIoRetCode_OldRequest);
        EXPECT_FALSE(transport.reply(index)->sync_completed());
    }
    EXPECT_EQ(released, 3);
}

TEST_F(DiskioWriteDrain, AQueuedExpiredWriteCannotRunBeforeTheFreshDrain)
{
    crowdb::diskio::DiskioServer guarded(disks, &transport, 50, 1);
    guarded.handle_write(request(false, 0, 41), connection.get());
    guarded.handle_write(request(false, 32, 42), connection.get());
    EXPECT_EQ(engine.writes.size(), 1);
    EXPECT_TRUE(transport.replies.empty());
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    guarded.handle_write(request(true, 0, 43, 0), connection.get());
    engine.write_done(32);
    EXPECT_TRUE(engine.writes.empty());
    EXPECT_EQ(engine.flushes.size(), 1);
    while (!engine.writes.empty()) {
        engine.write_done(32);
    }
    while (!engine.flushes.empty()) {
        engine.flush_done(0);
    }
    ASSERT_EQ(transport.replies.size(), 3);
    EXPECT_EQ(transport.reply()->id(), 41);
    EXPECT_EQ(transport.reply(1)->id(), 42);
    EXPECT_EQ(transport.reply(1)->ret_code(), dproto::FBDiskIoRetCode_OldRequest);
    EXPECT_FALSE(transport.reply(1)->sync_completed());
    EXPECT_EQ(transport.reply(2)->id(), 43);
    EXPECT_TRUE(transport.reply(2)->sync_completed());
}
} // namespace
