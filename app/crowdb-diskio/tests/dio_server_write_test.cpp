// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "common/synchronized_write.h"

#include <diskio_generated.h>
#include <flatbuffers/flatbuffers.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstring>
#include <deque>
#include <functional>
#include <memory>
#include <utility>
#include <vector>

namespace
{
namespace dproto = crowdb::diskio::proto;
namespace dio    = crowdb::diskio;
namespace rpc    = crowdb::rpc;

using DiskioSynchronizedWrite = crowdb::diskio::test::TestSynchronizedWrite;

TEST_F(DiskioSynchronizedWrite, ReplyWaitsForBothWriteAndFlush)
{
    server.handle_write(request(true), connection.get());
    ASSERT_EQ(engine.writes.size(), 1);
    EXPECT_TRUE(engine.flushes.empty());
    EXPECT_TRUE(transport.replies.empty());
    engine.write_done(32);
    ASSERT_EQ(engine.flushes.size(), 1);
    EXPECT_TRUE(transport.replies.empty());
    EXPECT_EQ(released, 0);
    engine.flush_done(0);
    ASSERT_EQ(transport.replies.size(), 1);
    EXPECT_EQ(transport.reply()->ret_code(), dproto::FBDiskIoRetCode_Success);
    EXPECT_TRUE(transport.reply()->sync_completed());
    EXPECT_EQ(released, 1);
}

TEST_F(DiskioSynchronizedWrite, BufferedWriteDoesNotFlush)
{
    server.handle_write(request(false), connection.get());
    engine.write_done(32);
    EXPECT_TRUE(engine.flushes.empty());
    ASSERT_EQ(transport.replies.size(), 1);
    EXPECT_FALSE(transport.reply()->sync_completed());
    EXPECT_EQ(released, 1);
}

TEST_F(DiskioSynchronizedWrite, FailedWriteDoesNotFlushOrSucceed)
{
    server.handle_write(request(true), connection.get());
    engine.write_done(-EIO);
    EXPECT_TRUE(engine.flushes.empty());
    ASSERT_EQ(transport.replies.size(), 1);
    EXPECT_EQ(transport.reply()->ret_code(), dproto::FBDiskIoRetCode_IoError);
    EXPECT_FALSE(transport.reply()->sync_completed());
    EXPECT_EQ(released, 1);
}

TEST_F(DiskioSynchronizedWrite, PartialWriteDoesNotFlushOrSucceed)
{
    server.handle_write(request(true), connection.get());
    engine.write_done(31);
    EXPECT_TRUE(engine.flushes.empty());
    ASSERT_EQ(transport.replies.size(), 1);
    EXPECT_EQ(transport.reply()->ret_code(), dproto::FBDiskIoRetCode_PartialWrite);
    EXPECT_FALSE(transport.reply()->sync_completed());
}

TEST_F(DiskioSynchronizedWrite, FailedFlushReturnsDedicatedFailure)
{
    server.handle_write(request(true), connection.get());
    engine.write_done(32);
    engine.flush_done(-EIO);
    ASSERT_EQ(transport.replies.size(), 1);
    EXPECT_EQ(transport.reply()->ret_code(), dproto::FBDiskIoRetCode_FsyncFailed);
    EXPECT_FALSE(transport.reply()->sync_completed());
    EXPECT_EQ(released, 1);
}

TEST_F(DiskioSynchronizedWrite, FlushRetainsRemovedDiskConnectionAndPayload)
{
    std::weak_ptr<dio::Disk>       weak_disk       = disk;
    std::weak_ptr<rpc::Connection> weak_connection = connection;
    server.handle_write(request(true), connection.get());
    ASSERT_TRUE(disks->remove_disk(disk->id()));
    disk.reset();
    connection.reset();
    engine.write_done(32);
    EXPECT_FALSE(weak_disk.expired());
    EXPECT_FALSE(weak_connection.expired());
    EXPECT_EQ(released, 0);
    engine.flush_done(0);
    EXPECT_TRUE(weak_disk.expired());
    EXPECT_TRUE(weak_connection.expired());
    EXPECT_EQ(released, 1);
    EXPECT_EQ(transport.attempts, 1);
}

TEST_F(DiskioSynchronizedWrite, RejectedResponseStillReleasesOwners)
{
    transport.reject                               = true;
    std::weak_ptr<rpc::Connection> weak_connection = connection;
    server.handle_write(request(true), connection.get());
    connection.reset();
    engine.write_done(32);
    engine.flush_done(0);
    EXPECT_EQ(transport.attempts, 1);
    EXPECT_TRUE(transport.replies.empty());
    EXPECT_EQ(released, 1);
    EXPECT_TRUE(weak_connection.expired());
}

TEST_F(DiskioSynchronizedWrite, InvalidControlPayloadAndZoneAreRejectedBeforeIo)
{
    auto *malformed = request(true);
    malformed->control.resize(3);
    server.handle_write(malformed, connection.get());
    auto *short_payload          = request(true);
    short_payload->data_buf->len = 16;
    server.handle_write(short_payload, connection.get());
    server.handle_write(request(true, 4090), connection.get());
    EXPECT_TRUE(engine.writes.empty());
    EXPECT_TRUE(engine.flushes.empty());
    ASSERT_EQ(transport.replies.size(), 3);
    for (size_t index = 0; index != 3; ++index) {
        EXPECT_EQ(transport.reply(index)->ret_code(), dproto::FBDiskIoRetCode_IoError);
        EXPECT_FALSE(transport.reply(index)->sync_completed());
    }
    EXPECT_EQ(released, 3);
}

TEST_F(DiskioSynchronizedWrite, SameStripWritesAndTheirFlushesRetainOrder)
{
    disk->block = 4096;
    server.handle_write(request(true, 0, 41), connection.get());
    server.handle_write(request(true, 32, 42), connection.get());
    ASSERT_EQ(engine.writes.size(), 1);
    engine.write_done(4096);
    ASSERT_EQ(engine.writes.size(), 1);
    EXPECT_TRUE(std::all_of(engine.writes.front().data, engine.writes.front().data + 64,
                            [](uint8_t value) { return value == 7; }));
    ASSERT_EQ(engine.flushes.size(), 1);
    EXPECT_TRUE(transport.replies.empty());
    engine.flush_done(0);
    ASSERT_EQ(transport.replies.size(), 1);
    EXPECT_EQ(transport.reply()->id(), 41);
    engine.write_done(4096);
    engine.flush_done(0);
    ASSERT_EQ(transport.replies.size(), 2);
    EXPECT_EQ(transport.reply(1)->id(), 42);
    EXPECT_TRUE(transport.reply()->sync_completed());
    EXPECT_TRUE(transport.reply(1)->sync_completed());
    EXPECT_EQ(released, 2);
}
} // namespace
