// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "disk/disk.h"
#include "rpc/dio_server.h"

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

class TestWriteEngine final : public dio::IoEngine
{
  public:
    struct PendingWrite
    {
        const uint8_t           *data;
        size_t                   size;
        std::function<void(int)> complete;
    };

    void submit_write(dio::Disk * /*disk*/, off_t /*offset*/, const uint8_t *data, size_t size,
                      std::function<void(int)> complete) override
    {
        writes.push_back({.data = data, .size = size, .complete = std::move(complete)});
    }

    void submit_read(dio::Disk * /*disk*/, off_t /*offset*/, uint8_t *data, size_t size, uint64_t /*pattern*/,
                     std::function<void(int)> complete) override
    {
        std::memset(data, 0, size);
        complete(static_cast<int>(size));
    }

    void submit_fsync(dio::Disk * /*disk*/, std::function<void(int)> complete) override
    {
        flushes.push_back(std::move(complete));
    }

    void write_done(int result)
    {
        ASSERT_FALSE(writes.empty());
        auto pending = std::move(writes.front());
        writes.pop_front();
        pending.complete(result);
    }

    void flush_done(int result)
    {
        ASSERT_FALSE(flushes.empty());
        auto complete = std::move(flushes.front());
        flushes.pop_front();
        complete(result);
    }

    std::deque<PendingWrite>             writes;
    std::deque<std::function<void(int)>> flushes;
};

class TestWriteDisk final : public dio::Disk
{
  public:
    explicit TestWriteDisk(dio::IoEngine *engine) : engine_(engine)
    {
        zones_.push_back({.zone_index = 0, .base_offset = 0, .capacity = 4096});
    }

    [[nodiscard]] dio::DiskType type() const override
    {
        return dio::DiskType::Mem;
    }

    [[nodiscard]] int fd() const override
    {
        return 1;
    }

    [[nodiscard]] bool is_o_direct() const override
    {
        return block > 1;
    }

    [[nodiscard]] size_t block_size() const override
    {
        return block;
    }

    dio::IoEngine *engine() override
    {
        return engine_;
    }

    [[nodiscard]] dio::DiskId id() const override
    {
        return {.high = 1, .low = 2};
    }

    dio::Zone *find_zone(uint32_t index) override
    {
        return index == 0 ? &zones_.front() : nullptr;
    }

    size_t block = 1;

  private:
    dio::IoEngine *engine_;
};

class TestWriteTransport final : public rpc::Transport
{
  public:
    ~TestWriteTransport() override
    {
        for (auto *frame : replies) {
            frame->control->release();
            frame->release_data();
            delete frame;
        }
    }

    bool submit(rpc::Connection * /*connection*/, rpc::OutFrame *frame) override
    {
        ++attempts;
        if (reject) {
            return false;
        }
        replies.push_back(frame);
        return true;
    }

    rpc::Buffer *register_buffer(rpc::Buffer *buffer) override
    {
        return buffer;
    }

    void shutdown() override
    {
    }

    [[nodiscard]] const dproto::FBDiskWriteResponse *reply(size_t index = 0) const
    {
        return flatbuffers::GetRoot<dproto::FBDiskWriteResponse>(replies.at(index)->control->data);
    }

    bool                         reject   = false;
    size_t                       attempts = 0;
    std::vector<rpc::OutFrame *> replies;
};

class DiskioSynchronizedWrite : public testing::Test
{
  protected:
    DiskioSynchronizedWrite()
        : disks(std::make_shared<dio::DiskSet>()),
          disk(std::make_shared<TestWriteDisk>(&engine)),
          connection(std::make_shared<rpc::Connection>(1, "write-test", &pool)),
          server(disks, &transport)
    {
        payload.fill(7);
        disks->add(disk);
    }

    rpc::Frame *request(bool sync, uint64_t offset = 0, uint64_t id = 41)
    {
        flatbuffers::FlatBufferBuilder builder;
        crowdb::rpc::proto::FBInt128   identity(1, 2);
        auto                           now = static_cast<uint64_t>(
            std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch())
                .count());
        auto control = dproto::CreateFBDiskWriteRequest(builder, id, 0, &identity, 0, offset, 32, 0, now, sync);
        builder.Finish(control);
        auto *frame       = new rpc::Frame;
        frame->request_id = id;
        frame->control.assign(builder.GetBufferPointer(), builder.GetBufferPointer() + builder.GetSize());
        auto *buffer     = new rpc::Buffer;
        buffer->data     = payload.data();
        buffer->len      = 32;
        buffer->capacity = 32;
        buffer->ref      = new std::atomic<int32_t>(1);
        buffer->free_ctx = &released;
        buffer->free_cb  = [](void *context) { ++*static_cast<size_t *>(context); };
        frame->data_buf  = buffer;
        return frame;
    }

    rpc::SystemBufferPool            pool;
    TestWriteTransport               transport;
    TestWriteEngine                  engine;
    std::shared_ptr<dio::DiskSet>    disks;
    std::shared_ptr<TestWriteDisk>   disk;
    std::shared_ptr<rpc::Connection> connection;
    dio::DiskioServer                server;
    std::array<uint8_t, 32>          payload{};
    size_t                           released = 0;
};

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
