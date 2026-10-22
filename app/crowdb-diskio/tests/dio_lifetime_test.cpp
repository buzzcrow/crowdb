// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "rpc/dio_server.h"

#include <diskio_generated.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <cerrno>
#include <chrono>
#include <cstring>
#include <vector>

namespace
{
using namespace crowdb::diskio;
using namespace crowdb::rpc;
namespace proto = crowdb::diskio::proto;

class TestDeferredEngine final : public IoEngine
{
  public:
    struct Pending
    {
        const uint8_t           *data;
        uint8_t                 *output;
        size_t                   size;
        std::function<void(int)> done;
    };

    std::vector<Pending> pending;

    void submit_write(Disk *, off_t, const uint8_t *data, size_t size, std::function<void(int)> done) override
    {
        pending.push_back({data, nullptr, size, std::move(done)});
    }

    void submit_read(Disk *, off_t, uint8_t *data, size_t size, uint64_t, std::function<void(int)> done) override
    {
        pending.push_back({nullptr, data, size, std::move(done)});
    }

    void submit_fsync(Disk *, std::function<void(int)> done) override
    {
        pending.push_back({nullptr, nullptr, 0, std::move(done)});
    }

    void complete(int result)
    {
        ASSERT_FALSE(pending.empty());
        auto io = std::move(pending.front());
        pending.erase(pending.begin());
        if (io.output != nullptr && result > 0)
            std::memset(io.output, 0x5a, std::min(io.size, static_cast<size_t>(result)));
        io.done(result);
    }
};

class TestDeferredDisk final : public Disk
{
  public:
    explicit TestDeferredDisk(IoEngine *engine) : engine_(engine)
    {
        zones_.push_back({.zone_index = 0, .base_offset = 0, .capacity = 8 << 20});
    }

    DiskType type() const override
    {
        return DiskType::Mem;
    }

    int fd() const override
    {
        return 1;
    }

    bool is_o_direct() const override
    {
        return false;
    }

    size_t block_size() const override
    {
        return 1;
    }

    IoEngine *engine() override
    {
        return engine_;
    }

    DiskId id() const override
    {
        return {7, 9};
    }

    Zone *find_zone(uint32_t index) override
    {
        return index == 0 ? &zones_[0] : nullptr;
    }

  private:
    IoEngine *engine_;
};

class DioLifetimeTest : public ::testing::Test
{
  protected:
    TestDeferredEngine            engine;
    std::shared_ptr<DiskSet>      disks = std::make_shared<DiskSet>();
    std::unique_ptr<RpcServer>    server;
    std::unique_ptr<DiskioServer> service;
    std::shared_ptr<Connection>   connection;

    void SetUp() override
    {
        disks->add(std::make_shared<TestDeferredDisk>(&engine));
        server = std::make_unique<RpcServer>();
        ASSERT_TRUE(server->configure_rdma({}, false, false, false));
        connection = std::make_shared<Connection>(1, "delayed", server->pool());
        // The unit fixture invokes handlers directly and records I/O only.
        // Completion must safely discard a reply to this closed connection.
        connection->close();
        service = std::make_unique<DiskioServer>(disks, server->shared_transport());
    }

    Frame *write(Buffer *data, uint64_t offset = 0, uint32_t size = 16, uint64_t ordering = 0)
    {
        flatbuffers::FlatBufferBuilder builder;
        crowdb::rpc::proto::FBInt128   disk(7, 9);
        const auto                     now = static_cast<uint64_t>(
            std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch())
                .count());
        auto request = proto::CreateFBDiskWriteRequest(builder, 17, 0, &disk, 0, offset, size, ordering, now);
        builder.Finish(request);
        auto *frame       = new Frame;
        frame->request_id = 17;
        frame->control.assign(builder.GetBufferPointer(), builder.GetBufferPointer() + builder.GetSize());
        frame->data_buf = data;
        return frame;
    }

    Frame *read(uint32_t size)
    {
        flatbuffers::FlatBufferBuilder builder;
        crowdb::rpc::proto::FBInt128   disk(7, 9);
        builder.Finish(proto::CreateFBDiskReadRequest(builder, 19, 0, &disk, 0, 0, size));
        auto *frame       = new Frame;
        frame->request_id = 19;
        frame->control.assign(builder.GetBufferPointer(), builder.GetBufferPointer() + builder.GetSize());
        return frame;
    }
};

TEST_F(DioLifetimeTest, DiskOwnsExactPayloadUntilCompletionWithoutReplacementAdmission)
{
    RegisteredBufferPool buffers(4096, 4);
    uint32_t             releases = 0;
    for (uint32_t index = 0; index < 4; ++index) {
        auto *data = buffers.alloc(32);
        ASSERT_NE(data, nullptr);
        data->write("prefix!registered data for disk", 30);
        data->view_offset = 7;
        data->len         = 16;
        data->ownership.store(BufferOwnership::CallerOwned);
        data->last_release_ctx = &releases;
        data->last_release_cb  = [](void *context, Buffer *buffer) {
            ++*static_cast<uint32_t *>(context);
            EXPECT_TRUE(buffer->transition(BufferOwnership::CallerOwned, BufferOwnership::Available));
            buffer->reset_view();
            buffer->release();
        };
        const auto *payload = data->payload_data();
        EXPECT_EQ(service->handle_write(write(data), connection.get()), nullptr);
        ASSERT_EQ(engine.pending.size(), index + 1);
        EXPECT_EQ(engine.pending.back().data, payload);
        EXPECT_EQ(data->ownership.load(), BufferOwnership::DiskOwned);
        EXPECT_EQ(std::memcmp(payload, "registered data ", 16), 0);
    }
    EXPECT_EQ(releases, 0U);
    EXPECT_EQ(buffers.alloc(1), nullptr);
    engine.complete(16);
    EXPECT_EQ(releases, 1U);
    auto *returned = buffers.alloc(1);
    ASSERT_NE(returned, nullptr);
    EXPECT_EQ(buffers.alloc(1), nullptr);
    returned->release();
    engine.complete(8);
    engine.complete(-EIO);
    engine.complete(-ECANCELED);
    EXPECT_EQ(releases, 4U);
    EXPECT_EQ(buffers.committed_bytes(), 4096U);
    EXPECT_EQ(buffers.quarantined_bytes(), 0U);
}

TEST_F(DioLifetimeTest, WriteCompletionRetainsTransportAndConnectionAfterBothFacadesDie)
{
    std::weak_ptr<SocketTransport> transport = server->shared_transport();
    std::weak_ptr<Connection>      peer      = connection;
    auto                          *data      = server->pool()->alloc(16);
    ASSERT_NE(data, nullptr);
    data->write("delayed I/O data", 16);
    service->handle_write(write(data), connection.get());
    ASSERT_EQ(engine.pending.size(), 1U);
    service.reset();
    server.reset();
    connection.reset();
    EXPECT_FALSE(transport.expired());
    EXPECT_FALSE(peer.expired());
    EXPECT_EQ(std::memcmp(engine.pending.front().data, "delayed I/O data", 16), 0);
    engine.complete(16);
    EXPECT_TRUE(peer.expired());
    EXPECT_TRUE(transport.expired());
}

TEST_F(DioLifetimeTest, ReadCompletionRetainsOutputAndTransportAfterBothFacadesDie)
{
    std::weak_ptr<SocketTransport> transport = server->shared_transport();
    service->handle_read(read(4096), connection.get());
    ASSERT_EQ(engine.pending.size(), 1U);
    ASSERT_NE(engine.pending.front().output, nullptr);
    service.reset();
    server.reset();
    connection.reset();
    EXPECT_FALSE(transport.expired());
    engine.complete(4096);
    EXPECT_TRUE(transport.expired());
}

TEST_F(DioLifetimeTest, MalformedControlAndPayloadOrZoneBoundsNeverReachDisk)
{
    auto *malformed = new Frame;
    malformed->control.assign(16, 0xff);
    service->handle_write(malformed, connection.get());
    auto *short_data = server->pool()->alloc(8);
    ASSERT_NE(short_data, nullptr);
    short_data->len = 8;
    service->handle_write(write(short_data), connection.get());
    for (auto offset : {uint64_t{8 << 20}, UINT64_MAX}) {
        auto *data = server->pool()->alloc(16);
        ASSERT_NE(data, nullptr);
        data->len = 16;
        service->handle_write(write(data, offset), connection.get());
    }
    auto *data = server->pool()->alloc(16);
    ASSERT_NE(data, nullptr);
    data->len = 16;
    service->handle_write(write(data, 0, 16, 1), connection.get());
    EXPECT_TRUE(engine.pending.empty());
}
} // namespace
