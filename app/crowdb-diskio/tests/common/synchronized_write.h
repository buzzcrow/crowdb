// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#pragma once

#include "disk/disk.h"
#include "rpc/dio_server.h"

#include <diskio_generated.h>
#include <flatbuffers/flatbuffers.h>
#include <gtest/gtest.h>

#include <array>
#include <atomic>
#include <chrono>
#include <cstring>
#include <deque>
#include <functional>
#include <memory>
#include <optional>
#include <utility>
#include <vector>

namespace crowdb::diskio::test
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

class TestSynchronizedWrite : public testing::Test
{
  protected:
    TestSynchronizedWrite()
        : disks(std::make_shared<dio::DiskSet>()),
          disk(std::make_shared<TestWriteDisk>(&engine)),
          connection(std::make_shared<rpc::Connection>(1, "write-test", &pool)),
          server(disks, transport_owner)
    {
        payload.fill(7);
        disks->add(disk);
    }

    rpc::Frame *request(bool sync, uint64_t offset = 0, uint64_t id = 41, uint32_t size = 32,
                        std::optional<uint64_t> created_ms = std::nullopt)
    {
        flatbuffers::FlatBufferBuilder builder;
        crowdb::rpc::proto::FBInt128   identity(1, 2);
        auto                           now = static_cast<uint64_t>(
            std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch())
                .count());
        auto control = dproto::CreateFBDiskWriteRequest(builder, id, 0, &identity, 0, offset, size, 0,
                                                        created_ms.value_or(now), sync);
        builder.Finish(control);
        auto *frame       = new rpc::Frame;
        frame->request_id = id;
        frame->control.assign(builder.GetBufferPointer(), builder.GetBufferPointer() + builder.GetSize());
        if (size == 0) {
            return frame;
        }
        auto *buffer     = new rpc::Buffer;
        buffer->data     = payload.data();
        buffer->len      = size;
        buffer->capacity = 32;
        buffer->ref      = new std::atomic<int32_t>(1);
        buffer->free_ctx = &released;
        buffer->free_cb  = [](void *context) { ++*static_cast<size_t *>(context); };
        frame->data_buf  = buffer;
        return frame;
    }

    rpc::SystemBufferPool               pool;
    std::shared_ptr<TestWriteTransport> transport_owner = std::make_shared<TestWriteTransport>();
    TestWriteTransport                 &transport       = *transport_owner;
    TestWriteEngine                     engine;
    std::shared_ptr<dio::DiskSet>       disks;
    std::shared_ptr<TestWriteDisk>      disk;
    std::shared_ptr<rpc::Connection>    connection;
    dio::DiskioServer                   server;
    std::array<uint8_t, 32>             payload{};
    size_t                              released = 0;
};

} // namespace crowdb::diskio::test
