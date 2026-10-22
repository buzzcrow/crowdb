// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#pragma once

#include "crowdb-common/runtime_path.h"
#include "crowdb-rpc/client/client.h"
#include "disk/block_disk.h"
#include "engine/uring/uring_engine.h"
#include "rpc/dio_server.h"

#include <diskio_generated.h>
#include <gtest/gtest.h>
#include <msg_type_generated.h>

#if defined(CROWDB_HAVE_LIBURING)
#    include <fcntl.h>
#    include <unistd.h>

#    include <atomic>
#    include <chrono>
#    include <filesystem>
#    include <thread>

namespace crowdb::diskio
{
class TestObservedEngine final : public IoEngine
{
  public:
    struct ParkedWrite
    {
        Disk                    *disk;
        off_t                    offset;
        const uint8_t           *data;
        size_t                   size;
        std::function<void(int)> done;
    };

    UringEngine                            io{32};
    std::atomic<uintptr_t>                 write_pointer{0}, read_pointer{0};
    std::atomic<uint64_t>                  reads{0};
    std::atomic<bool>                      hold_writes{false};
    std::atomic<ParkedWrite *>             parked{nullptr};
    std::shared_ptr<std::atomic<uint64_t>> completed_writes = std::make_shared<std::atomic<uint64_t>>(0);

    void submit_write(Disk *disk, off_t offset, const uint8_t *data, size_t size,
                      std::function<void(int)> done) override
    {
        write_pointer.store(reinterpret_cast<uintptr_t>(data));
        ParkedWrite write{disk, offset, data, size, std::move(done)};
        if (hold_writes.load(std::memory_order_acquire)) {
            parked.store(new ParkedWrite(std::move(write)), std::memory_order_release);
            return;
        }
        start_write(std::move(write));
    }

    void submit_read(Disk *disk, off_t offset, uint8_t *data, size_t size, uint64_t pattern,
                     std::function<void(int)> done) override
    {
        read_pointer.store(reinterpret_cast<uintptr_t>(data));
        reads.fetch_add(1);
        io.submit_read(disk, offset, data, size, pattern, std::move(done));
    }

    void submit_fsync(Disk *disk, std::function<void(int)> done) override
    {
        io.submit_fsync(disk, std::move(done));
    }

    void resume_write()
    {
        auto pending = std::unique_ptr<ParkedWrite>(parked.exchange(nullptr, std::memory_order_acq_rel));
        if (pending != nullptr)
            start_write(std::move(*pending));
    }

  private:
    void start_write(ParkedWrite write)
    {
        io.submit_write(write.disk, write.offset, write.data, write.size,
                        [done = std::move(write.done), completed = completed_writes](int result) {
                            done(result);
                            completed->fetch_add(1, std::memory_order_release);
                        });
    }
};

struct TestDiskCall
{
    std::atomic<uint32_t> callbacks{0};
    crowdb_rpc_status     status  = CROWDB_RPC_ERR_CONN_ERROR;
    crowdb_rpc_buffer_t   control = nullptr, data = nullptr;

    ~TestDiskCall()
    {
        release_data();
        if (control != nullptr)
            crowdb_rpc_buffer_release(control);
    }

    void release_data()
    {
        if (data != nullptr)
            crowdb_rpc_buffer_release(std::exchange(data, nullptr));
    }

    static void complete(uint64_t, crowdb_rpc_buffer_t control, crowdb_rpc_buffer_t data, crowdb_rpc_status status,
                         void *context)
    {
        const auto owner =
            std::unique_ptr<std::shared_ptr<TestDiskCall>>(static_cast<std::shared_ptr<TestDiskCall> *>(context));
        auto &call   = **owner;
        call.control = control;
        call.data    = data;
        call.status  = status;
        call.callbacks.fetch_add(1, std::memory_order_release);
    }
};

class TestDioHardware : public ::testing::Test
{
  protected:
    bool                                     use_rdma           = true;
    bool                                     allow_tcp_fallback = false;
    static constexpr uint32_t                BLOCK              = 1 << 20;
    std::shared_ptr<TestObservedEngine>      engine;
    std::shared_ptr<DiskSet>                 disks;
    std::unique_ptr<crowdb::rpc::RpcServer>  server, client;
    std::unique_ptr<DiskioServer>            service;
    std::shared_ptr<crowdb::rpc::Connection> peer;
    crowdb::rpc::RpcClient                   rpc;
    std::atomic<uintptr_t>                   receive_pointer{0};
    std::atomic<uintptr_t>                   read_receive_pointer{0};
    std::filesystem::path                    path;

    void SetUp() override
    {
        const auto *address = use_rdma ? std::getenv("CROWDB_RDMA_E2E_LISTEN_ADDR") : "127.0.0.1";
        if (use_rdma && (std::getenv("CROWDB_RDMA_E2E") == nullptr || address == nullptr))
            GTEST_SKIP() << "set CROWDB_RDMA_E2E and local HCA address";
#    ifndef CROWDB_RPC_HAVE_RDMA
        if (use_rdma)
            GTEST_SKIP() << "RDMA support not compiled";
#    endif
        path          = crowdb::common::test_runtime_path("diskio-rdma") / "block.bin";
        const auto fd = ::open(path.c_str(), O_CREAT | O_TRUNC | O_RDWR, 0600);
        ASSERT_GE(fd, 0);
        const auto sized = ::ftruncate(fd, 4 * BLOCK);
        ::close(fd);
        ASSERT_EQ(sized, 0);
        engine    = std::make_shared<TestObservedEngine>();
        auto disk = std::make_shared<BlockDisk>(
            DiskId{
                7, 9
        },
            path.string(), engine, std::vector<Zone>{{.zone_index = 0, .base_offset = 0, .capacity = 4 * BLOCK}}, true);
        ASSERT_GE(disk->fd(), 0);
        ASSERT_TRUE(disk->is_o_direct());
        disks = std::make_shared<DiskSet>();
        disks->add(std::move(disk));
        crowdb::rpc::RdmaConfig config =
            crowdb::rpc::RdmaConfig::for_layout(crowdb::rpc::RdmaWireLayout::ControlData, BLOCK);
        config.use_environment = false;
        config.sq_depth = config.rq_depth = config.posted_receive_target = 4;
        config.cq_depth                                                  = 8;
        config.send_arena_bytes                                          = 4 * BLOCK;
        config.receive_arena_bytes                                       = uint64_t{4} * (BLOCK + 1024);
        config.source_address                                            = address;
        if (const auto *device = std::getenv("CROWDB_RDMA_DEVICE"))
            config.device = device;
        server = std::make_unique<crowdb::rpc::RpcServer>();
        ASSERT_TRUE(server->configure_rdma(config, use_rdma, allow_tcp_fallback, use_rdma));
        ASSERT_TRUE(server->listen(address, 0));
        service = std::make_unique<DiskioServer>(disks, server->shared_transport());
        service->register_handlers(*server);
        const auto write_type = static_cast<uint16_t>(crowdb::rpc::proto::FBMsgType_EDiskWriteRequest);
        server->register_handler(write_type, [this](crowdb::rpc::Frame *frame, crowdb::rpc::Connection *connection) {
            receive_pointer.store(reinterpret_cast<uintptr_t>(frame->data_buf->payload_data()));
            return service->handle_write(frame, connection);
        });
        const auto read_type = static_cast<uint16_t>(crowdb::rpc::proto::FBMsgType_EDiskReadRequest);
        server->register_handler(read_type, [this](crowdb::rpc::Frame *frame, crowdb::rpc::Connection *connection) {
            read_receive_pointer.store(
                frame->data_buf == nullptr ? 0 : reinterpret_cast<uintptr_t>(frame->data_buf->payload_data()));
            return service->handle_read(frame, connection);
        });
        server->start();
        client = std::make_unique<crowdb::rpc::RpcServer>();
        ASSERT_TRUE(client->configure_rdma(config, use_rdma, allow_tcp_fallback, false));
        client->start();
        peer = client->transport()->connect(address, server->listen_port());
        ASSERT_NE(peer, nullptr);
        ASSERT_EQ(peer->path(), use_rdma ? crowdb::rpc::ConnectionPath::Rdma : crowdb::rpc::ConnectionPath::Tcp);
        rpc.attach(peer.get());
    }

    void TearDown() override
    {
        if (engine != nullptr)
            engine->resume_write();
        if (peer != nullptr) {
            peer->close();
            rpc.fail_all(peer.get(), crowdb::rpc::RpcError::ConnectionClosed);
        }
        if (client != nullptr)
            client->stop();
        if (server != nullptr)
            server->stop();
        service.reset();
        disks.reset();
        engine.reset();
        if (!path.empty())
            std::filesystem::remove(path);
    }

    template <typename Predicate> bool wait_for(Predicate predicate)
    {
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
        while (!predicate() && std::chrono::steady_clock::now() < deadline)
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        return predicate();
    }

    std::shared_ptr<TestDiskCall> call(uint64_t id, uint32_t index, bool writing)
    {
        flatbuffers::FlatBufferBuilder builder;
        crowdb::rpc::proto::FBInt128   disk(7, 9);
        uint16_t                       type;
        crowdb::rpc::Buffer           *data = nullptr;
        if (writing) {
            const auto now = static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::milliseconds>(
                                                       std::chrono::system_clock::now().time_since_epoch())
                                                       .count());
            builder.Finish(
                proto::CreateFBDiskWriteRequest(builder, id, 0, &disk, 0, uint64_t{index} * BLOCK, BLOCK, 0, now));
            type = static_cast<uint16_t>(crowdb::rpc::proto::FBMsgType_EDiskWriteRequest);
            data = client->transport()->allocate_producer_buffer(BLOCK, use_rdma);
            if (data == nullptr)
                return nullptr;
            for (uint32_t byte = 0; byte < BLOCK; ++byte)
                data->data[byte] = static_cast<uint8_t>(byte * 131U + index);
            data->len = BLOCK;
        }
        else {
            builder.Finish(proto::CreateFBDiskReadRequest(builder, id, 0, &disk, 0, uint64_t{index} * BLOCK, BLOCK));
            type = static_cast<uint16_t>(crowdb::rpc::proto::FBMsgType_EDiskReadRequest);
        }
        auto *control = client->pool()->alloc(builder.GetSize());
        if (control == nullptr) {
            if (data != nullptr)
                data->release();
            return nullptr;
        }
        control->write(builder.GetBufferPointer(), builder.GetSize());
        auto  result  = std::make_shared<TestDiskCall>();
        auto *context = new std::shared_ptr<TestDiskCall>(result);
        if (!rpc.send(client->transport(), peer.get(), id, control, data, type, TestDiskCall::complete, context)) {
            delete context;
            return nullptr;
        }
        return result;
    }
};
} // namespace crowdb::diskio
#endif
