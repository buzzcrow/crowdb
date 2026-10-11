// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "crowdb-rpc/client/client.h"
#include "crowdb-rpc/pool.h"

#include <gtest/gtest.h>

#include <atomic>
#include <functional>
#include <thread>

namespace
{
using namespace crowdb::rpc;

class TestSubmitTransport final : public Transport
{
  public:
    std::function<bool(Connection *, OutFrame *)> action;

    bool submit(Connection *connection, OutFrame *frame) override
    {
        return action(connection, frame);
    }

    Buffer *register_buffer(Buffer *buffer) override
    {
        return buffer;
    }

    void shutdown() override
    {
    }
};

struct TestCompletion
{
    std::atomic<int>      calls{0};
    std::function<void()> after;
};

void complete(uint64_t /*request*/, crowdb_rpc_buffer_t /*control*/, crowdb_rpc_buffer_t /*data*/,
              crowdb_rpc_status /*status*/, void *context)
{
    auto *completion = static_cast<TestCompletion *>(context);
    ++completion->calls;
    if (completion->after) {
        completion->after();
    }
}

void consume(OutFrame *frame)
{
    if (frame->control != nullptr) {
        frame->control->release();
    }
    frame->release_data();
    delete frame;
}

void check_rejection_ownership(bool slab)
{
    SystemBufferPool pool;
    RpcClient        caller;
    if (slab) {
        caller.set_completion_pool_size(1);
    }
    auto                connection = std::make_shared<Connection>(1, "submit-race", &pool);
    TestSubmitTransport transport;
    TestCompletion      completion;
    transport.action = [](Connection *, OutFrame *) { return false; };
    EXPECT_FALSE(caller.send(&transport, connection.get(), 1, pool.alloc(1), nullptr, 42, complete, &completion));
    EXPECT_EQ(completion.calls.load(), 0);
    EXPECT_EQ(caller.pending_count(), 0U);
    transport.action = [&](Connection *owner, OutFrame *) {
        caller.fail_all(owner, RpcError::ConnectionClosed);
        return false;
    };
    EXPECT_TRUE(caller.send(&transport, connection.get(), 2, pool.alloc(1), nullptr, 42, complete, &completion));
    EXPECT_EQ(completion.calls.load(), 1);
    EXPECT_EQ(caller.pending_count(), 0U);
}
} // namespace

TEST(ClientSubmitOwnershipTest, RejectedSlabSubmitReturnsCompletionOwnership)
{
    check_rejection_ownership(true);
}

TEST(ClientSubmitOwnershipTest, RejectedMapSubmitReturnsCompletionOwnership)
{
    check_rejection_ownership(false);
}

TEST(ClientSubmitOwnershipTest, RejectedOlderSubmitCannotCancelReusedSlot)
{
    SystemBufferPool pool;
    RpcClient        caller;
    caller.set_completion_pool_size(1);
    auto                connection = std::make_shared<Connection>(1, "reused-slot", &pool);
    TestSubmitTransport transport;
    TestCompletion      first;
    TestCompletion      next;
    first.after = [&] {
        EXPECT_TRUE(caller.send(&transport, connection.get(), 2, pool.alloc(1), nullptr, 42, complete, &next));
    };
    transport.action = [&](Connection *owner, OutFrame *frame) {
        if (frame->request_id == 1) {
            caller.fail_all(owner, RpcError::ConnectionClosed);
            return false;
        }
        consume(frame);
        return true;
    };
    EXPECT_TRUE(caller.send(&transport, connection.get(), 1, pool.alloc(1), nullptr, 42, complete, &first));
    EXPECT_EQ(first.calls.load(), 1);
    EXPECT_EQ(next.calls.load(), 0);
    EXPECT_EQ(caller.pending_count(), 1U);
    auto *response       = new Frame;
    response->request_id = 2;
    EXPECT_TRUE(caller.on_response(2, response));
    EXPECT_EQ(next.calls.load(), 1);
    EXPECT_EQ(caller.pending_count(), 0U);
}

static void check_concurrent_completion(bool slab)
{
    SystemBufferPool pool;
    RpcClient        caller;
    if (slab) {
        caller.set_completion_pool_size(1);
    }
    auto                connection = std::make_shared<Connection>(1, "map-claim", &pool);
    TestSubmitTransport transport;
    transport.action = [](Connection *, OutFrame *frame) {
        consume(frame);
        return true;
    };
    for (uint64_t request = 1; request <= 1000; ++request) {
        TestCompletion completion;
        ASSERT_TRUE(
            caller.send(&transport, connection.get(), request, pool.alloc(1), nullptr, 42, complete, &completion));
        std::atomic<bool> start{false};
        std::thread       close([&] {
            start.wait(false);
            caller.fail_all(connection.get(), RpcError::ConnectionClosed);
        });
        start.store(true);
        start.notify_one();
        auto *response       = new Frame;
        response->request_id = request;
        if (!caller.on_response(request, response)) {
            delete response;
        }
        close.join();
        EXPECT_EQ(completion.calls.load(), 1);
        EXPECT_EQ(caller.pending_count(), 0U);
    }
}

TEST(ClientSubmitOwnershipTest, ResponseAndDisconnectClaimMapCompletionOnce)
{
    check_concurrent_completion(false);
}

TEST(ClientSubmitOwnershipTest, ResponseAndDisconnectClaimSlabCompletionOnce)
{
    check_concurrent_completion(true);
}
