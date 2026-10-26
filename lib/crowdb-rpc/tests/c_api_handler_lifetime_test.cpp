// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "crowdb-rpc/c_api_internal.h"

#include <gtest/gtest.h>

using namespace crowdb::rpc;

TEST(CApiHandlerLifetimeTest, ControlOnlyRequestPinsConnectionUntilFinalFrameRelease)
{
    SystemBufferPool          pool;
    auto                      connection = std::make_shared<Connection>(1, "handler", &pool);
    std::weak_ptr<Connection> lifetime   = connection;
    uint32_t                  closes     = 0;
    connection->set_on_close([&](Connection *) { ++closes; });

    struct TestRequest
    {
        void *frame      = nullptr;
        void *connection = nullptr;
    } held;

    const auto callback = [](uint64_t, uint64_t, uint16_t, const uint8_t *, uint32_t, const uint8_t *, uint32_t,
                             void *conn, void *frame, void *context) {
        auto &request      = *static_cast<TestRequest *>(context);
        request.frame      = frame;
        request.connection = conn;
    };
    invoke_c_handler(callback, &held, new Frame, connection.get());
    ASSERT_NE(held.frame, nullptr);
    EXPECT_EQ(held.connection, connection.get());
    connection->close();
    connection.reset();
    EXPECT_FALSE(lifetime.expired());
    crowdb_rpc_frame_close_connection(held.frame);
    EXPECT_EQ(closes, 1U);
    crowdb_rpc_frame_release(held.frame);
    EXPECT_TRUE(lifetime.expired());
}
