// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "common/test_dio_hardware.h"

#include <folly/json.h>

#if defined(CROWDB_RPC_HAVE_RDMA) && defined(CROWDB_HAVE_LIBURING)
namespace crowdb::diskio
{
namespace
{
folly::dynamic test_status(const std::string &snapshot)
{
    folly::json::serialization_opts options;
    // Capability generations span all uint64_t values. Preserve decimal
    // counters exactly instead of forcing the whole snapshot through int64_t.
    options.parse_numbers_as_strings = true;
    return folly::parseJson(snapshot, options)["rdma"];
}
} // namespace

class TestDioFailure : public TestDioHardware
{
  protected:
    void SetUp() override
    {
        allow_tcp_fallback = true;
        TestDioHardware::SetUp();
    }
};

TEST_F(TestDioHardware, DirectBlockRoundTripKeepsRegisteredPointersAndBoundsHeldResponses)
{
    for (uint32_t index = 0; index < 3; ++index) {
        auto written = call(index + 1, index, true);
        ASSERT_NE(written, nullptr);
        ASSERT_TRUE(wait_for([&] { return written->callbacks.load(std::memory_order_acquire) == 1; }));
        ASSERT_EQ(written->status, CROWDB_RPC_OK);
        ASSERT_NE(written->control, nullptr);
        const auto *response =
            flatbuffers::GetRoot<proto::FBDiskWriteResponse>(crowdb_rpc_buffer_data(written->control));
        ASSERT_EQ(response->ret_code(), proto::FBDiskIoRetCode_Success);
        EXPECT_EQ(engine->write_pointer.load(), receive_pointer.load());
        EXPECT_EQ(engine->write_pointer.load() % 4096, 0U);
    }
    const auto                                 registered_before = server->transport()->status_json();
    std::vector<std::shared_ptr<TestDiskCall>> reads;
    for (uint32_t index = 0; index < 8; ++index) {
        auto read = call(index + 100, index % 3, false);
        ASSERT_NE(read, nullptr);
        reads.push_back(std::move(read));
    }
    ASSERT_TRUE(wait_for([&] { return engine->reads.load() == 8; }));
    ASSERT_TRUE(wait_for([&] {
        return std::count_if(reads.begin(), reads.end(), [](const auto &read) {
                   return read->callbacks.load(std::memory_order_acquire) == 1;
               }) == 4;
    }));
    EXPECT_EQ(engine->read_pointer.load() % 4096, 0U);
    EXPECT_EQ(engine->read_pointer.load(), read_receive_pointer.load());
    const auto before = test_status(registered_before);
    const auto held   = test_status(server->transport()->status_json());
    EXPECT_EQ(held["registered_bytes"], before["registered_bytes"]);
    EXPECT_EQ(held["registered_bytes_high_water"], before["registered_bytes_high_water"]);
    // Four caller-held response leases consume all client RQ slots. The next
    // four replies retain the server's own receive data regions in SEND.
    std::vector<bool> released(reads.size(), false);
    size_t            remaining = reads.size();
    while (remaining != 0) {
        size_t index = reads.size();
        ASSERT_TRUE(wait_for([&] {
            for (size_t candidate = 0; candidate < reads.size(); ++candidate) {
                if (!released[candidate] && reads[candidate]->callbacks.load(std::memory_order_acquire) == 1) {
                    index = candidate;
                    return true;
                }
            }
            return false;
        }));
        auto &read = reads[index];
        ASSERT_EQ(read->status, CROWDB_RPC_OK);
        ASSERT_NE(read->control, nullptr);
        const auto *response = flatbuffers::GetRoot<proto::FBDiskReadResponse>(crowdb_rpc_buffer_data(read->control));
        ASSERT_EQ(response->ret_code(), proto::FBDiskIoRetCode_Success);
        ASSERT_NE(read->data, nullptr);
        ASSERT_EQ(crowdb_rpc_buffer_len(read->data), BLOCK);
        const auto          *bytes = crowdb_rpc_buffer_data(read->data);
        std::vector<uint8_t> expected(BLOCK);
        for (uint32_t byte = 0; byte < BLOCK; ++byte)
            expected[byte] = static_cast<uint8_t>(byte * 131U + index % 3);
        ASSERT_EQ(std::memcmp(bytes, expected.data(), BLOCK), 0);
        read->release_data();
        released[index] = true;
        --remaining;
    }
    const auto client_status = client->transport()->status_json();
    const auto server_status = server->transport()->status_json();
    const auto after         = test_status(server_status);
    EXPECT_EQ(after["registered_bytes"], before["registered_bytes"]);
    EXPECT_EQ(after["registered_bytes_high_water"], before["registered_bytes_high_water"]);
    for (const auto &status : {client_status, server_status}) {
        EXPECT_NE(status.find("\"copied_payload_bytes\":0"), std::string::npos);
        EXPECT_NE(status.find("\"fallback_count\":0"), std::string::npos);
        EXPECT_NE(status.find("\"receive_post_errors\":0"), std::string::npos);
        EXPECT_NE(status.find("\"send_errors\":0"), std::string::npos);
        EXPECT_NE(status.find("\"protocol_errors\":0"), std::string::npos);
        EXPECT_NE(status.find("\"cq_errors\":0"), std::string::npos);
    }
    EXPECT_NE(client_status.find("\"direct_payload_bytes\":3145728"), std::string::npos);
    EXPECT_NE(server_status.find("\"direct_payload_bytes\":8388608"), std::string::npos);
    RecordProperty("client_transport", client_status);
    RecordProperty("server_transport_before_reads", registered_before);
    RecordProperty("server_transport", server_status);
    RecordProperty("topology", "single_process_hca_local_loop");
}

TEST_F(TestDioFailure, PeerCloseAndServerDestructionRetainPendingDiskDataWithoutFallback)
{
    engine->hold_writes.store(true, std::memory_order_release);
    auto written = call(41, 0, true);
    ASSERT_NE(written, nullptr);
    ASSERT_TRUE(wait_for([&] { return engine->parked.load(std::memory_order_acquire) != nullptr; }));
    peer->close();
    ASSERT_TRUE(wait_for([&] { return written->callbacks.load(std::memory_order_acquire) == 1; }));
    EXPECT_EQ(written->status, CROWDB_RPC_ERR_CONN_CLOSED);
    std::weak_ptr<crowdb::rpc::SocketTransport> retained = server->shared_transport();
    server->stop();
    server.reset();
    ASSERT_FALSE(retained.expired());
    const auto *pending = engine->parked.load(std::memory_order_acquire);
    ASSERT_NE(pending, nullptr);
    EXPECT_EQ(reinterpret_cast<uintptr_t>(pending->data), receive_pointer.load());
    EXPECT_EQ(pending->data[0], 0U);
    EXPECT_EQ(pending->data[BLOCK - 1], static_cast<uint8_t>((BLOCK - 1) * 131U));
    engine->resume_write();
    ASSERT_TRUE(wait_for([&] { return engine->completed_writes->load(std::memory_order_acquire) == 1; }));
    EXPECT_EQ(written->callbacks.load(), 1U);
    EXPECT_NE(client->transport()->status_json().find("\"fallback_count\":0"), std::string::npos);
    RecordProperty("client_transport", client->transport()->status_json());
    RecordProperty("expected_connection_errors", 1);
    const auto fd = ::open(path.c_str(), O_RDONLY);
    ASSERT_GE(fd, 0);
    std::vector<uint8_t> stored(BLOCK);
    const auto           bytes = ::pread(fd, stored.data(), stored.size(), 0);
    ::close(fd);
    ASSERT_EQ(bytes, BLOCK);
    for (uint32_t index = 0; index < BLOCK; ++index)
        ASSERT_EQ(stored[index], static_cast<uint8_t>(index * 131U));
    service.reset();
    EXPECT_TRUE(retained.expired());
}
} // namespace crowdb::diskio
#endif
