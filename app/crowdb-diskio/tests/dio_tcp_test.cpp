// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "common/test_dio_hardware.h"

#ifdef CROWDB_HAVE_LIBURING
namespace crowdb::diskio
{
class TestDioTcp : public TestDioHardware
{
  protected:
    void SetUp() override
    {
        use_rdma = false;
        TestDioHardware::SetUp();
    }
};

TEST_F(TestDioTcp, DirectBlockCorpusMatchesRdmaBytesWithExplicitTcp)
{
    for (uint32_t index = 0; index < 3; ++index) {
        auto written = call(index + 1, index, true);
        ASSERT_NE(written, nullptr);
        ASSERT_TRUE(wait_for([&] { return written->callbacks.load(std::memory_order_acquire) == 1; }));
        ASSERT_EQ(written->status, CROWDB_RPC_OK);
        ASSERT_NE(written->control, nullptr);
        const auto *write_response =
            flatbuffers::GetRoot<proto::FBDiskWriteResponse>(crowdb_rpc_buffer_data(written->control));
        ASSERT_EQ(write_response->ret_code(), proto::FBDiskIoRetCode_Success);
        auto read = call(index + 100, index, false);
        ASSERT_NE(read, nullptr);
        ASSERT_TRUE(wait_for([&] { return read->callbacks.load(std::memory_order_acquire) == 1; }));
        ASSERT_EQ(read->status, CROWDB_RPC_OK);
        ASSERT_NE(read->control, nullptr);
        const auto *read_response =
            flatbuffers::GetRoot<proto::FBDiskReadResponse>(crowdb_rpc_buffer_data(read->control));
        ASSERT_EQ(read_response->ret_code(), proto::FBDiskIoRetCode_Success);
        ASSERT_NE(read->data, nullptr);
        ASSERT_EQ(crowdb_rpc_buffer_len(read->data), BLOCK);
        std::vector<uint8_t> expected(BLOCK);
        for (uint32_t byte = 0; byte < BLOCK; ++byte)
            expected[byte] = static_cast<uint8_t>(byte * 131U + index);
        ASSERT_EQ(std::memcmp(crowdb_rpc_buffer_data(read->data), expected.data(), BLOCK), 0);
    }
    EXPECT_NE(client->transport()->status_json().find("\"rdma\":null"), std::string::npos);
    EXPECT_NE(server->transport()->status_json().find("\"rdma\":null"), std::string::npos);
    RecordProperty("client_transport", client->transport()->status_json());
    RecordProperty("server_transport", server->transport()->status_json());
    RecordProperty("topology", "single_process_tcp_loopback");
}
} // namespace crowdb::diskio
#endif
