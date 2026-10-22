// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#pragma once

#include "crowdb-rpc/client/client.h"
#include "crowdb-rpc/transport/socket_transport.h"

#ifdef CROWDB_RPC_HAVE_RDMA

#    include <array>
#    include <bit>
#    include <chrono>
#    include <memory>
#    include <thread>
#    include <vector>

namespace crowdb::rpc
{

inline uint64_t test_rdma_clock_ns()
{
    return static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now().time_since_epoch())
            .count());
}

struct TestRdmaLatency
{
    static constexpr size_t                    SUB_BUCKETS = 128;
    static constexpr size_t                    BUCKETS     = 64 * SUB_BUCKETS;
    std::array<std::atomic<uint64_t>, BUCKETS> buckets{};
    std::atomic<uint64_t>                      sum_ns{0};
    std::atomic<uint64_t>                      max_ns{0};

    void            observe(uint64_t ns);
    static uint64_t percentile(const std::array<uint64_t, BUCKETS> &buckets, uint64_t count, uint32_t percent);
};

struct TestRdmaProducer;

struct TestRdmaLoadSlot
{
    std::atomic<uint64_t> expected_id{0};
    uint64_t              started_ns = 0;
    uint64_t              generation = 0;
    bool                  measured   = false;
    TestRdmaProducer     *producer   = nullptr;
};

struct TestRdmaLoad
{
    static constexpr uint16_t                      MESSAGE_TYPE  = 239;
    static constexpr uint16_t                      FINISHED_TYPE = 240;
    static constexpr uint32_t                      PAYLOAD_BYTES = 128;
    uint32_t                                       producer_count;
    uint32_t                                       in_flight_per_producer;
    uint32_t                                       duration_seconds;
    uint64_t                                       id_stride;
    std::atomic<uint64_t>                          warmup_end_ns{0};
    std::atomic<uint64_t>                          measure_end_ns{0};
    std::atomic<uint32_t>                          ready_producers{0};
    std::atomic<uint32_t>                          warmed_producers{0};
    std::atomic<uint64_t>                          errors{0};
    std::vector<std::unique_ptr<TestRdmaProducer>> producers;

    TestRdmaLoad(uint32_t count, uint32_t in_flight, uint32_t seconds);
    ~TestRdmaLoad();
    void complete(TestRdmaLoadSlot &slot, uint64_t request_id, crowdb_rpc_buffer_t data, crowdb_rpc_status status);
    void fail(const char *reason, uint64_t request_id, crowdb_rpc_status status);
};

struct TestRdmaProducer
{
    TestRdmaLoad                       &load;
    uint32_t                            index;
    std::unique_ptr<TestRdmaLoadSlot[]> slots;
    TestRdmaLatency                     latency;
    uint64_t                            total_submitted    = 0;
    uint64_t                            measured_submitted = 0;
    std::atomic<uint64_t>               total_completed{0};
    std::atomic<uint64_t>               window_completed{0};
    std::atomic<uint64_t>               drain_completed{0};

    TestRdmaProducer(TestRdmaLoad &owner, uint32_t producer_index);
    void run(RpcClient &client, SocketTransport &transport, Connection *connection);
    void phase(RpcClient &client, SocketTransport &transport, Connection *connection, uint64_t deadline_ns,
               bool measured);
    void drain();
};

} // namespace crowdb::rpc

#endif
