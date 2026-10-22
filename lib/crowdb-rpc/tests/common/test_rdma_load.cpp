// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "test_rdma_load.h"

#ifdef CROWDB_RPC_HAVE_RDMA

#    include "crowdb-rpc/server/message.h"

#    include <algorithm>
#    include <cstdio>

namespace crowdb::rpc
{

TestRdmaLoad::TestRdmaLoad(uint32_t count, uint32_t in_flight, uint32_t seconds)
    : producer_count(count),
      in_flight_per_producer(in_flight),
      duration_seconds(seconds),
      id_stride(std::bit_ceil(static_cast<uint64_t>(count) * in_flight))
{
    for (uint32_t index = 0; index < count; ++index)
        producers.push_back(std::make_unique<TestRdmaProducer>(*this, index));
}

TestRdmaLoad::~TestRdmaLoad() = default;

void TestRdmaLoad::fail(const char *reason, uint64_t request_id, crowdb_rpc_status status)
{
    const auto previous = errors.fetch_add(1, std::memory_order_relaxed);
    if (previous < 8)
        std::fprintf(stderr, "RDMA timed load failure: %s request_id=%llu status=%d\n", reason,
                     static_cast<unsigned long long>(request_id), status);
}

void TestRdmaLoad::complete(TestRdmaLoadSlot &slot, uint64_t request_id, crowdb_rpc_buffer_t data,
                            crowdb_rpc_status status)
{
    uint64_t expected = request_id;
    if (!slot.expected_id.compare_exchange_strong(expected, UINT64_MAX, std::memory_order_acq_rel)) {
        fail("duplicate or mismatched completion", request_id, status);
        return;
    }
    const uint64_t now    = test_rdma_clock_ns();
    const auto    *bytes  = data == nullptr ? nullptr : crowdb_rpc_buffer_data(data);
    const uint32_t length = data == nullptr ? 0 : crowdb_rpc_buffer_len(data);
    if (status != CROWDB_RPC_OK)
        fail("RPC completion failed", request_id, status);
    else if (length != PAYLOAD_BYTES || bytes == nullptr ||
             !std::all_of(bytes, bytes + length,
                          [request_id](uint8_t byte) { return byte == static_cast<uint8_t>(request_id); }))
        fail("response payload mismatch", request_id, status);
    else if (slot.measured) {
        if (now < measure_end_ns.load(std::memory_order_relaxed)) {
            slot.producer->latency.observe(now - slot.started_ns);
            slot.producer->window_completed.fetch_add(1, std::memory_order_relaxed);
        }
        else
            slot.producer->drain_completed.fetch_add(1, std::memory_order_relaxed);
    }
    slot.producer->total_completed.fetch_add(1, std::memory_order_release);
}

static void test_rdma_load_complete(uint64_t request_id, crowdb_rpc_buffer_t control, crowdb_rpc_buffer_t data,
                                    crowdb_rpc_status status, void *opaque)
{
    auto &slot = *static_cast<TestRdmaLoadSlot *>(opaque);
    slot.producer->load.complete(slot, request_id, data, status);
    if (control != nullptr)
        crowdb_rpc_buffer_release(control);
    if (data != nullptr)
        crowdb_rpc_buffer_release(data);
    if (slot.expected_id.load(std::memory_order_relaxed) == UINT64_MAX)
        slot.expected_id.store(0, std::memory_order_release);
}

TestRdmaProducer::TestRdmaProducer(TestRdmaLoad &owner, uint32_t producer_index)
    : load(owner),
      index(producer_index),
      slots(std::make_unique<TestRdmaLoadSlot[]>(owner.in_flight_per_producer))
{
    for (uint32_t entry = 0; entry < load.in_flight_per_producer; ++entry)
        slots[entry].producer = this;
}

void TestRdmaProducer::phase(RpcClient &client, SocketTransport &transport, Connection *connection,
                             uint64_t deadline_ns, bool measured)
{
    while (test_rdma_clock_ns() < deadline_ns && load.errors.load(std::memory_order_relaxed) == 0) {
        bool progress = false;
        for (uint32_t entry = 0; entry < load.in_flight_per_producer; ++entry) {
            auto &slot = slots[entry];
            if (slot.expected_id.load(std::memory_order_acquire) != 0)
                continue;
            const uint64_t now = test_rdma_clock_ns();
            if (now >= deadline_ns)
                break;
            const uint64_t id      = ++slot.generation * load.id_stride + index * load.in_flight_per_producer + entry;
            Buffer        *control = build_ping_request(transport.pool(), id, 0);
            Buffer        *data    = transport.pool()->alloc(TestRdmaLoad::PAYLOAD_BYTES);
            if (control == nullptr || data == nullptr) {
                if (control != nullptr)
                    control->release();
                if (data != nullptr)
                    data->release();
                load.fail("request buffer admission failed", id, CROWDB_RPC_ERR_SEND_QUEUE);
                return;
            }
            std::array<uint8_t, TestRdmaLoad::PAYLOAD_BYTES> payload{};
            payload.fill(static_cast<uint8_t>(id));
            data->write(payload.data(), payload.size());
            slot.started_ns = now;
            slot.measured   = measured;
            slot.expected_id.store(id, std::memory_order_release);
            if (!client.send(&transport, connection, id, control, data, TestRdmaLoad::MESSAGE_TYPE,
                             test_rdma_load_complete, &slot)) {
                slot.expected_id.store(0, std::memory_order_release);
                load.fail("request submission rejected", id, CROWDB_RPC_ERR_SEND_QUEUE);
                return;
            }
            ++total_submitted;
            if (measured)
                ++measured_submitted;
            progress = true;
        }
        if (!progress)
            std::this_thread::yield();
    }
}

void TestRdmaProducer::drain()
{
    const uint64_t deadline = test_rdma_clock_ns() + 5'000'000'000ULL;
    while (total_completed.load(std::memory_order_acquire) < total_submitted &&
           load.errors.load(std::memory_order_relaxed) == 0) {
        if (test_rdma_clock_ns() >= deadline) {
            load.fail("in-flight requests did not drain", 0, CROWDB_RPC_ERR_TIMEOUT);
            break;
        }
        std::this_thread::yield();
    }
}

void TestRdmaProducer::run(RpcClient &client, SocketTransport &transport, Connection *connection)
{
    load.ready_producers.fetch_add(1, std::memory_order_release);
    while (load.warmup_end_ns.load(std::memory_order_acquire) == 0)
        std::this_thread::yield();
    phase(client, transport, connection, load.warmup_end_ns.load(std::memory_order_relaxed), false);
    drain();
    load.warmed_producers.fetch_add(1, std::memory_order_release);
    while (load.measure_end_ns.load(std::memory_order_acquire) == 0)
        std::this_thread::yield();
    phase(client, transport, connection, load.measure_end_ns.load(std::memory_order_relaxed), true);
    drain();
}

} // namespace crowdb::rpc

#endif
