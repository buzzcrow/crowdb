// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "../status_json.h"

#include <sys/resource.h>
#include <unistd.h>
#ifdef __linux__
#    include <sched.h>
#endif
#include "crowdb-rpc/transport/socket_transport.h"

namespace crowdb::rpc
{
std::string Connection::status_json() const
{
    StatusJson status;
    status.number("schema_version", 1);
    status.string("path", path() == ConnectionPath::Rdma ? "rdma" : "tcp");
    status.boolean("open", is_open());
    status.raw("info", transport_info_json_);
    return status.finish();
}

std::string SocketTransport::status_json() const
{
    StatusJson status;
    status.number("schema_version", 1);
    status.number("process_id", static_cast<uint64_t>(::getpid()));
    status.number("io_engines", engines_.size());
    status.number("io_workers", workers_.size());
    status.number("connection_count", connection_count());
    status.boolean("tcp_nodelay", tcp_nodelay_);
    status.boolean("tcp_quickack", quickack_);
    status.boolean("event_write", event_write_);
    status.boolean("allow_tcp_fallback", allow_tcp_fallback_);
    status.number("send_queue_capacity", send_queue_capacity_);
    status.number("send_queue_rejects", stats_.send_queue_rejects.load(std::memory_order_relaxed));
    status.number("writev_calls", stats_.writev_calls.load(std::memory_order_relaxed));
    status.number("writev_progress_calls", stats_.writev_progress_calls.load(std::memory_order_relaxed));
    status.number("wire_bytes_sent", stats_.wire_bytes_sent.load(std::memory_order_relaxed));
    status.number("rpc_frames_sent", stats_.rpc_frames_sent.load(std::memory_order_relaxed));
    if (const auto *system_pool = dynamic_cast<const SystemBufferPool *>(pool_); system_pool != nullptr) {
        status.number("application_buffer_capacity", system_pool->max_buffers());
        status.number("application_buffers_in_use", system_pool->outstanding());
    }
    else {
        status.null("application_buffer_capacity");
        status.null("application_buffers_in_use");
    }
    rusage usage{};
    if (::getrusage(RUSAGE_SELF, &usage) == 0) {
        const auto nanos = [](const timeval &time) {
            return static_cast<uint64_t>(time.tv_sec) * 1'000'000'000ULL + static_cast<uint64_t>(time.tv_usec) * 1000;
        };
        status.number("cpu_user_ns", nanos(usage.ru_utime));
        status.number("cpu_system_ns", nanos(usage.ru_stime));
        status.number("voluntary_context_switches", usage.ru_nvcsw);
        status.number("involuntary_context_switches", usage.ru_nivcsw);
    }
    else {
        status.null("cpu_user_ns");
        status.null("cpu_system_ns");
        status.null("voluntary_context_switches");
        status.null("involuntary_context_switches");
    }
    status.null("cpu_migrations");
#ifdef __linux__
    cpu_set_t affinity;
    CPU_ZERO(&affinity);
    if (::sched_getaffinity(0, sizeof(affinity), &affinity) == 0) {
        std::string cpus;
        for (int cpu = 0; cpu < CPU_SETSIZE; ++cpu) {
            if (CPU_ISSET(cpu, &affinity)) {
                if (!cpus.empty())
                    cpus += ',';
                cpus += std::to_string(cpu);
            }
        }
        status.string("cpu_affinity", cpus);
    }
    else {
        status.null("cpu_affinity");
    }
#else
    status.null("cpu_affinity");
#endif
#ifdef CROWDB_RPC_HAVE_RDMA
    status.boolean("rdma_compiled", true);
    if (rdma_transport_ != nullptr)
        status.raw("rdma", rdma_transport_->status_json());
    else
        status.null("rdma");
#else
    status.boolean("rdma_compiled", false);
    status.null("rdma");
#endif
    return status.finish();
}
} // namespace crowdb::rpc
