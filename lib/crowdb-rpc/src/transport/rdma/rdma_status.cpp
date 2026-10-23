// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "../../status_json.h"
#include "rdma_cq.h"

#ifdef CROWDB_RPC_HAVE_RDMA
namespace crowdb::rpc
{
std::string RdmaTransport::status_json() const
{
    const auto &counters   = stats();
    const auto  capability = local_capability();
    StatusJson  status;
    status.boolean("ready", ready());
    status.boolean("listen_active", listen_active_.load(std::memory_order_acquire));
    status.string("device", capability.device);
    status.string("gid", capability.gid);
    status.string("source_address", config_.source_address);
    status.string("cm_address", capability.cm_address);
    status.number("cm_port", capability.port);
    status.number("generation", capability.generation);
    status.number("protocol_version", capability.protocol_version);
    status.boolean("aggregation_enabled", config_.aggregation_enabled);
    status.string("wire_layout", config_.wire_layout == RdmaWireLayout::Contiguous ? "contiguous" : "control_data");
    status.number("data_slot_capacity", config_.data_slot_capacity());
    status.number("control_slot_capacity",
                  config_.wire_layout == RdmaWireLayout::ControlData ? RDMA_CONTROL_CAPACITY : 0);
#    define CONFIG_FIELD(name) status.number(#name, config_.name)
    CONFIG_FIELD(port);
    CONFIG_FIELD(cq_count);
    CONFIG_FIELD(cq_depth);
    CONFIG_FIELD(sq_depth);
    CONFIG_FIELD(send_batch_size);
    CONFIG_FIELD(receive_batch_size);
    CONFIG_FIELD(send_window_size);
    CONFIG_FIELD(send_window_bytes);
    status.number("sends_per_round", CompletionQueue::SENDS_PER_ROUND);
    status.number("prepare_frames_per_round", CompletionQueue::PREPARE_FRAMES_PER_ROUND);
    status.number("prepare_bytes_per_round", CompletionQueue::PREPARE_BYTES_PER_ROUND);
    status.number("receive_frames_per_round", CompletionQueue::RECEIVE_FRAMES_PER_ROUND);
    status.number("receive_bytes_per_round", CompletionQueue::RECEIVE_BYTES_PER_ROUND);
    CONFIG_FIELD(rq_depth);
    CONFIG_FIELD(posted_receive_target);
    CONFIG_FIELD(wire_slot_capacity);
    CONFIG_FIELD(send_arena_bytes);
    CONFIG_FIELD(receive_arena_bytes);
    CONFIG_FIELD(poll_batch);
    CONFIG_FIELD(verbs_poll_batch);
    CONFIG_FIELD(poll_empty_budget);
    CONFIG_FIELD(send_queue_capacity);
    CONFIG_FIELD(aggregation_max_bytes);
    CONFIG_FIELD(aggregation_max_frames);
    CONFIG_FIELD(max_control_message_bytes);
    CONFIG_FIELD(control_send_budget_bytes);
    CONFIG_FIELD(control_receive_budget_bytes);
    CONFIG_FIELD(control_assembly_timeout_ms);
#    undef CONFIG_FIELD
#    define COUNTER_FIELD(name) status.number(#name, counters.name.load(std::memory_order_relaxed))
    COUNTER_FIELD(sends);
    COUNTER_FIELD(receives);
    COUNTER_FIELD(rpc_frames_sent);
    COUNTER_FIELD(send_post_calls);
    COUNTER_FIELD(send_completions);
    COUNTER_FIELD(rpc_frames_received);
    COUNTER_FIELD(wire_bytes_sent);
    COUNTER_FIELD(wire_bytes_received);
    COUNTER_FIELD(cq_errors);
    COUNTER_FIELD(protocol_errors);
    COUNTER_FIELD(send_errors);
    COUNTER_FIELD(receive_post_errors);
    COUNTER_FIELD(receive_post_calls);
    COUNTER_FIELD(direct_sends);
    COUNTER_FIELD(direct_payload_bytes);
    COUNTER_FIELD(copied_payload_bytes);
    COUNTER_FIELD(copied_aggregate_payload_bytes);
    COUNTER_FIELD(copied_staging_payload_bytes);
    COUNTER_FIELD(copied_framing_control_bytes);
    COUNTER_FIELD(submit_rejects);
    COUNTER_FIELD(send_queue_high_water);
    COUNTER_FIELD(fallback_count);
    COUNTER_FIELD(posted_receives);
    COUNTER_FIELD(cq_polls);
    COUNTER_FIELD(cq_empty_polls);
    COUNTER_FIELD(cm_disconnects);
    COUNTER_FIELD(flush_errors);
    COUNTER_FIELD(shutdown_flushes);
    COUNTER_FIELD(local_protection_errors);
    COUNTER_FIELD(retry_exceeded_errors);
    COUNTER_FIELD(remote_access_errors);
    COUNTER_FIELD(poller_count);
    COUNTER_FIELD(cq_events);
    COUNTER_FIELD(cq_wakeups);
    COUNTER_FIELD(pending_deferred_receives);
    COUNTER_FIELD(registered_memory_budget_bytes);
    COUNTER_FIELD(registered_bytes);
    COUNTER_FIELD(registered_bytes_high_water);
    COUNTER_FIELD(registration_rejects);
    COUNTER_FIELD(registration_failures);
    COUNTER_FIELD(cm_attempts);
    COUNTER_FIELD(preflight_attempts);
    COUNTER_FIELD(preflight_failures);
    COUNTER_FIELD(control_fragments_sent);
    COUNTER_FIELD(control_fragments_received);
    COUNTER_FIELD(copied_control_send_bytes);
    COUNTER_FIELD(copied_control_receive_bytes);
    COUNTER_FIELD(control_stream_timeouts);
    COUNTER_FIELD(control_send_bytes);
    COUNTER_FIELD(control_send_bytes_high_water);
    COUNTER_FIELD(control_send_stalls);
    COUNTER_FIELD(control_receive_bytes);
    COUNTER_FIELD(control_receive_bytes_high_water);
    COUNTER_FIELD(control_receive_stalls);
#    undef COUNTER_FIELD
    status.null("quarantined_bytes");
    std::string depths = "[";
    for (const auto &queue : completion_queues_) {
        if (depths.size() > 1)
            depths += ',';
        depths += std::to_string(queue->effective_depth.load(std::memory_order_acquire));
    }
    status.raw("effective_cq_depths", depths + ']');
    return status.finish();
}
} // namespace crowdb::rpc
#endif
