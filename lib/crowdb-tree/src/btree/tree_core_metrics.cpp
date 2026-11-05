// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/allocation_account.h"
#include "btree/tree_core.h"
#include "btree/tree_work_registry.h"
#include "runtime/allocation_memory_resource.h"

namespace crowdb::tree
{
namespace
{
void register_source_metrics(MetricsRegistry &registry, TreeCore &core, const std::string &prefix)
{
    auto source = [&](const char *name, uint64_t SourceMemoryStats::*field) {
        registry.register_callback_gauge_inline(prefix, name,
                                                [target = &core, field] { return target->source_stats().*field; });
    };
    source(".mt.tables.g", &SourceMemoryStats::tables);
    source(".mt.frozen.g", &SourceMemoryStats::frozen_tables);
    source(".mt.writers.g", &SourceMemoryStats::writers);
    source(".mt.freezing.writers.g", &SourceMemoryStats::freezing_writers);
    source(".mt.frozen.retained.bytes.g", &SourceMemoryStats::frozen_retained_bytes);
    source(".mt.records.g", &SourceMemoryStats::records);
    source(".mt.logical.bytes.g", &SourceMemoryStats::logical_bytes);
    auto memory = [&](const char *name, uint64_t VersionMemory::*field) {
        registry.register_callback_gauge_inline(
            prefix, name, [target = &core, field] { return target->source_stats().memory.*field; });
    };
    // Linked-source summaries preserve the existing schema. Unique allocation
    // capacities and old-reader retention belong to the physical owner gauges.
    memory(".mt.history.live.g", &VersionMemory::history_count);
    memory(".mt.history.bytes.g", &VersionMemory::history_bytes);
    memory(".mt.descriptor.bytes.g", &VersionMemory::descriptor_bytes);
    memory(".mt.node.bytes.g", &VersionMemory::node_bytes);
    memory(".mt.payload.bytes.g", &VersionMemory::payload_bytes);
    registry.register_callback_gauge_inline(prefix, ".mt.summary.known.g", [target = &core] {
        return static_cast<uint64_t>(target->source_stats().known);
    });
}
} // namespace

void Crowdbtree::init_core_metrics(const std::string &prefix)
{
    // Registry storage grows with the schema and prefix, independently of the
    // fixed bootstrap reserve. Every real allocation still obeys Tree/host quota.
    std::shared_ptr<detail::AllocationAccount> budget;
    if (!detail::AllocationAccount::create(core_->binding()->account(), detail::BudgetClass::kRouting, &budget).ok()) {
        throw std::bad_alloc();
    }
    auto resource_owner = std::allocate_shared<detail::AllocationMemoryResource>(
        MetadataAllocator<detail::AllocationMemoryResource>(budget), budget);
    auto  registry_owner = create_tree_work_registry(resource_owner, prefix, core_->binding()->work_metrics(), budget);
    auto *registry       = registry_owner.get();
    register_source_metrics(*registry, *core_, prefix);
    auto logical = [this, registry, &prefix](const char *name, uint64_t TreeLogicalStats::*field) {
        registry->register_callback_gauge_inline(prefix, name,
                                                 [this, field] { return core_->capture()->stats.*field; });
    };
    logical(".tree.live.leaf_pages", &TreeLogicalStats::leaf_pages);
    logical(".tree.live.inner_pages", &TreeLogicalStats::inner_pages);
    logical(".tree.live.height", &TreeLogicalStats::height);
    logical(".tree.live.dirty_pages", &TreeLogicalStats::dirty_pages);
    logical(".tree.live.overlay_pages", &TreeLogicalStats::overlay_pages);
    logical(".tree.live.overlay_runs", &TreeLogicalStats::overlay_runs);
    logical(".tree.live.overlay_entries", &TreeLogicalStats::overlay_entries);
    logical(".tree.live.kv_known_count", &TreeLogicalStats::kv_known_count);
    logical(".tree.live.kv_unknown_pages", &TreeLogicalStats::kv_unknown_pages);
    logical(".tree.live.checkpoint_pending_pages", &TreeLogicalStats::checkpoint_pending_pages);
    registry->register_callback_gauge_inline(prefix, ".tree.resident.frames",
                                             [this] { return core_->state_stats().resources.resident_frames; });
    registry->register_callback_gauge_inline(prefix, ".tree.resident.cache_bytes",
                                             [this] { return core_->state_stats().resources.cache_bytes; });
    registry->register_callback_gauge_inline(prefix, ".tree.resident.pinned_bytes",
                                             [this] { return core_->state_stats().resources.cache_pinned_bytes; });
    registry->register_callback_gauge_inline(prefix, ".tree.live.proxy_pages",
                                             [this] { return core_->state_stats().resources.proxy_pages; });
    logical(".tree.leaf.count.g", &TreeLogicalStats::leaf_pages);
    logical(".tree.inner.count.g", &TreeLogicalStats::inner_pages);
    logical(".tree.height.g", &TreeLogicalStats::height);
    logical(".page.dirty.count.g", &TreeLogicalStats::dirty_pages);
    logical(".page.overlay.count.g", &TreeLogicalStats::overlay_pages);
    logical(".overlay.runs.g", &TreeLogicalStats::overlay_runs);
    logical(".overlay.entries.g", &TreeLogicalStats::overlay_entries);
    logical(".page.size.known.bytes.g", &TreeLogicalStats::page_size_known_bytes);
    logical(".page.size.unknown.count.g", &TreeLogicalStats::page_size_unknown_pages);
    logical(".page.size.estimate.bytes.g", &TreeLogicalStats::page_size_estimate_bytes);
    logical(".page.size.max.observed.g", &TreeLogicalStats::page_size_max_observed_bytes);
    logical(".tree.kv.known.count.g", &TreeLogicalStats::kv_known_count);
    logical(".tree.kv.known.bytes.g", &TreeLogicalStats::kv_known_bytes);
    logical(".tree.kv.unknown.page.count.g", &TreeLogicalStats::kv_unknown_pages);
    logical(".candidate.consolidate.pages.g", &TreeLogicalStats::consolidate_candidate_pages);
    logical(".candidate.split.pages.g", &TreeLogicalStats::split_candidate_pages);
    logical(".candidate.split.ready.pages.g", &TreeLogicalStats::split_ready_pages);
    logical(".candidate.merge.pages.g", &TreeLogicalStats::merge_candidate_pages);
    logical(".candidate.merge.pairs.g", &TreeLogicalStats::merge_candidate_pairs);
    logical(".candidate.merge.ready.pairs.g", &TreeLogicalStats::merge_ready_pairs);
    logical(".candidate.writeback.pages.g", &TreeLogicalStats::writeback_candidate_pages);
    logical(".candidate.evict.pages.g", &TreeLogicalStats::evict_candidate_pages);
    logical(".page.checkpoint.pending.count.g", &TreeLogicalStats::checkpoint_pending_pages);
    auto resource = [this, registry, &prefix](const char *name, uint64_t TreeResourceStats::*field) {
        registry->register_callback_gauge_inline(prefix, name,
                                                 [this, field] { return core_->state_stats().resources.*field; });
    };
    resource(".memory.allocated.bytes.g", &TreeResourceStats::allocated_bytes);
    resource(".memory.reserved.bytes.g", &TreeResourceStats::reserved_bytes);
    resource(".memory.payload.bytes.g", &TreeResourceStats::payload_bytes);
    resource(".memory.frame.bytes.g", &TreeResourceStats::frame_bytes);
    resource(".memory.metadata.bytes.g", &TreeResourceStats::metadata_bytes);
    resource(".memory.io.bytes.g", &TreeResourceStats::io_bytes);
    resource(".memory.scratch.bytes.g", &TreeResourceStats::scratch_bytes);
    resource(".memory.retired.bytes.g", &TreeResourceStats::retired_bytes);
    resource(".cache.frames.g", &TreeResourceStats::resident_frames);
    resource(".io.read.inflight.bytes.g", &TreeResourceStats::read_inflight_bytes);
    resource(".io.write.inflight.bytes.g", &TreeResourceStats::write_inflight_bytes);
    resource(".page.resident.frames.g", &TreeResourceStats::resident_frames);
    resource(".cache.bytes.g", &TreeResourceStats::cache_bytes);
    resource(".cache.pinned.bytes.g", &TreeResourceStats::cache_pinned_bytes);
    resource(".page.proxy.count.g", &TreeResourceStats::proxy_pages);
    resource(".read.inflight.bytes.g", &TreeResourceStats::read_inflight_bytes);
    resource(".write.inflight.bytes.g", &TreeResourceStats::write_inflight_bytes);
    resource(".materialize.inflight.bytes.g", &TreeResourceStats::materialize_inflight_bytes);
    resource(".backing.allocated.bytes.g", &TreeResourceStats::allocated_backing_bytes);
    resource(".maintenance.queued.g", &TreeResourceStats::maintenance_queued);
    resource(".maintenance.inflight.g", &TreeResourceStats::maintenance_inflight);
    registry->register_callback_gauge_inline(prefix, ".tree.page.count.g",
                                             [this] { return core_->capture()->stats.page_count(); });
    registry->register_callback_gauge_inline(prefix, ".checkpoint.mapping.pending.g", [this] {
        return static_cast<uint64_t>(core_->capture()->stats.checkpoint_mapping_pending);
    });
    registry->register_callback_gauge_inline(prefix, ".candidate.oldest.age.ns.g",
                                             [this] { return core_->state_stats().progress.candidate_oldest_age_ns; });
    registry->register_callback_gauge_inline(prefix, ".checkpoint.oldest.pending.age.ns.g", [this] {
        return core_->state_stats().progress.checkpoint_oldest_pending_age_ns;
    });
    registry->register_callback_gauge_inline(prefix, ".frontier.published.g",
                                             [this] { return core_->capture()->coverage; });
    registry->register_callback_gauge_inline(prefix, ".frontier.durable.g",
                                             [this] { return durable_snapshot_last_applied_slot(); });
    registry->register_callback_gauge_inline(prefix, ".flush.repair.pending.g", [this] {
        return static_cast<uint64_t>(core_->capture()->pending_handoff.has_value());
    });
    registry->register_callback_gauge_inline(prefix, ".checkpoint.inflight.g", [this] {
        return static_cast<uint64_t>(core_->capture()->checkpoint_inflight);
    });
    registry->register_callback_gauge_inline(prefix, ".maintenance.oldest.age.ns.g", [this] {
        return core_->state_stats().progress.maintenance_oldest_age_ns;
    });
    auto dispatch = [this, registry, &prefix](const char *name, uint64_t FlushDispatchSnapshot::*field) {
        registry->register_callback_gauge_inline(
            prefix, name, [this, field] { return core_->binding()->work_metrics()->dispatch.snapshot().*field; });
    };
    dispatch(".flush.dispatch.inline.probes.total", &FlushDispatchSnapshot::inline_probes);
    dispatch(".flush.dispatch.inline.ranges.total", &FlushDispatchSnapshot::inline_ranges);
    dispatch(".flush.dispatch.queued.steps.total", &FlushDispatchSnapshot::queued_steps);
    dispatch(".flush.dispatch.runs.total", &FlushDispatchSnapshot::prepared_runs);
    dispatch(".flush.dispatch.entries.total", &FlushDispatchSnapshot::prepared_entries);
    dispatch(".flush.dispatch.bytes.total", &FlushDispatchSnapshot::prepared_bytes);
    dispatch(".flush.dispatch.cow.bytes.total", &FlushDispatchSnapshot::cow_bytes);
    dispatch(".flush.dispatch.active.steps.g", &FlushDispatchSnapshot::active_steps);
    dispatch(".flush.dispatch.peak.steps.g", &FlushDispatchSnapshot::peak_steps);
    metrics_registry_ = std::move(registry_owner);
}
} // namespace crowdb::tree
