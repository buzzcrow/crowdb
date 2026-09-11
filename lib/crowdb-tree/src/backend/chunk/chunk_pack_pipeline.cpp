// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "chunk_pack_pipeline.h"

#include "c_api_internal.h"
#include "chunk_page_store.h"
#include "crowdb-common/crc32c.h"
#include "stdexec_adapter.h"

#include <algorithm>
#include <array>
#include <chrono>
#include <limits>
#include <utility>

namespace crowdb::tree::detail
{

uint64_t monotonic_millis()
{
    return std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now().time_since_epoch())
        .count();
}

uint64_t monotonic_nanos()
{
    return std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now().time_since_epoch())
        .count();
}

class ChunkPackPipelineImpl;

struct MirrorWriteSource
{
    std::shared_ptr<ChunkTransport> transport;
    ChunkId                         chunk_id;
    uint32_t                        mirror_index = 0;
    uint64_t                        offset       = 0;
    const std::vector<uint8_t>     *bytes        = nullptr;
    ChunkCancellation               cancellation;
    uint32_t                        retry_limit       = 0;
    const std::atomic<uint8_t>     *failure_mask      = nullptr;
    std::atomic<uint64_t>          *attempts          = nullptr;
    std::atomic<uint64_t>          *failures          = nullptr;
    std::atomic<bool>              *stop_requested    = nullptr;
    std::atomic<uint64_t>          *diskio_operations = nullptr;
    std::atomic<uint64_t>          *diskio_latency_ns = nullptr;
    CallbackComplete                complete          = nullptr;
    void                           *operation         = nullptr;
    uint32_t                        attempt           = 0;
    uint64_t                        started_at_ns     = 0;

    static void submit(void *context, CallbackComplete complete_fn, void *operation_context)
    {
        auto *self      = static_cast<MirrorWriteSource *>(context);
        self->complete  = complete_fn;
        self->operation = operation_context;
        self->attempt   = 0;
        self->submit_attempt();
    }

    static void transport_complete(void *context, Status status)
    {
        auto *self = static_cast<MirrorWriteSource *>(context);
        self->diskio_operations->fetch_add(1, std::memory_order_relaxed);
        self->diskio_latency_ns->fetch_add(monotonic_nanos() - self->started_at_ns, std::memory_order_relaxed);
        if (self->stop_requested_now()) {
            self->stop_requested->store(true, std::memory_order_release);
            self->complete(self->operation, CallbackSignal::kStopped,
                           Status::unavailable("chunk mirror write cancelled"));
            return;
        }
        if (status.ok()) {
            self->complete(self->operation, CallbackSignal::kValue, Status::Ok());
            return;
        }
        self->failures->fetch_add(1, std::memory_order_relaxed);
        if (!self->stop_requested->load(std::memory_order_acquire) && self->attempt++ < self->retry_limit) {
            self->submit_attempt();
            return;
        }
        self->stop_requested->store(true, std::memory_order_release);
        self->complete(self->operation, CallbackSignal::kError,
                       Status::unavailable("chunk mirror write unavailable after bounded retries"));
    }

    static bool transport_stop_requested(void *context)
    {
        return static_cast<MirrorWriteSource *>(context)->stop_requested_now();
    }

    [[nodiscard]] bool stop_requested_now() const
    {
        return cancellation.cancelled() || stop_requested->load(std::memory_order_acquire);
    }

    void submit_attempt()
    {
        if (cancellation.cancelled() || stop_requested->load(std::memory_order_acquire)) {
            stop_requested->store(true, std::memory_order_release);
            complete(operation, CallbackSignal::kStopped, Status::unavailable("chunk mirror write cancelled"));
            return;
        }
        attempts->fetch_add(1, std::memory_order_relaxed);
        started_at_ns = monotonic_nanos();
        if ((failure_mask->load(std::memory_order_acquire) & (1U << mirror_index)) != 0) {
            transport_complete(this, Status::unavailable("injected chunk mirror write failure"));
            return;
        }
        transport->submit_write_mirror(chunk_id, mirror_index, offset, bytes->data(), bytes->size(),
                                       {.complete_fn       = &MirrorWriteSource::transport_complete,
                                        .stop_requested_fn = &MirrorWriteSource::transport_stop_requested,
                                        .context           = this});
    }
};

struct PackReceiver
{
    using receiver_concept = stdexec::receiver_tag;

    std::weak_ptr<ChunkPackPipelineImpl> pipeline;
    size_t                               index = 0;

    void set_value() noexcept;
    void set_error(Status status) noexcept;
    void set_stopped() noexcept;
};

using PackSender    = decltype(stdexec::when_all(std::declval<CallbackSender>(), std::declval<CallbackSender>(),
                                                 std::declval<CallbackSender>()));
using PackOperation = decltype(stdexec::connect(std::declval<PackSender>(), std::declval<PackReceiver>()));

struct PackWrite
{
    ChunkPagePack                    pack;
    uint64_t                         physical_length = 0;
    uint64_t                         source_offset   = 0;
    std::vector<uint8_t>             framed;
    std::array<MirrorWriteSource, 3> mirrors;
    std::unique_ptr<PackOperation>   operation;
};

class ChunkPackPipelineImpl final : public ChunkPackPipeline, public std::enable_shared_from_this<ChunkPackPipelineImpl>
{
  public:
    ChunkPackPipelineImpl(ChunkPageStore *owner, uint64_t generation, ChunkCancellation cancel,
                          AsyncCompletion on_complete)
        : store(owner),
          expected_generation(generation),
          cancellation(cancel),
          completion(on_complete)
    {
    }

    Status prepare()
    {
        if (expected_generation == std::numeric_limits<uint64_t>::max()) {
            return Status::resource_exhausted("chunk manifest generation is exhausted");
        }
        manifest                  = std::make_shared<ChunkManifest>();
        manifest->format_version  = kChunkManifestFormat;
        manifest->tree_id         = store->config_.tree_id;
        manifest->generation      = expected_generation + 1;
        manifest->owner_epoch     = store->config_.owner_epoch;
        manifest->logical_size    = store->staged_.size();
        manifest->published_at_ms = monotonic_millis();
        auto reuse_base           = store->reuse_base_manifest();
        if (reuse_base != nullptr) {
            const RootCatalog &reuse_catalog = reuse_base == store->inherited_manifest_ && store->inherited_catalog_
                                                 ? *store->inherited_catalog_
                                                 : *store->catalog_;
            Status             status        = store->validate_manifest(*reuse_base, reuse_catalog);
            if (!status.ok()) {
                return status;
            }
        }

        ChunkId  chunk         = store->active_chunk_id_;
        uint64_t logical_bytes = store->active_chunk_bytes_;
        uint64_t cursor        = store->active_chunk_cursor_;
        uint64_t offset        = 0;
        while (offset < store->staged_.size()) {
            if (cancellation.cancelled()) {
                return Status::unavailable("chunk manifest build cancelled");
            }
            const size_t length =
                std::min(store->config_.pack_bytes, store->staged_.size() - static_cast<size_t>(offset));
            const uint32_t checksum = crowdb::common::crc32c(store->staged_.data() + offset, length);
            if (reuse_base != nullptr && reuse_base->format_version >= 3 &&
                ChunkPageStore::find_pack_at(*reuse_base, offset, static_cast<uint32_t>(length)) == nullptr &&
                !store->range_was_written(offset, length)) {
                offset += length;
                continue;
            }
            if (reuse_base != nullptr) {
                const ChunkPagePack *reused = store->find_reusable_pack(
                    *reuse_base, offset, static_cast<uint32_t>(length), checksum, cancellation);
                if (cancellation.cancelled()) {
                    return Status::unavailable("chunk manifest reuse verification cancelled");
                }
                if (reused != nullptr) {
                    manifest->packs.push_back({.owner_tree_id  = chunk_pack_owner(*reuse_base, *reused),
                                               .ordinal        = manifest->packs.size(),
                                               .logical_offset = offset,
                                               .ref            = reused->ref,
                                               .reused         = true});
                    ++manifest->packs_reused;
                    manifest->pack_bytes_reused += length;
                    offset += length;
                    continue;
                }
            }
            if (!chunk.empty() && length > store->config_.max_chunk_bytes - logical_bytes) {
                if (writes.empty()) {
                    initial_rotated_chunk  = chunk;
                    initial_rotated_cursor = cursor;
                }
                chunk         = {};
                logical_bytes = 0;
                cursor        = 0;
            }
            if (chunk.empty()) {
                const uint64_t packs_per_chunk =
                    (store->config_.max_chunk_bytes + store->config_.pack_bytes - 1) / store->config_.pack_bytes;
                const uint64_t physical_pack_bytes =
                    round_up_to_iu(store->config_.pack_bytes, store->config_.page_alignment);
                if (packs_per_chunk > std::numeric_limits<uint64_t>::max() / physical_pack_bytes) {
                    return Status::resource_exhausted("chunk page framing exceeds address space");
                }
                Status status = store->transport_->allocate_mirror_chunk(packs_per_chunk * physical_pack_bytes,
                                                                         store->config_.owner_epoch, &chunk);
                if (!status.ok()) {
                    return status;
                }
            }

            auto job                 = std::make_unique<PackWrite>();
            job->pack.owner_tree_id  = store->config_.tree_id;
            job->pack.ordinal        = manifest->packs.size();
            job->pack.logical_offset = offset;
            job->pack.ref            = {
                .chunk_id = chunk, .offset = cursor, .length = static_cast<uint32_t>(length), .checksum = checksum};
            job->physical_length = round_up_to_iu(length, store->config_.page_alignment);
            job->source_offset   = offset;
            manifest->packs.push_back(job->pack);
            logical_bytes += length;
            cursor += job->physical_length;
            offset += length;
            writes.push_back(std::move(job));
        }
        ending_chunk         = chunk;
        ending_logical_bytes = logical_bytes;
        ending_cursor        = cursor;
        return Status::Ok();
    }

    void launch()
    {
        if (writes.empty()) {
            completion.complete(Status::Ok());
            return;
        }
        const size_t count = std::min(store->config_.max_concurrent_packs, writes.size());
        active.store(count, std::memory_order_relaxed);
        next.store(count, std::memory_order_relaxed);
        for (size_t index = 0; index < count; ++index) {
            launch_one(index);
        }
    }

    void pack_complete(Status status)
    {
        if (!status.ok()) {
            bool expected = false;
            if (failed.compare_exchange_strong(expected, true, std::memory_order_acq_rel)) {
                first_error = std::move(status);
            }
        }
        if (!failed.load(std::memory_order_acquire) && !stop_requested.load(std::memory_order_acquire)) {
            const size_t index = next.fetch_add(1, std::memory_order_acq_rel);
            if (index < writes.size()) {
                launch_one(index);
                return;
            }
        }
        if (active.fetch_sub(1, std::memory_order_acq_rel) == 1) {
            completion.complete(failed.load(std::memory_order_acquire) ? std::move(first_error) : Status::Ok());
        }
    }

    void release_frame(size_t index)
    {
        std::vector<uint8_t>().swap(writes[index]->framed);
    }

    Status finish(ChunkCancellation finish_cancellation, Status io_status) override
    {
        if (!io_status.ok()) {
            return abandon(std::move(io_status));
        }
        if (!initial_rotated_chunk.empty()) {
            Status status = store->transport_->seal_chunk(initial_rotated_chunk, store->config_.owner_epoch,
                                                          initial_rotated_cursor);
            if (!status.ok()) {
                return abandon(std::move(status));
            }
        }
        for (size_t index = 0; index < writes.size(); ++index) {
            const auto &job = writes[index];
            if (finish_cancellation.cancelled()) {
                return abandon(Status::unavailable("chunk root publication cancelled"));
            }
            Status status = store->transport_->advance_write(job->pack.ref.chunk_id, job->pack.ref.offset,
                                                             job->pack.ref.offset + job->physical_length);
            if (!status.ok()) {
                return abandon(std::move(status));
            }
            const bool last_for_chunk =
                index + 1 < writes.size() && writes[index + 1]->pack.ref.chunk_id != job->pack.ref.chunk_id;
            if (last_for_chunk) {
                status = store->transport_->seal_chunk(job->pack.ref.chunk_id, store->config_.owner_epoch,
                                                       job->pack.ref.offset + job->physical_length);
                if (!status.ok()) {
                    return abandon(std::move(status));
                }
            }
        }
        store->active_chunk_id_     = ending_chunk;
        store->active_chunk_bytes_  = ending_logical_bytes;
        store->active_chunk_cursor_ = ending_cursor;
        auto   reuse_base           = store->reuse_base_manifest();
        Status segment_status       = store->persist_reference_segments(manifest.get(), reuse_base.get());
        if (!segment_status.ok()) {
            remember_orphan_segments();
            return segment_status;
        }
        manifest->checksum       = ChunkPageStore::manifest_checksum(*manifest);
        Status validation_status = store->validate_manifest(*manifest, *store->catalog_);
        if (!validation_status.ok()) {
            remember_orphan_segments();
            return validation_status;
        }
        if (finish_cancellation.cancelled()) {
            remember_orphan_segments();
            return Status::unavailable("chunk root publication cancelled");
        }
        const uint64_t publish_started = monotonic_nanos();
        Status         status =
            store->catalog_->publish(store->config_.tree_id, expected_generation, store->config_.owner_epoch, manifest);
        const uint64_t publish_elapsed = monotonic_nanos() - publish_started;
        store->manifest_publication_latency_ns_.fetch_add(publish_elapsed, std::memory_order_relaxed);
        store->rpc_latency_ns_.fetch_add(publish_elapsed, std::memory_order_relaxed);
        store->rpc_operations_.fetch_add(1, std::memory_order_relaxed);
        if (!status.ok()) {
            remember_orphan_segments();
            return status;
        }
        store->generations_published_.fetch_add(1, std::memory_order_relaxed);
        store->packs_written_.fetch_add(manifest->packs.size() - manifest->packs_reused, std::memory_order_relaxed);
        store->pack_bytes_written_.fetch_add(new_pack_bytes(), std::memory_order_relaxed);
        store->packs_reused_.fetch_add(manifest->packs_reused, std::memory_order_relaxed);
        store->pack_bytes_reused_.fetch_add(manifest->pack_bytes_reused, std::memory_order_relaxed);
        store->cached_layout_.store(manifest, std::memory_order_release);
        store->layout_valid_until_ms_.store(monotonic_millis() + store->config_.layout_validity_ms,
                                            std::memory_order_release);
        store->staged_initialized_ = false;
        store->staged_.clear();
        store->dirty_ranges_.clear();
        store->inherited_manifest_.reset();
        store->inherited_catalog_.reset();
        store->data_durable_ = false;
        store->anchor_dirty_ = false;
        return Status::Ok();
    }

  private:
    void launch_one(size_t index)
    {
        auto &job = *writes[index];
        orphan_pack_bytes.fetch_add(job.pack.ref.length, std::memory_order_relaxed);
        job.framed.assign(job.physical_length, 0);
        std::copy_n(store->staged_.data() + job.source_offset, job.pack.ref.length, job.framed.data());
        for (uint32_t mirror = 0; mirror < job.mirrors.size(); ++mirror) {
            job.mirrors[mirror] = {.transport         = store->transport_,
                                   .chunk_id          = job.pack.ref.chunk_id,
                                   .mirror_index      = mirror,
                                   .offset            = job.pack.ref.offset,
                                   .bytes             = &job.framed,
                                   .cancellation      = cancellation,
                                   .retry_limit       = store->config_.mirror_retry_limit,
                                   .failure_mask      = &store->mirror_write_failure_mask_,
                                   .attempts          = &store->mirror_write_attempts_,
                                   .failures          = &store->mirror_write_failures_,
                                   .stop_requested    = &stop_requested,
                                   .diskio_operations = &store->diskio_operations_,
                                   .diskio_latency_ns = &store->diskio_latency_ns_};
        }
        auto sender   = stdexec::when_all(CallbackSender(&job.mirrors[0], &MirrorWriteSource::submit),
                                          CallbackSender(&job.mirrors[1], &MirrorWriteSource::submit),
                                          CallbackSender(&job.mirrors[2], &MirrorWriteSource::submit));
        job.operation = std::unique_ptr<PackOperation>(new PackOperation(
            stdexec::connect(std::move(sender), PackReceiver{.pipeline = weak_from_this(), .index = index})));
        stdexec::start(*job.operation);
    }

    void remember_orphan_segments()
    {
        store->orphan_bytes_.fetch_add(new_pack_bytes(), std::memory_order_relaxed);
        auto ids = ChunkPageStore::reference_segment_ids(*manifest);
        store->orphan_reference_segments_.insert(store->orphan_reference_segments_.end(), ids.begin(), ids.end());
    }

    Status abandon(Status status)
    {
        store->active_chunk_id_     = {};
        store->active_chunk_bytes_  = 0;
        store->active_chunk_cursor_ = 0;
        store->orphan_bytes_.fetch_add(orphan_pack_bytes.load(std::memory_order_relaxed), std::memory_order_relaxed);
        return status;
    }

    [[nodiscard]] uint64_t new_pack_bytes() const
    {
        uint64_t bytes = 0;
        for (const auto &write : writes) {
            bytes += write->pack.ref.length;
        }
        return bytes;
    }

    ChunkPageStore                         *store;
    uint64_t                                expected_generation;
    ChunkCancellation                       cancellation;
    AsyncCompletion                         completion;
    std::shared_ptr<ChunkManifest>          manifest;
    std::vector<std::unique_ptr<PackWrite>> writes;
    ChunkId                                 ending_chunk;
    uint64_t                                ending_logical_bytes = 0;
    uint64_t                                ending_cursor        = 0;
    ChunkId                                 initial_rotated_chunk;
    uint64_t                                initial_rotated_cursor = 0;
    std::atomic<size_t>                     next{0};
    std::atomic<size_t>                     active{0};
    std::atomic<bool>                       failed{false};
    std::atomic<bool>                       stop_requested{false};
    std::atomic<uint64_t>                   orphan_pack_bytes{0};
    Status                                  first_error;
};

void PackReceiver::set_value() noexcept
{
    if (auto owner = pipeline.lock()) {
        owner->release_frame(index);
        owner->pack_complete(Status::Ok());
    }
}

void PackReceiver::set_error(Status status) noexcept
{
    if (auto owner = pipeline.lock()) {
        owner->release_frame(index);
        owner->pack_complete(std::move(status));
    }
}

void PackReceiver::set_stopped() noexcept
{
    if (auto owner = pipeline.lock()) {
        owner->release_frame(index);
        owner->pack_complete(Status::unavailable("chunk mirror fan-out stopped"));
    }
}

std::shared_ptr<ChunkPackPipeline> ChunkPackPipeline::start(ChunkPageStore *store, uint64_t expected_generation,
                                                            ChunkCancellation cancellation, AsyncCompletion completion,
                                                            Status *start_status)
{
    auto pipeline = std::shared_ptr<ChunkPackPipelineImpl>(
        new ChunkPackPipelineImpl(store, expected_generation, cancellation, completion));
    *start_status = pipeline->prepare();
    if (start_status->ok()) {
        pipeline->launch();
    }
    return pipeline;
}

} // namespace crowdb::tree::detail
