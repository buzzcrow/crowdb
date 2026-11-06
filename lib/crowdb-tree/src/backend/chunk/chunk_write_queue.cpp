// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "chunk_write_queue.h"

#include "backend/local/scheduled_page_store.h"
#include "chunk_frame.h"
#include "chunk_physical_registry.h"
#include "crowdb-common/log.h"

namespace crowdb::tree
{
struct ChunkWriteQueue::Request
{
    std::shared_ptr<Request>            queued_owner;
    Request                            *next = nullptr;
    std::shared_ptr<ScheduledPageStore> store;
    std::shared_ptr<ChunkPackBacking>   backing;
    std::span<const uint8_t>            input;
    AsyncCompletion                     complete;
};

struct ChunkWriteQueue::Batch
{
    std::shared_ptr<ChunkWriteQueue> queue;
    Request                         *head = nullptr;
    size_t                           pipe = 0;

    Status write()
    {
        auto &first = *head->backing;
        if (head->next == nullptr) {
            return write_chunk_backing(first, head->input, queue->metadata_);
        }
        const auto &domain = *first.domain;
        auto        status = domain.catalog->protection()->check_read_owner();
        if (!status.ok()) {
            return status;
        }
        std::vector<detail::ChunkFixedPackStore::Write, MetadataAllocator<detail::ChunkFixedPackStore::Write>> inputs{
            MetadataAllocator<detail::ChunkFixedPackStore::Write>(queue->metadata_)};
        for (auto *item = head; item != nullptr; item = item->next) {
            auto &backing = *item->backing;
            // Every member shares the writer pin. Its final owner releases the
            // entire allocation; dropping one page cannot expose its peers.
            backing.pin        = first.pin;
            backing.pack.stage = first.pack.stage;
            backing.pack.chunk = first.pack.chunk;
            inputs.push_back({.input = item->input, .out = &backing.pack});
        }
        first.pin->arm();
        detail::ChunkFixedPackStore store(domain.tree, domain.epoch, domain.mirrors, domain.catalog, domain.transport,
                                          domain.physical_registry);
        status = store.write_many(first.pack.stage, first.pack.chunk, first.pack.purpose, inputs, queue->metadata_,
                                  &first.physical, first.physical_budget);
        for (auto *item = head; item != nullptr; item = item->next) {
            item->backing->physical         = first.physical;
            item->backing->physical_metrics = ChunkPhysicalRegistry::extent(first.physical);
        }
        return status.ok() ? domain.catalog->protection()->check_read_owner() : status;
    }

    void finish(Status status) noexcept
    {
        queue->active_[pipe].store(false, std::memory_order_release);
        while (head != nullptr) {
            auto *item = head;
            head       = head->next;
            auto owner = std::move(item->queued_owner);
            try {
                item->complete.complete(status);
            }
            catch (...) {
                CRB_LOG_ERROR("Chunk write completion receiver threw");
            }
        }
        queue->wake();
    }

    static void completed(void *raw, Status status)
    {
        static_cast<Batch *>(raw)->finish(std::move(status));
    }
};

void ChunkWriteQueue::push(Request *request) noexcept
{
    auto *head = pending_.load(std::memory_order_relaxed);
    do {
        request->next = head;
    } while (!pending_.compare_exchange_weak(head, request, std::memory_order_release, std::memory_order_relaxed));
}

void ChunkWriteQueue::submit(std::shared_ptr<ScheduledPageStore> store, std::shared_ptr<ChunkPackBacking> backing,
                             std::span<const uint8_t> input, AsyncCompletion complete)
try {
    if (!backing || !backing->domain || !complete ||
        !chunk_backing_matches(*backing, backing->domain->backend_id, input.size()) ||
        backing->pack.stage.owner_epoch != backing->domain->epoch ||
        detail::chunk_framed_length(input.size()) > backing->domain->max_chunk_bytes) {
        complete.complete(Status::invalid_argument("write requires one reserved Chunk pack"));
        return;
    }
    auto request          = std::allocate_shared<Request>(MetadataAllocator<Request>(metadata_));
    request->store        = std::move(store);
    request->backing      = std::move(backing);
    request->input        = input;
    request->complete     = std::move(complete);
    request->queued_owner = request;
    push(request.get());
    wake();
}
catch (const std::bad_alloc &) {
    complete.complete(Status::resource_exhausted("Chunk write queue allocation failed"));
}

void ChunkWriteQueue::wake() noexcept
{
    for (size_t pipe = 0; pipe < active_.size(); ++pipe) {
        start(pipe);
    }
}

void ChunkWriteQueue::start(size_t pipe) noexcept
{
    if (active_[pipe].exchange(true, std::memory_order_acq_rel)) {
        return;
    }
    auto *incoming = pending_.exchange(nullptr, std::memory_order_acquire);
    if (incoming == nullptr) {
        active_[pipe].store(false, std::memory_order_release);
        if (pending_.load(std::memory_order_acquire) != nullptr) {
            start(pipe);
        }
        return;
    }
    // Reverse the producer stack so an old queued page is never starved.
    Request *ordered = nullptr;
    while (incoming != nullptr) {
        auto *next     = incoming->next;
        incoming->next = ordered;
        ordered        = incoming;
        incoming       = next;
    }
    std::shared_ptr<Batch> batch;
    try {
        batch        = std::allocate_shared<Batch>(MetadataAllocator<Batch>(metadata_));
        batch->queue = weak_from_this().lock();
        batch->pipe  = pipe;
    }
    catch (const std::bad_alloc &) {
        active_[pipe].store(false, std::memory_order_release);
        while (ordered != nullptr) {
            auto *item = ordered;
            ordered    = ordered->next;
            auto owner = std::move(item->queued_owner);
            item->complete.complete(Status::resource_exhausted("Chunk write batch allocation failed"));
        }
        wake();
        return;
    }
    const auto domain  = ordered->backing->domain;
    const auto purpose = ordered->backing->pack.purpose;
    Request  **tail    = &batch->head;
    size_t     bytes   = 0;
    size_t     count   = 0;
    while (ordered != nullptr) {
        auto *item        = ordered;
        ordered           = ordered->next;
        const auto length = detail::chunk_framed_length(item->input.size());
        if (item->backing->domain == domain && item->backing->pack.purpose == purpose &&
            count < detail::kMaxStagePacks && length <= domain->max_chunk_bytes - bytes) {
            item->next = nullptr;
            *tail      = item;
            tail       = &item->next;
            bytes += length;
            ++count;
        }
        else {
            push(item);
        }
    }
    auto status = batch->head->store->submit_step([batch] { return batch->write(); },
                                                  {.context     = batch.get(),
                                                   .complete_fn = &Batch::completed,
                                                   .owner       = batch,
                                                   .allocations = metadata_,
                                                   .cause       = batch->head->complete.cause},
                                                  0, bytes);
    if (!status.ok()) {
        batch->finish(std::move(status));
    }
}
} // namespace crowdb::tree
