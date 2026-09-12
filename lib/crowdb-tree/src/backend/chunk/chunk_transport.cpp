// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "chunk_transport.h"

#include <algorithm>
#include <array>
#include <cstring>
#include <limits>
#include <thread>

namespace crowdb::tree::detail
{
namespace
{

class SharedChunkIoExecutor
{
  public:
    struct Task
    {
        ChunkTransport          *transport = nullptr;
        ChunkId                  chunk_id;
        uint32_t                 mirror_index = 0;
        uint64_t                 offset       = 0;
        const uint8_t           *data         = nullptr;
        size_t                   length       = 0;
        ChunkTransportCompletion completion;
    };

    SharedChunkIoExecutor() : slots_(std::make_unique<Slot[]>(kCapacity))
    {
        for (uint64_t index = 0; index < kCapacity; ++index) {
            slots_[index].sequence.store(index, std::memory_order_relaxed);
        }
        for (auto &worker : workers_) {
            worker = std::thread([this] { run(); });
        }
    }

    ~SharedChunkIoExecutor()
    {
        uint64_t admission = admission_state_.fetch_or(kClosedBit, std::memory_order_acq_rel) | kClosedBit;
        while ((admission & ~kClosedBit) != 0) {
            admission_state_.wait(admission, std::memory_order_acquire);
            admission = admission_state_.load(std::memory_order_acquire);
        }
        wake_epoch_.fetch_add(1, std::memory_order_release);
        wake_epoch_.notify_all();
        for (auto &worker : workers_) {
            if (worker.joinable()) {
                worker.join();
            }
        }
    }

    bool submit(Task task)
    {
        if (!enter_submission()) {
            return false;
        }
        uint64_t position = enqueue_.load(std::memory_order_relaxed);
        for (;;) {
            Slot          &slot       = slots_[position % kCapacity];
            const uint64_t sequence   = slot.sequence.load(std::memory_order_acquire);
            const auto     difference = static_cast<int64_t>(sequence - position);
            if (difference == 0) {
                if (!enqueue_.compare_exchange_weak(position, position + 1, std::memory_order_relaxed)) {
                    continue;
                }
                slot.task = std::move(task);
                pending_.fetch_add(1, std::memory_order_release);
                slot.sequence.store(position + 1, std::memory_order_release);
                wake_epoch_.fetch_add(1, std::memory_order_release);
                wake_epoch_.notify_one();
                leave_submission();
                return true;
            }
            if (difference < 0) {
                leave_submission();
                return false;
            }
            position = enqueue_.load(std::memory_order_relaxed);
        }
    }

  private:
    static constexpr uint64_t kClosedBit = uint64_t{1} << 63U;
    static constexpr uint64_t kCapacity  = 1024;
    static constexpr size_t   kWorkers   = 4;

    struct Slot
    {
        std::atomic<uint64_t> sequence{0};
        Task                  task;
    };

    bool enter_submission()
    {
        uint64_t admission = admission_state_.load(std::memory_order_acquire);
        for (;;) {
            if ((admission & kClosedBit) != 0) {
                return false;
            }
            if (admission_state_.compare_exchange_weak(admission, admission + 1, std::memory_order_acq_rel,
                                                       std::memory_order_acquire)) {
                return true;
            }
        }
    }

    void leave_submission()
    {
        const uint64_t prior = admission_state_.fetch_sub(1, std::memory_order_acq_rel);
        if ((prior & kClosedBit) != 0) {
            admission_state_.notify_all();
        }
    }

    void run()
    {
        for (;;) {
            uint64_t position = dequeue_.load(std::memory_order_relaxed);
            bool     claimed  = false;
            for (;;) {
                Slot          &slot       = slots_[position % kCapacity];
                const uint64_t sequence   = slot.sequence.load(std::memory_order_acquire);
                const auto     difference = static_cast<int64_t>(sequence - (position + 1));
                if (difference == 0) {
                    if (!dequeue_.compare_exchange_weak(position, position + 1, std::memory_order_relaxed)) {
                        continue;
                    }
                    Task task = slot.task;
                    slot.sequence.store(position + kCapacity, std::memory_order_release);
                    pending_.fetch_sub(1, std::memory_order_acq_rel);
                    task.completion.complete(
                        task.completion.stop_requested()
                            ? Status::unavailable("chunk mirror write stopped before transport submission")
                            : task.transport->write_mirror(task.chunk_id, task.mirror_index, task.offset, task.data,
                                                           task.length));
                    claimed = true;
                    break;
                }
                if (difference < 0) {
                    break;
                }
                position = dequeue_.load(std::memory_order_relaxed);
            }
            if (claimed) {
                continue;
            }
            const uint64_t admission = admission_state_.load(std::memory_order_acquire);
            if ((admission & kClosedBit) != 0 && (admission & ~kClosedBit) == 0 &&
                pending_.load(std::memory_order_acquire) == 0) {
                return;
            }
            const uint64_t epoch = wake_epoch_.load(std::memory_order_acquire);
            if (pending_.load(std::memory_order_acquire) == 0 && (admission & kClosedBit) == 0) {
                wake_epoch_.wait(epoch, std::memory_order_relaxed);
            }
            else {
                std::this_thread::yield();
            }
        }
    }

    std::unique_ptr<Slot[]>           slots_;
    std::array<std::thread, kWorkers> workers_;
    std::atomic<uint64_t>             enqueue_{0};
    std::atomic<uint64_t>             dequeue_{0};
    std::atomic<uint64_t>             pending_{0};
    std::atomic<uint64_t>             wake_epoch_{0};
    std::atomic<uint64_t>             admission_state_{0};
};

SharedChunkIoExecutor &shared_chunk_io_executor()
{
    static SharedChunkIoExecutor executor;
    return executor;
}

} // namespace

void ChunkTransport::submit_write_mirror(ChunkId chunk_id, uint32_t mirror_index, uint64_t offset, const uint8_t *data,
                                         size_t length, ChunkTransportCompletion completion)
{
    if (!shared_chunk_io_executor().submit({
            .transport    = this,
            .chunk_id     = chunk_id,
            .mirror_index = mirror_index,
            .offset       = offset,
            .data         = data,
            .length       = length,
            .completion   = completion,
        })) {
        completion.complete(Status::resource_exhausted("shared chunk I/O queue is full"));
    }
}

template <typename Mutation> Status MemoryChunkTransport::mutate(ChunkId chunk_id, Mutation mutation)
{
    if (unavailable_.load(std::memory_order_acquire)) {
        return Status::unavailable("chunk transport is unavailable");
    }
    auto current = chunks_.load(std::memory_order_acquire);
    for (;;) {
        if (current == nullptr) {
            return Status::unavailable("chunk does not exist");
        }
        auto next  = std::make_shared<Chunks>(*current);
        auto found = std::find_if(next->begin(), next->end(),
                                  [chunk_id](const Chunk &chunk) { return chunk.layout.chunk_id == chunk_id; });
        if (found == next->end()) {
            return Status::unavailable("chunk does not exist");
        }
        Status status = mutation(*found);
        if (!status.ok()) {
            return status;
        }
        if (chunks_.compare_exchange_weak(current, next, std::memory_order_release, std::memory_order_acquire)) {
            return Status::Ok();
        }
    }
}

Status MemoryChunkTransport::allocate_mirror_chunk(uint64_t logical_capacity, uint64_t owner_epoch, ChunkId *chunk_id)
{
    if (logical_capacity == 0 || chunk_id == nullptr) {
        return Status::invalid_argument("chunk allocation arguments are invalid");
    }
    if (unavailable_.load(std::memory_order_acquire)) {
        return Status::unavailable("chunk transport is unavailable");
    }
    const ChunkId allocated(next_chunk_id_.fetch_add(1, std::memory_order_relaxed));
    auto          current = chunks_.load(std::memory_order_acquire);
    for (;;) {
        auto next = current == nullptr ? std::make_shared<Chunks>() : std::make_shared<Chunks>(*current);
        next->push_back({
            .layout      = {.chunk_id = allocated, .logical_capacity = logical_capacity},
            .owner_epoch = owner_epoch,
            .mirrors     = {}
        });
        if (chunks_.compare_exchange_weak(current, next, std::memory_order_release, std::memory_order_acquire)) {
            *chunk_id = allocated;
            return Status::Ok();
        }
    }
}

Status MemoryChunkTransport::write_mirror(ChunkId chunk_id, uint32_t mirror_index, uint64_t offset, const uint8_t *data,
                                          size_t length)
{
    if (mirror_index >= 3 || (data == nullptr && length != 0) || offset > std::numeric_limits<size_t>::max() ||
        length > std::numeric_limits<size_t>::max() - offset) {
        return Status::invalid_argument("chunk mirror write arguments are invalid");
    }
    return mutate(chunk_id, [mirror_index, offset, data, length](Chunk &chunk) {
        if (chunk.layout.sealed) {
            return Status::invalid_argument("sealed chunk cannot be written");
        }
        const size_t end = static_cast<size_t>(offset) + length;
        if (end > chunk.layout.logical_capacity) {
            return Status::resource_exhausted("chunk mirror write exceeds capacity");
        }
        auto &mirror = chunk.mirrors[mirror_index];
        if (end > mirror.size()) {
            mirror.resize(end, 0);
        }
        if (length != 0) {
            std::memcpy(mirror.data() + offset, data, length);
        }
        return Status::Ok();
    });
}

Status MemoryChunkTransport::advance_write(ChunkId chunk_id, uint64_t expected_bytes, uint64_t acknowledged_bytes)
{
    return mutate(chunk_id, [expected_bytes, acknowledged_bytes](Chunk &chunk) {
        if (chunk.layout.sealed || chunk.layout.acknowledged_bytes != expected_bytes ||
            acknowledged_bytes < expected_bytes || acknowledged_bytes > chunk.layout.logical_capacity) {
            return Status::invalid_argument("chunk acknowledged cursor is invalid");
        }
        chunk.layout.acknowledged_bytes = acknowledged_bytes;
        return Status::Ok();
    });
}

Status MemoryChunkTransport::read_mirror(ChunkId chunk_id, uint32_t mirror_index, uint64_t offset, uint8_t *data,
                                         size_t length) const
{
    if (unavailable_.load(std::memory_order_acquire)) {
        return Status::unavailable("chunk transport is unavailable");
    }
    if (mirror_index >= 3 || (data == nullptr && length != 0)) {
        return Status::invalid_argument("chunk mirror read arguments are invalid");
    }
    auto chunks = chunks_.load(std::memory_order_acquire);
    if (chunks == nullptr) {
        return Status::unavailable("chunk does not exist");
    }
    const auto found = std::find_if(chunks->begin(), chunks->end(),
                                    [chunk_id](const Chunk &chunk) { return chunk.layout.chunk_id == chunk_id; });
    if (found == chunks->end() || offset > found->layout.acknowledged_bytes ||
        length > found->layout.acknowledged_bytes - offset || offset > found->mirrors[mirror_index].size() ||
        length > found->mirrors[mirror_index].size() - offset) {
        return Status::unavailable("chunk mirror range is not acknowledged");
    }
    if (length != 0) {
        std::memcpy(data, found->mirrors[mirror_index].data() + offset, length);
    }
    return Status::Ok();
}

Status MemoryChunkTransport::query_chunk(ChunkId chunk_id, ChunkLayout *layout) const
{
    if (layout == nullptr) {
        return Status::invalid_argument("chunk layout output is null");
    }
    if (unavailable_.load(std::memory_order_acquire)) {
        return Status::unavailable("chunk transport is unavailable");
    }
    auto chunks = chunks_.load(std::memory_order_acquire);
    if (chunks == nullptr) {
        return Status::unavailable("chunk does not exist");
    }
    const auto found = std::find_if(chunks->begin(), chunks->end(),
                                    [chunk_id](const Chunk &chunk) { return chunk.layout.chunk_id == chunk_id; });
    if (found == chunks->end()) {
        return Status::unavailable("chunk does not exist");
    }
    *layout = found->layout;
    return Status::Ok();
}

Status MemoryChunkTransport::seal_chunk(ChunkId chunk_id, uint64_t owner_epoch, uint64_t acknowledged_bytes)
{
    return mutate(chunk_id, [owner_epoch, acknowledged_bytes](Chunk &chunk) {
        if (chunk.owner_epoch != owner_epoch || chunk.layout.acknowledged_bytes != acknowledged_bytes) {
            return Status::unavailable("chunk seal is fenced by owner epoch or cursor");
        }
        chunk.layout.sealed = true;
        return Status::Ok();
    });
}

void MemoryChunkTransport::corrupt_mirror(ChunkId chunk_id, uint32_t mirror_index, uint64_t offset)
{
    if (mirror_index >= 3) {
        return;
    }
    static_cast<void>(mutate(chunk_id, [mirror_index, offset](Chunk &chunk) {
        if (offset < chunk.mirrors[mirror_index].size()) {
            chunk.mirrors[mirror_index][offset] ^= 0xffU;
        }
        return Status::Ok();
    }));
}

} // namespace crowdb::tree::detail
