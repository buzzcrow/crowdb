// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#pragma once

#include "crowdb-tree/backend/async_page_store.h"

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <thread>

namespace crowdb::tree::detail
{

class ChunkPageStore;

class ChunkAsyncExecutor
{
  public:
    enum class Kind : uint8_t { kRead, kWrite, kFsync };

    struct Task
    {
        Kind            kind         = Kind::kRead;
        PageAddr        addr         = 0;
        void           *buffer       = nullptr;
        const void     *const_buffer = nullptr;
        size_t          length       = 0;
        AsyncCompletion completion;
    };

    ChunkAsyncExecutor(ChunkPageStore *store, size_t capacity);
    ~ChunkAsyncExecutor();

    ChunkAsyncExecutor(const ChunkAsyncExecutor &)            = delete;
    ChunkAsyncExecutor &operator=(const ChunkAsyncExecutor &) = delete;

    [[nodiscard]] uint64_t submit(Task task);
    void                   cancel(uint64_t operation_id);

  private:
    struct Slot
    {
        std::atomic<uint64_t>  sequence{0};
        std::atomic<uint64_t>  active_id{0};
        std::atomic<uint64_t>  cancelled_id{0};
        std::atomic<bool>      async_started{false};
        std::atomic<bool>      async_ready{false};
        std::atomic<uint64_t> *wake_epoch = nullptr;
        Status                 async_status;
        std::shared_ptr<void>  async_state;
        Task                   task;
    };

    static constexpr uint64_t kClosedBit = uint64_t{1} << 63U;

    struct State
    {
        State(ChunkPageStore *owner, size_t queue_capacity);

        std::atomic<ChunkPageStore *> store;
        size_t                        capacity;
        std::unique_ptr<Slot[]>       slots;
        std::atomic<uint64_t>         enqueue_position{0};
        uint64_t                      dequeue_position = 0;
        std::atomic<uint64_t>         pending{0};
        std::atomic<uint64_t>         wake_epoch{0};
        std::atomic<uint64_t>         admission_state{0};
    };

    static void run(const std::shared_ptr<State> &state);
    static bool execute(const std::shared_ptr<State> &state, Slot *slot, uint64_t position);
    static void async_complete(void *context, Status status);
    static void release_slot(const std::shared_ptr<State> &state, Slot *slot, uint64_t position, Status status);
    static bool enter_submission(const std::shared_ptr<State> &state);
    static void leave_submission(const std::shared_ptr<State> &state);

    std::shared_ptr<State> state_;
    std::thread            worker_;
};

} // namespace crowdb::tree::detail
