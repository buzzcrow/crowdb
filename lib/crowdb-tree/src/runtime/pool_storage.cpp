// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "pool_storage.h"

#include <pthread.h>

#include <system_error>

namespace crowdb::tree::detail
{
struct PoolStorage::Worker
{
    PoolStorage *storage;
    void (*execute)(void *, uint32_t) noexcept;
    void             *context;
    uint32_t          index;
    pthread_t         thread{};
    std::thread::id   id;
    std::atomic<bool> ready{false};
};

void *PoolStorage::do_allocate(size_t bytes, size_t alignment)
{
    if (budget_ && !budget_->claim(bytes)) {
        throw std::bad_alloc();
    }
    void *pointer = nullptr;
    try {
        pointer = ::operator new(bytes, std::align_val_t(alignment));
    }
    catch (...) {
        if (budget_) {
            budget_->release(bytes);
        }
        throw;
    }
    if (budget_) {
        budget_->record_allocation(AllocationKind::kMetadata, bytes, true);
    }
    return pointer;
}

void PoolStorage::do_deallocate(void *pointer, size_t bytes, size_t alignment)
{
    ::operator delete(pointer, std::align_val_t(alignment));
    if (budget_) {
        budget_->record_allocation(AllocationKind::kMetadata, bytes, false);
        budget_->release(bytes);
    }
}

bool PoolStorage::do_is_equal(const std::pmr::memory_resource &other) const noexcept
{
    return this == &other;
}

exec::pool_thread_factory PoolStorage::thread_factory() noexcept
{
    return {.context = this, .start = &start, .join = &join, .id = &identity};
}

size_t PoolStorage::worker_bytes() noexcept
{
    return sizeof(Worker);
}

void *PoolStorage::start(void *resource, void (*execute)(void *, uint32_t) noexcept, void *context, uint32_t index)
{
    auto *storage = static_cast<PoolStorage *>(resource);
    auto *worker  = static_cast<Worker *>(storage->allocate(sizeof(Worker), alignof(Worker)));
    std::construct_at(worker, storage, execute, context, index);
    const auto error = pthread_create(&worker->thread, nullptr, &run, worker);
    if (error != 0) {
        std::destroy_at(worker);
        storage->deallocate(worker, sizeof(Worker), alignof(Worker));
        throw std::system_error(error, std::generic_category(), "tree worker start");
    }
    while (!worker->ready.load(std::memory_order_acquire)) {
        worker->ready.wait(false, std::memory_order_acquire);
    }
    return worker;
}

void *PoolStorage::run(void *pointer) noexcept
{
    auto *worker = static_cast<Worker *>(pointer);
    worker->id   = std::this_thread::get_id();
    worker->ready.store(true, std::memory_order_release);
    worker->ready.notify_one();
    worker->execute(worker->context, worker->index);
    return nullptr;
}

void PoolStorage::join(void *pointer) noexcept
{
    auto *worker = static_cast<Worker *>(pointer);
    if (pthread_join(worker->thread, nullptr) != 0) {
        std::terminate();
    }
    auto *storage = worker->storage;
    std::destroy_at(worker);
    storage->deallocate(worker, sizeof(Worker), alignof(Worker));
}

std::thread::id PoolStorage::identity(void *pointer) noexcept
{
    return static_cast<Worker *>(pointer)->id;
}
} // namespace crowdb::tree::detail
