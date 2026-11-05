// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "block_directory.h"

#include "btree/allocation_bound.h"

#include <unistd.h>

#include <cerrno>
#include <cstring>

namespace crowdb::tree
{
namespace
{
template <class T> T *create_metadata(const std::shared_ptr<AllocationBudget> &budget)
{
    MetadataAllocator<T> allocator(budget);
    auto                *result = allocator.allocate(1);
    std::construct_at(result, budget);
    return result;
}

template <class T> void destroy_metadata(T *value) noexcept
{
    if (value == nullptr) {
        return;
    }
    MetadataAllocator<T> allocator(value->budget);
    std::destroy_at(value);
    allocator.deallocate(value, 1);
}
} // namespace

BlockDirectory::Entry::Entry(std::shared_ptr<AllocationBudget> owner) : budget(std::move(owner))
{
    destroy = [](EpochManager::Deferred *entry) noexcept { destroy_metadata(static_cast<Entry *>(entry)); };
}

BlockDirectory::BlockDirectory(std::shared_ptr<AllocationBudget> budget, std::shared_ptr<BackendReleaseService> release)
    : budget_(std::move(budget)),
      epoch_(budget_)
{
    if (release) {
        retirement_.directory = this;
        retirement_.dispose   = [](BackendReleaseService::Node *base) noexcept {
            auto *directory = static_cast<Retirement *>(base)->directory;
            if (!directory->retire_step()) {
                return false;
            }
            MetadataAllocator<BlockDirectory> allocator(directory->budget_);
            std::destroy_at(directory);
            allocator.deallocate(directory, 1);
            return true;
        };
        if (!release->register_node(retirement_).ok()) {
            throw std::bad_alloc();
        }
    }
}

void BlockDirectoryDeleter::operator()(BlockDirectory *directory) const noexcept
{
    if (directory != nullptr) {
        directory->release_owned();
    }
}

void BlockDirectory::release_owned() noexcept
{
    if (retirement_.service) {
        BackendReleaseService::enqueue(retirement_);
    }
    else {
        MetadataAllocator<BlockDirectory> allocator(budget_);
        std::destroy_at(this);
        allocator.deallocate(this, 1);
    }
}

BlockDirectory::~BlockDirectory()
{
    if (!retiring_) {
        (void)epoch_.try_reclaim();
        destroy_node(root_, 24);
    }
}

void BlockDirectory::destroy_node(Node &node, unsigned shift)
{
    for (auto &slot : node.children) {
        void *child = slot.load(std::memory_order_relaxed);
        if (child == nullptr) {
            continue;
        }
        if (shift == 8) {
            auto *segment = static_cast<Segment *>(child);
            for (auto &entry : segment->slots) {
                destroy_metadata(entry.load(std::memory_order_relaxed));
            }
            destroy_metadata(segment);
        }
        else {
            auto *directory = static_cast<Node *>(child);
            destroy_node(*directory, shift - 8);
            destroy_metadata(directory);
        }
    }
}

BlockDirectory::Segment *BlockDirectory::segment(uint32_t index, bool create,
                                                 const std::shared_ptr<AllocationBudget> &budget)
{
    auto *node = &root_;
    for (unsigned shift = 24; shift >= 8; shift -= 8) {
        auto &slot  = node->children[(index >> shift) & 255];
        auto *child = slot.load(std::memory_order_acquire);
        if (child == nullptr && create) {
            auto *candidate = shift == 8 ? static_cast<void *>(create_metadata<Segment>(budget))
                                         : static_cast<void *>(create_metadata<Node>(budget));
            if (!slot.compare_exchange_strong(child, candidate, std::memory_order_release, std::memory_order_acquire)) {
                if (shift == 8) {
                    destroy_metadata(static_cast<Segment *>(candidate));
                }
                else {
                    destroy_metadata(static_cast<Node *>(candidate));
                }
            }
            else {
                child = candidate;
            }
        }
        if (child == nullptr || shift == 8) {
            return static_cast<Segment *>(child);
        }
        node = static_cast<Node *>(child);
    }
    return nullptr;
}

const BlockDirectory::Segment *BlockDirectory::segment(uint32_t index) const
{
    const auto *node = &root_;
    for (unsigned shift = 24; shift >= 8; shift -= 8) {
        auto *child = node->children[(index >> shift) & 255].load(std::memory_order_acquire);
        if (child == nullptr || shift == 8) {
            return static_cast<const Segment *>(child);
        }
        node = static_cast<const Node *>(child);
    }
    return nullptr;
}

std::shared_ptr<FileMedium> BlockDirectory::get(uint32_t index, const std::shared_ptr<AllocationBudget> &budget) const
{
    auto        guard = epoch_.enter(budget ? budget : budget_);
    const auto *leaf  = segment(index);
    const auto *entry = leaf != nullptr ? leaf->slots[index & 255].load(std::memory_order_acquire) : nullptr;
    return entry != nullptr && entry != &busy_ ? entry->medium : std::shared_ptr<FileMedium>{};
}

Status BlockDirectory::ensure(uint32_t index, const char *path, bool direct, std::shared_ptr<FileMedium> *out,
                              const std::shared_ptr<AllocationBudget> &budget)
try {
    auto  guard = epoch_.enter(budget ? budget : budget_);
    auto &slot  = segment(index, true, budget ? budget : budget_)->slots[index & 255];
    auto *entry = slot.load(std::memory_order_acquire);
    if (entry != nullptr && entry != &busy_) {
        *out = entry->medium;
        return Status::Ok();
    }
    if (entry == &busy_ || !slot.compare_exchange_strong(entry, &busy_, std::memory_order_acq_rel)) {
        return Status::unavailable("block metadata operation already in progress");
    }

    struct Restore
    {
        std::atomic<Entry *> &slot;
        bool                  published = false;

        ~Restore()
        {
            if (!published) {
                slot.store(nullptr, std::memory_order_release);
            }
        }
    } restore{.slot = slot};

    std::unique_ptr<Entry, void (*)(Entry *)> prepared(create_metadata<Entry>(budget ? budget : budget_),
                                                       &destroy_metadata<Entry>);
    std::shared_ptr<FileMedium>               file;
    auto status = FileMedium::open_owned(path, direct, budget ? budget : budget_, &file, retirement_.service);
    if (!status.ok()) {
        return status;
    }
    prepared->medium = std::move(file);
    *out             = prepared->medium;
    live_.fetch_add(1, std::memory_order_relaxed);
    slot.store(prepared.release(), std::memory_order_release);
    restore.published = true;
    return Status::Ok();
}
catch (const std::bad_alloc &) {
    return Status::resource_exhausted();
}

Status BlockDirectory::erase(uint32_t index, const char *path, const std::shared_ptr<AllocationBudget> &budget)
{
    {
        auto  guard = epoch_.enter(budget ? budget : budget_);
        auto *leaf  = segment(index, false, budget ? budget : budget_);
        if (leaf == nullptr) {
            return Status::Ok();
        }
        auto &slot  = leaf->slots[index & 255];
        auto *entry = slot.load(std::memory_order_acquire);
        if (entry == nullptr) {
            return Status::Ok();
        }
        if (entry == &busy_ || !slot.compare_exchange_strong(entry, &busy_, std::memory_order_acq_rel)) {
            return Status::unavailable("block metadata operation already in progress");
        }
        if (::unlink(path) < 0 && errno != ENOENT) {
            slot.store(entry, std::memory_order_release);
            return Status::io_error(std::string("unlink: ") + std::strerror(errno));
        }
        live_.fetch_sub(1, std::memory_order_relaxed);
        slot.store(nullptr, std::memory_order_release);
        epoch_.defer(entry);
    }
    (void)epoch_.try_reclaim();
    return Status::Ok();
}

bool BlockDirectory::next_node(const Node &node, unsigned shift, uint64_t prefix, uint64_t first, uint32_t *index,
                               std::shared_ptr<FileMedium> *out) const
{
    for (size_t i = 0; i < 256; ++i) {
        const uint64_t begin = prefix | (static_cast<uint64_t>(i) << shift);
        if (begin + (uint64_t{1} << shift) <= first) {
            continue;
        }
        const auto *child = node.children[i].load(std::memory_order_acquire);
        if (child == nullptr) {
            continue;
        }
        if (shift != 8) {
            if (next_node(*static_cast<const Node *>(child), shift - 8, begin, first, index, out)) {
                return true;
            }
            continue;
        }
        const auto &leaf = *static_cast<const Segment *>(child);
        for (auto j = static_cast<size_t>(std::max(first, begin) - begin); j < 256; ++j) {
            const auto *entry = leaf.slots[j].load(std::memory_order_acquire);
            if (entry != nullptr && entry != &busy_) {
                *index = static_cast<uint32_t>(begin + j);
                *out   = entry->medium;
                return true;
            }
        }
    }
    return false;
}

bool BlockDirectory::next(uint64_t *cursor, uint32_t *index, std::shared_ptr<FileMedium> *out,
                          const std::shared_ptr<AllocationBudget> &budget) const
{
    if (*cursor > UINT32_MAX) {
        return false;
    }
    auto guard = epoch_.enter(budget ? budget : budget_);
    if (!next_node(root_, 24, 0, *cursor, index, out)) {
        return false;
    }
    *cursor = static_cast<uint64_t>(*index) + 1;
    return true;
}

size_t BlockDirectory::extent_allocation_bound() noexcept
{
    // A previously absent sparse path owns two nodes, one leaf segment, one
    // incarnation and its file object/control. File object header/alignment
    // and the supported standard-library control fit in the shared allowance.
    return (2 * sizeof(Node)) + sizeof(Segment) + sizeof(Entry) + detail::shared_allocation_bound<FileMedium>() +
           FileMedium::close_allocation_bound() + 128;
}

size_t BlockDirectory::count() const noexcept
{
    return live_.load(std::memory_order_relaxed);
}
} // namespace crowdb::tree
