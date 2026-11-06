// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "crowdb-tree/backend/backend_release_service.h"
#include "crowdb-tree/backend/block_page_store.h"

#include <unistd.h>

namespace crowdb::tree
{
struct FileMedium::CloseNode : BackendReleaseService::Node
{
    std::shared_ptr<AllocationBudget> budget;
    int                               fd = -1;

    explicit CloseNode(std::shared_ptr<AllocationBudget> owner) : budget(std::move(owner))
    {
        dispose = [](BackendReleaseService::Node *base) noexcept {
            auto *node = static_cast<CloseNode *>(base);
            // close consumes this descriptor incarnation even on Linux EINTR.
            // Retrying the numeric descriptor could close an unrelated file.
            if (node->fd >= 0) {
                (void)::close(node->fd);
            }
            MetadataAllocator<CloseNode> allocator(node->budget);
            std::destroy_at(node);
            allocator.deallocate(node, 1);
            return true;
        };
    }
};

Status FileMedium::prepare_close(const std::shared_ptr<AllocationBudget>      &budget,
                                 const std::shared_ptr<BackendReleaseService> &service)
{
    if (!service) {
        return {};
    }
    MetadataAllocator<CloseNode> allocator(budget);
    auto                        *node = allocator.allocate(1);
    std::construct_at(node, budget);
    auto status = service->register_node(*node);
    if (!status.ok()) {
        std::destroy_at(node);
        allocator.deallocate(node, 1);
        return status;
    }
    close_ = node;
    return {};
}

FileMedium::~FileMedium()
{
    if (close_ != nullptr) {
        close_->fd = fd_;
        BackendReleaseService::enqueue(*close_);
    }
    else if (fd_ >= 0) {
        // Unique legacy tooling has an explicit synchronous host boundary.
        (void)::close(fd_);
    }
}

size_t FileMedium::close_allocation_bound() noexcept
{
    return sizeof(CloseNode);
}
} // namespace crowdb::tree
