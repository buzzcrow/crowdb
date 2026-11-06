// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#ifdef CROWDB_HAVE_LIBURING
#    include "crowdb-common/diskio_uring.h"

#    include <cerrno>
#    include <chrono>
#    include <climits>
#    include <new>

namespace crowdb::common
{
namespace
{
struct OwnedCommand
{
    int     fd;
    void   *buffer;
    size_t  length;
    off_t   offset;
    uint8_t opcode;
    bool    data_only;
};

void prepare_owned(const void *context, struct io_uring_sqe *sqe)
{
    const auto &command = *static_cast<const OwnedCommand *>(context);
    if (command.opcode == IORING_OP_READ) {
        ::io_uring_prep_read(sqe, command.fd, command.buffer, static_cast<unsigned>(command.length),
                             static_cast<__u64>(command.offset));
    }
    else if (command.opcode == IORING_OP_WRITE) {
        ::io_uring_prep_write(sqe, command.fd, command.buffer, static_cast<unsigned>(command.length),
                              static_cast<__u64>(command.offset));
    }
    else {
        ::io_uring_prep_fsync(sqe, command.fd, command.data_only ? IORING_FSYNC_DATASYNC : 0);
    }
}
} // namespace

size_t DiskIOUring::owned_submission_allocation_bytes() noexcept
{
    return sizeof(CallbackEntry);
}

void DiskIOUring::complete_entry(CallbackEntry *entry, int result)
{
    if (entry->owned.callback != nullptr) {
        entry->owned.complete(result);
    }
    else if (entry->cb) {
        entry->cb(result);
    }
}

void DiskIOUring::delete_entry(CallbackEntry *entry) noexcept
{
    if (entry->owned.allocation.deallocate == nullptr) {
        delete entry;
        return;
    }
    auto allocation = std::move(entry->owned.allocation);
    std::destroy_at(entry);
    allocation.deallocate(allocation.context, entry, sizeof(CallbackEntry), alignof(CallbackEntry));
}

void DiskIOUring::submit_owned(int fd, void *buffer, size_t length, off_t offset, uint8_t opcode, bool data_only,
                               IOCompletion complete)
{
    if (fd < 0 || fd >= fd_table_size_ || pipelines_.empty()) {
        complete.complete(-EBADF);
        return;
    }
    if (complete.callback == nullptr || !complete.allocation.owner || complete.allocation.allocate == nullptr ||
        complete.allocation.deallocate == nullptr || length > INT_MAX || offset < 0) {
        complete.complete(-EINVAL);
        return;
    }
    auto &pipeline = *pipelines_[fd_table_[fd].registered ? fd_table_[fd].pipeline : 0];
    if (!pipeline.valid) {
        complete.complete(-EIO);
        return;
    }
    CallbackEntry *entry = nullptr;
    try {
        void *storage =
            complete.allocation.allocate(complete.allocation.context, sizeof(CallbackEntry), alignof(CallbackEntry));
        if (storage == nullptr) {
            complete.complete(-ENOMEM);
            return;
        }
        entry               = ::new (storage) CallbackEntry{};
        entry->owned        = std::move(complete);
        entry->fd           = fd;
        entry->queued_nanos = static_cast<uint64_t>(
            std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now().time_since_epoch())
                .count());
    }
    catch (const std::bad_alloc &) {
        complete.complete(-ENOMEM);
        return;
    }
    const OwnedCommand command{
        .fd = fd, .buffer = buffer, .length = length, .offset = offset, .opcode = opcode, .data_only = data_only};
    submit_entry(pipeline, fd, entry, &command, &prepare_owned);
}

void DiskIOUring::submit_read_owned(int fd, void *buffer, size_t length, off_t offset, IOCompletion complete)
{
    submit_owned(fd, buffer, length, offset, IORING_OP_READ, false, std::move(complete));
}

void DiskIOUring::submit_write_owned(int fd, const void *buffer, size_t length, off_t offset, IOCompletion complete)
{
    submit_owned(fd, const_cast<void *>(buffer), length, offset, IORING_OP_WRITE, false, std::move(complete));
}

void DiskIOUring::submit_fsync_owned(int fd, bool data_only, IOCompletion complete)
{
    submit_owned(fd, nullptr, 0, 0, IORING_OP_FSYNC, data_only, std::move(complete));
}
} // namespace crowdb::common

#endif // CROWDB_HAVE_LIBURING
