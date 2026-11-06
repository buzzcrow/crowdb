// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "backend_metadata.h"
#include "crowdb-tree/backend/block_page_store.h"

#include <fcntl.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <cstring>

namespace crowdb::tree
{
// ── MemoryMedium ───────────────────────────────────────────────────

Status MemoryMedium::pwrite_at(uint64_t off, const uint8_t *buf, size_t len)
{
    if (len == 0) {
        return Status::Ok();
    }
    std::scoped_lock lk(mu_);
    if (off + len > data_.size()) {
        data_.resize(off + len, 0);
    }
    std::memcpy(data_.data() + off, buf, len);
    return Status::Ok();
}

Status MemoryMedium::pread_partial(uint64_t off, uint8_t *buf, size_t len, size_t *out_read) const
{
    std::scoped_lock lk(mu_);
    if (off >= data_.size()) {
        *out_read = 0;
        return Status::Ok();
    }
    size_t avail = data_.size() - off;
    size_t n     = len < avail ? len : avail;
    std::memcpy(buf, data_.data() + off, n);
    *out_read = n;
    return Status::Ok();
}

Status MemoryMedium::fsync()
{
    return Status::Ok();
}

uint64_t MemoryMedium::size() const
{
    std::scoped_lock lk(mu_);
    return data_.size();
}

// ── FileMedium ─────────────────────────────────────────────────────

namespace
{
Status open_file_descriptor(const char *path, bool o_direct, int *out)
{
#ifdef __APPLE__
    (void)o_direct; // macOS has no O_DIRECT; use F_NOCACHE instead
    int fd = ::open(path, O_RDWR | O_CREAT, 0644);
    if (fd < 0) {
        return Status::io_error(std::string("open: ") + std::strerror(errno));
    }
    if (::fcntl(fd, F_NOCACHE, 1) < 0) {
        ::close(fd);
        return Status::io_error(std::string("fcntl(F_NOCACHE): ") + std::strerror(errno));
    }
#else
    int flags = O_RDWR | O_CREAT;
    if (o_direct) {
        flags |= O_DIRECT;
    }
    int fd = ::open(path, flags, 0644);
    if (fd < 0) {
        return Status::io_error(std::string("open: ") + std::strerror(errno));
    }
#endif
    *out = fd;
    return Status::Ok();
}
} // namespace

void FileMedium::operator delete(void *pointer) noexcept
{
    detail::BackendObjectStorage::destroy(pointer);
}

Status FileMedium::open(const std::string &path, bool o_direct, std::unique_ptr<FileMedium> *out)
{
    detail::BackendObjectStorage storage(sizeof(FileMedium), alignof(FileMedium), {});
    auto                         result = std::unique_ptr<FileMedium>(::new (storage.get()) FileMedium(-1));
    (void)storage.release();
    auto status = open_file_descriptor(path.c_str(), o_direct, &result->fd_);
    if (status.ok()) {
        *out = std::move(result);
    }
    return status;
}

Status FileMedium::open_owned(const char *path, bool direct, const std::shared_ptr<AllocationBudget> &budget,
                              std::shared_ptr<FileMedium> *out, const std::shared_ptr<BackendReleaseService> &release)
{
    detail::BackendObjectStorage storage(sizeof(FileMedium), alignof(FileMedium), budget);
    auto                        *file = ::new (storage.get()) FileMedium(-1);
    (void)storage.release();
    std::shared_ptr<FileMedium> result(file, std::default_delete<FileMedium>{}, MetadataAllocator<FileMedium>(budget));
    auto                        status = result->prepare_close(budget, release);
    if (!status.ok()) {
        return status;
    }
    status = open_file_descriptor(path, direct, &result->fd_);
    if (status.ok()) {
        *out = std::move(result);
    }
    return status;
}

Status FileMedium::pwrite_at(uint64_t off, const uint8_t *buf, size_t len)
{
    if (len == 0) {
        return Status::Ok();
    }
    mark_dirty();

    struct CompletionMark
    {
        FileMedium &medium;

        ~CompletionMark()
        {
            medium.mark_dirty();
        }
    } completion{*this};

    size_t done = 0;
    while (done < len) {
        ssize_t n = ::pwrite(fd_, buf + done, len - done, static_cast<off_t>(off + done));
        if (n < 0) {
            if (errno == EINTR) {
                continue;
            }
            return Status::io_error(std::string("pwrite: ") + std::strerror(errno));
        }
        if (n == 0) {
            return Status::io_error("pwrite made no progress");
        }
        done += static_cast<size_t>(n);
    }
    return Status::Ok();
}

Status FileMedium::pread_partial(uint64_t off, uint8_t *buf, size_t len, size_t *out_read) const
{
    size_t done = 0;
    while (done < len) {
        ssize_t n = ::pread(fd_, buf + done, len - done, static_cast<off_t>(off + done));
        if (n < 0) {
            if (errno == EINTR) {
                continue;
            }
            *out_read = done;
            return Status::io_error(std::string("pread: ") + std::strerror(errno));
        }
        if (n == 0) {
            break; // EOF: not an error here, caller decides how to treat the
                   // shortfall
        }
        done += static_cast<size_t>(n);
    }
    *out_read = done;
    return Status::Ok();
}

Status FileMedium::fsync()
{
    const auto generation = dirty_generation();
#ifdef __APPLE__
    if (::fsync(fd_) < 0) {
        return Status::io_error(std::string("fsync: ") + std::strerror(errno));
    }
#else
    if (::fdatasync(fd_) < 0) {
        return Status::io_error(std::string("fdatasync: ") + std::strerror(errno));
    }
#endif
    acknowledge_sync(generation);
    return Status::Ok();
}

void FileMedium::mark_dirty() noexcept
{
    auto generation = written_.load(std::memory_order_relaxed);
    while (generation != UINT64_MAX &&
           !written_.compare_exchange_weak(generation, generation + 1, std::memory_order_release,
                                           std::memory_order_relaxed)) {
    }
}

uint64_t FileMedium::dirty_generation() const noexcept
{
    return written_.load(std::memory_order_acquire);
}

bool FileMedium::dirty() const noexcept
{
    const auto synced  = synced_.load(std::memory_order_acquire);
    const auto written = dirty_generation();
    return written == UINT64_MAX || written != synced;
}

void FileMedium::acknowledge_sync(uint64_t generation) noexcept
{
    // Saturation remains conservatively dirty instead of wrapping to clean.
    if (generation == UINT64_MAX) {
        return;
    }
    auto synced = synced_.load(std::memory_order_relaxed);
    while (synced < generation &&
           !synced_.compare_exchange_weak(synced, generation, std::memory_order_release, std::memory_order_relaxed)) {
    }
}

uint64_t FileMedium::size() const
{
    off_t end = ::lseek(fd_, 0, SEEK_END);
    return end < 0 ? 0 : static_cast<uint64_t>(end);
}

} // namespace crowdb::tree
