// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

// Local asynchronous block-backend implementation.

#include "crowdb-common/diskio_uring.h"
#include "crowdb-tree/backend/async_page_store.h"
#include "crowdb-tree/backend/block_page_store.h"

#include <fcntl.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <cstdlib>
#include <cstring>
#include <limits>
#include <memory>
#include <utility>
#include <vector>

namespace crowdb::tree
{

namespace
{
// Maps a DiskIOUring callback's raw CQE `res` (>=0 bytes transferred, <0
// -errno) to a Status, treating a short read/write (res >= 0 but < the
// requested `len`) as an io_error too. Each extent is submitted once;
// crossing a block boundary does not retry a short read in the first file.
Status result_to_status(int res, size_t len, const char *op)
{
    if (res < 0) {
        return Status::io_error(std::string(op) + ": " + std::strerror(-res));
    }
    if (std::cmp_less(res, len)) {
        return Status::io_error(std::string("short ") + op + " (" + std::to_string(res) + " of " + std::to_string(len) +
                                " bytes)");
    }
    return Status::Ok();
}

struct ExtentRead
{
    int      fd;
    void    *buffer;
    size_t   length;
    uint64_t offset;
};

struct ExtentReads : std::enable_shared_from_this<ExtentReads>
{
    ::crowdb::common::DiskIOUring *uring = nullptr;
    std::vector<ExtentRead>        parts;
    AsyncCompletion                completion;

    void submit(size_t index)
    {
        const auto &part = parts[index];
        uring->submit_read(part.fd, part.buffer, part.length, static_cast<off_t>(part.offset),
                           [state = shared_from_this(), index, length = part.length](int result) {
                               Status status = result_to_status(result, length, "read");
                               if (!status.ok() || index + 1 == state->parts.size()) {
                                   if (state->completion) {
                                       state->completion.complete(std::move(status));
                                   }
                                   return;
                               }
                               state->submit(index + 1);
                           });
    }
};

// RAII aligned buffer for O_DIRECT writes via io_uring. std::vector memory
// is not guaranteed IU-aligned; this wraps posix_memalign + memcpy.
class AlignedIoBuf
{
  public:
    AlignedIoBuf(size_t len, size_t align) : len_(len)
    {
        if (::posix_memalign(&ptr_, align, len) != 0) {
            ptr_ = nullptr;
        }
    }

    ~AlignedIoBuf()
    {
        if (ptr_ != nullptr) {
            ::free(ptr_);
        }
    }

    AlignedIoBuf(const AlignedIoBuf &)            = delete;
    AlignedIoBuf &operator=(const AlignedIoBuf &) = delete;

    [[nodiscard]] bool ok() const
    {
        return ptr_ != nullptr;
    }

    [[nodiscard]] void *data() const
    {
        return ptr_;
    }

    [[nodiscard]] size_t size() const
    {
        return len_;
    }

  private:
    void  *ptr_ = nullptr;
    size_t len_ = 0;
};
} // namespace

// ── BlockAsyncPageStore ───────────────────────────────────────────

BlockAsyncPageStore::BlockAsyncPageStore(BlockPageStore *store, ::crowdb::common::DiskIOUring *uring)
    : store_(store),
      uring_(uring)
{
}

uint64_t BlockAsyncPageStore::submit_read(PageAddr addr, void *buf, size_t len, AsyncCompletion on_complete)
{
    if (len == 0 || len > std::numeric_limits<uint64_t>::max() - addr) {
        if (on_complete) {
            on_complete.complete(len == 0 ? Status::Ok() : Status::invalid_argument("read range overflow"));
        }
        return 0;
    }
    const uint64_t block_size = store_->block_size();
    if (block_size > 0 && len > block_size - (addr % block_size)) {
        auto state        = std::make_shared<ExtentReads>();
        state->uring      = uring_;
        state->completion = on_complete;
        size_t done       = 0;
        while (done < len) {
            uint64_t local = 0;
            int      fd    = store_->fd_for_offset(addr + done, &local);
            if (fd < 0) {
                if (on_complete) {
                    on_complete.complete(Status::io_error("BlockAsyncPageStore: no fd for offset"));
                }
                return 0;
            }
            size_t chunk = std::min(static_cast<uint64_t>(len - done), block_size - local);
            state->parts.push_back(
                {.fd = fd, .buffer = static_cast<uint8_t *>(buf) + done, .length = chunk, .offset = local});
            done += chunk;
        }
        state->submit(0);
        return 0;
    }
    uint64_t local = 0;
    int      fd    = store_->fd_for_offset(addr, &local);
    if (fd < 0) {
        if (on_complete) {
            on_complete.complete(Status::io_error("BlockAsyncPageStore: no fd for offset"));
        }
        return 0;
    }
    uring_->submit_read(fd, buf, len, static_cast<off_t>(local), [len, cb = on_complete](int res) {
        if (cb) {
            cb.complete(result_to_status(res, len, "read"));
        }
    });
    return 0;
}

uint64_t BlockAsyncPageStore::submit_write(PageAddr addr, const void *buf, size_t len, AsyncCompletion on_complete)
{
    // Ensure block files exist for this address range (mirrors sync
    // write_at_extents' allocation loop).
    Status es = store_->ensure_extents(addr, len);
    if (!es.ok()) {
        if (on_complete) {
            on_complete.complete(es);
        }
        return 0;
    }

    // O_DIRECT requires the buffer pointer to be IU/4096-aligned. The
    // caller's buffer (e.g. a std::vector<uint8_t> blob from persist.cpp)
    // is not guaranteed aligned, so when iu_size > 1 we copy into an
    // AlignedIoBuf kept alive via shared_ptr until the io_uring completion
    // callback fires.
    const uint32_t iu          = store_->iu_size();
    auto           maybe_align = [iu](const void *p, size_t n) -> std::shared_ptr<AlignedIoBuf> {
        if (iu <= 1 || (reinterpret_cast<uintptr_t>(p) % 4096 == 0)) {
            return nullptr; // already aligned or no alignment requirement
        }
        auto ab = std::make_shared<AlignedIoBuf>(n, 4096);
        if (!ab->ok()) {
            return nullptr; // alloc failure — caller will get io_error
        }
        std::memcpy(ab->data(), p, n);
        return ab;
    };

    uint64_t block_size = store_->block_size();
    if (block_size > 0) {
        uint64_t first_extent = addr / block_size;
        uint64_t last_extent  = (addr + len - 1) / block_size;
        if (last_extent != first_extent) {
            // Cross-extent write: split into per-extent submissions. All
            // completions share a WriteState; the last one invokes on_complete.
            struct WriteState
            {
                AsyncCompletion                            cb;
                Status                                     first_error;
                int                                        pending;
                std::vector<std::shared_ptr<AlignedIoBuf>> align_bufs; // keep alive until all CQEs fire
            };

            auto state     = std::make_shared<WriteState>();
            state->cb      = on_complete;
            state->pending = 0;

            uint64_t cur  = addr;
            size_t   done = 0;
            while (done < len) {
                uint64_t local = 0;
                int      fd    = store_->fd_for_offset(cur, &local);
                if (fd < 0) {
                    if (state->cb) {
                        state->cb.complete(Status::io_error("BlockAsyncPageStore: no fd for offset"));
                        state->cb = {};
                    }
                    return 0;
                }
                uint64_t avail = block_size - local;
                size_t   chunk = std::min(static_cast<uint64_t>(len - done), avail);

                const void *write_buf = static_cast<const uint8_t *>(buf) + done;
                auto        ab        = maybe_align(write_buf, chunk);
                if (iu > 1 && ab == nullptr && reinterpret_cast<uintptr_t>(write_buf) % 4096 != 0) {
                    if (state->cb) {
                        state->cb.complete(Status::io_error("BlockAsyncPageStore: aligned buffer alloc failed"));
                        state->cb = {};
                    }
                    return 0;
                }
                if (ab != nullptr) {
                    write_buf = ab->data();
                    state->align_bufs.push_back(ab);
                }

                state->pending++;
                uring_->submit_write(fd, write_buf, chunk, static_cast<off_t>(local), [state, chunk](int res) {
                    if (res < 0 || std::cmp_less(res, chunk)) {
                        Status s = result_to_status(res, chunk, "write");
                        if (state->first_error.ok()) {
                            state->first_error = std::move(s);
                        }
                    }
                    state->pending--;
                    if (state->pending == 0 && state->cb) {
                        state->cb.complete(state->first_error);
                    }
                });
                cur += chunk;
                done += chunk;
            }
            return 0;
        }
    }

    // Single-extent write
    uint64_t local = 0;
    int      fd    = store_->fd_for_offset(addr, &local);
    if (fd < 0) {
        if (on_complete) {
            on_complete.complete(Status::io_error("BlockAsyncPageStore: no fd for offset"));
        }
        return 0;
    }
    const void *write_buf = buf;
    auto        ab        = maybe_align(buf, len);
    if (iu > 1 && ab == nullptr && reinterpret_cast<uintptr_t>(buf) % 4096 != 0) {
        if (on_complete) {
            on_complete.complete(Status::io_error("BlockAsyncPageStore: aligned buffer alloc failed"));
        }
        return 0;
    }
    if (ab != nullptr) {
        write_buf = ab->data();
    }
    // Capture ab in the callback lambda to keep the aligned buffer alive
    // until the io_uring completion fires.
    uring_->submit_write(fd, write_buf, len, static_cast<off_t>(local), [len, cb = on_complete, ab](int res) {
        if (cb) {
            cb.complete(result_to_status(res, len, "write"));
        }
    });
    return 0;
}

Status BlockAsyncPageStore::submit_fsync(AsyncCompletion on_complete)
{
    std::vector<int> fds = store_->dirty_fds();
    if (fds.empty()) {
        if (on_complete) {
            on_complete.complete(Status::Ok());
        }
        return Status::Ok();
    }

    // Chain fsync across all dirty fds: each completion submits the next;
    // the last one invokes on_complete. A shared_ptr keeps the state alive
    // across the async chain (completions run on the poll thread).
    struct FsyncState
    {
        std::vector<int> fds;
        size_t           idx = 0;
        AsyncCompletion  cb;
    };

    auto state = std::make_shared<FsyncState>();
    state->fds = std::move(fds);
    state->cb  = on_complete;

    auto chain = std::make_shared<std::function<void()>>();
    *chain     = [this, state, chain] {
        if (state->idx >= state->fds.size()) {
            if (state->cb) {
                state->cb.complete(Status::Ok());
            }
            return;
        }
        int fd = state->fds[state->idx++];
        uring_->submit_fsync(fd, [this, state, chain](int res) {
            if (res < 0) {
                if (state->cb) {
                    state->cb.complete(Status::io_error(std::string("fsync: ") + std::strerror(-res)));
                    state->cb = {};
                }
                return;
            }
            (*chain)();
        });
    };
    (*chain)();
    return Status::Ok();
}

void BlockAsyncPageStore::cancel(uint64_t /*op_id*/)
{
    // No-op — per-op cancel is removed. Use DiskIOUring::cancel_fd for
    // fd-level cancellation.
}

} // namespace crowdb::tree
