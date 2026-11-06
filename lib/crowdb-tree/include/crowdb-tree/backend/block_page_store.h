// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

// BlockPageStore: raw block-device / O_DIRECT-file / in-memory backend.
//
// Uses a BlockPageStoreMedium abstraction to support multiple backing media
// (file, block device, memory) through a single code path. When iu_size == 1
// (byte-aligned), all I/O is byte-granular with no alignment checks or bounce
// buffers. When iu_size > 1 (e.g. 4096 for NVMe), unaligned writes are
// bounced through an aligned scratch buffer (read-modify-write).
//
// MemoryMedium (iu_size=1) replaces MemPageStore for tests/SCM.
// FileMedium wraps a single fd with pwrite/pread/fdatasync/lseek.
// Future ScmMedium will add SCM/PMEM support — same interface.
#pragma once

#include "crowdb-tree/backend/page_store.h"
#include "crowdb-tree/memory_budget.h"

#include <atomic>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

namespace crowdb::tree
{

class BlockDirectory;
class BackendReleaseService;

namespace detail
{
class AllocationAccount;
struct BlockAccountAccess;
} // namespace detail

struct BlockDirectoryDeleter
{
    std::shared_ptr<AllocationBudget> budget;
    void                              operator()(BlockDirectory *directory) const noexcept;
};

// ── Medium abstraction ────────────────────────────────────────────
// BlockPageStore delegates all raw I/O to a Medium. This allows the same
// alignment/bounce logic to work over files, block devices, and in-memory
// buffers.
class BlockPageStoreMedium
{
  public:
    virtual ~BlockPageStoreMedium() = default;

    // Write `len` bytes at `off`. Grows the medium as needed.
    virtual Status pwrite_at(uint64_t off, const uint8_t *buf, size_t len) = 0;

    // Read up to `len` bytes at `off`. `*out_read` reports how many bytes
    // were actually read (may be less than `len` on EOF / growing file).
    virtual Status pread_partial(uint64_t off, uint8_t *buf, size_t len, size_t *out_read) const = 0;

    // Read exactly `len` bytes at `off`. Returns not_found if fewer bytes
    // available.
    Status pread_at(uint64_t off, uint8_t *buf, size_t len) const
    {
        size_t got = 0;
        Status s   = pread_partial(off, buf, len, &got);
        if (!s.ok()) {
            return s;
        }
        if (got < len) {
            return Status::not_found("BlockPageStoreMedium: read past end");
        }
        return Status::Ok();
    }

    // Durability barrier.
    virtual Status fsync() = 0;

    // Current logical size in bytes.
    [[nodiscard]] virtual uint64_t size() const = 0;
};

// In-memory medium: backs onto std::vector<uint8_t> + mutex. fsync() is
// a no-op. This is the test/SCM path (iu_size=1, byte-aligned).
class MemoryMedium : public BlockPageStoreMedium
{
  public:
    MemoryMedium() = default;

    Status                 pwrite_at(uint64_t off, const uint8_t *buf, size_t len) override;
    Status                 pread_partial(uint64_t off, uint8_t *buf, size_t len, size_t *out_read) const override;
    Status                 fsync() override;
    [[nodiscard]] uint64_t size() const override;

    // Direct access to the underlying buffer (for content verification tests).
    [[nodiscard]] const std::vector<uint8_t> &data() const
    {
        return data_;
    }

  private:
    mutable std::mutex   mu_;
    std::vector<uint8_t> data_;
};

// File medium: wraps a single fd with pwrite/pread/fdatasync/lseek.
// Used by array-of-blocks (one FileMedium per block extent).
class FileMedium : public BlockPageStoreMedium
{
  public:
    ~FileMedium() override;
    static void operator delete(void *pointer) noexcept;

    FileMedium(const FileMedium &)            = delete;
    FileMedium &operator=(const FileMedium &) = delete;

    // Open (creating if absent) a regular file. `o_direct` selects O_DIRECT
    // on Linux / F_NOCACHE on macOS.
    static Status open(const std::string &path, bool o_direct, std::unique_ptr<FileMedium> *out);
    static Status open_owned(const char *path, bool direct, const std::shared_ptr<AllocationBudget> &budget,
                             std::shared_ptr<FileMedium>                  *out,
                             const std::shared_ptr<BackendReleaseService> &release = {});

    Status                 pwrite_at(uint64_t off, const uint8_t *buf, size_t len) override;
    Status                 pread_partial(uint64_t off, uint8_t *buf, size_t len, size_t *out_read) const override;
    Status                 fsync() override;
    [[nodiscard]] uint64_t size() const override;

    [[nodiscard]] static size_t close_allocation_bound() noexcept;

    [[nodiscard]] int fd() const
    {
        return fd_;
    }

    // A barrier acknowledges only the generation observed before submission.
    // Writers mark both submission and completion so a racing barrier cannot
    // consume a write that has not yet reached the device.
    void                   mark_dirty() noexcept;
    [[nodiscard]] uint64_t dirty_generation() const noexcept;
    [[nodiscard]] bool     dirty() const noexcept;
    void                   acknowledge_sync(uint64_t generation) noexcept;

  private:
    explicit FileMedium(int fd) : fd_(fd)
    {
    }

    struct CloseNode;
    [[nodiscard]] Status  prepare_close(const std::shared_ptr<AllocationBudget>      &budget,
                                        const std::shared_ptr<BackendReleaseService> &service);
    CloseNode            *close_ = nullptr;
    int                   fd_    = -1;
    std::atomic<uint64_t> written_{0};
    std::atomic<uint64_t> synced_{0};
};

struct BlockSyncTarget
{
    std::shared_ptr<FileMedium> medium;
    uint64_t                    generation = 0;
};

// ── BlockPageStore ────────────────────────────────────────────────
class BlockPageStore : public PageStore
{
  public:
    ~BlockPageStore() override;

    // A standalone C++ host retains this service and pumps it until drained
    // after releasing the store and any retained file readers. A bound wake
    // driver owns this responsibility across Tree/runtime close.
    [[nodiscard]] const std::shared_ptr<BackendReleaseService> &release_service() const noexcept
    {
        return release_;
    }

    static void operator delete(void *pointer) noexcept;

    BlockPageStore(const BlockPageStore &)            = delete;
    BlockPageStore &operator=(const BlockPageStore &) = delete;

    // Open a raw block device or a regular file with O_DIRECT (creating a
    // regular file if absent). `iu_size` is the alignment/IU unit; for a
    // real block device on Linux it is overridden by the probed logical
    // sector size.
    static Status open(const std::string &path, uint32_t iu_size, std::unique_ptr<BlockPageStore> *out);

    // Open an array-of-blocks store. Creates
    // `{dir}/{store_id}-{group_id}.blk-{NNNN}` files on demand when the current
    // block fills up. On reopen, scans the directory for existing block files and
    // opens them all.
    static Status open_blocks(const std::string &dir, uint32_t store_id, uint32_t group_id, uint64_t block_size,
                              uint32_t iu_size, std::unique_ptr<BlockPageStore> *out);

    // Open an in-memory store (MemoryMedium, iu_size=1). Replaces MemPageStore.
    static Status open_mem(uint32_t iu_size, std::unique_ptr<BlockPageStore> *out);

    Status                 write_at(uint64_t off, const uint8_t *buf, size_t len) override;
    Status                 read_at(uint64_t off, uint8_t *buf, size_t len) const override;
    Status                 sync() override;
    [[nodiscard]] uint64_t size() const override;
    Status write_at(uint64_t off, const uint8_t *buf, size_t len, const std::shared_ptr<AllocationBudget> &budget);
    Status read_at(uint64_t off, uint8_t *buf, size_t len, const std::shared_ptr<AllocationBudget> &budget) const;
    Status sync(const std::shared_ptr<AllocationBudget> &budget);
    [[nodiscard]] uint64_t size(const std::shared_ptr<AllocationBudget> &budget) const;

    [[nodiscard]] uint32_t iu_size() const override
    {
        return iu_size_;
    }

    [[nodiscard]] uint64_t block_size() const override
    {
        return block_size_;
    }

    // Delete a block file (array-of-blocks mode only). Unlinks its current
    // incarnation; already accepted I/O retains the old fd until completion.
    // Safe only after snapshot commit confirms zero live pages in the block.
    Status delete_block(uint32_t block_idx) override;
    Status delete_block(uint32_t block_idx, const std::shared_ptr<AllocationBudget> &budget);

    [[nodiscard]] bool is_block_device() const
    {
        return is_block_device_;
    }

    // Access the underlying medium (for single-medium stores only).
    [[nodiscard]] BlockPageStoreMedium *medium() const
    {
        return medium_.get();
    }

    // For async I/O: maps a global byte offset to the underlying fd and
    // local offset within that fd's file. Returns -1 if no extent covers
    // this offset, the extent is deleted, or the medium is not a FileMedium
    // (e.g. MemoryMedium). Used by BlockAsyncPageStore.
    int fd_for_offset(uint64_t off, uint64_t *out_local) const;

    // Retain the actual fd incarnation through an asynchronous operation.
    [[nodiscard]] std::shared_ptr<FileMedium>
    file_for_offset(uint64_t off, uint64_t *out_local, const std::shared_ptr<AllocationBudget> &budget = {}) const;

    // Ensures block files are allocated for the byte range [off, off+len),
    // mirroring write_at_extents' allocation logic without writing any data.
    // Also marks the covered extents as dirty. Call before fd_for_offset()
    // in the async write path so the fd exists.
    Status ensure_extents(uint64_t off, size_t len, const std::shared_ptr<AllocationBudget> &budget = {});

    // Returns fds of all dirty, non-deleted extents (array-of-blocks) or the
    // single medium's fd (single-file mode), for async fsync via DiskIOUring.
    // Enumeration does not acknowledge durability. Failed or unknown barriers
    // keep their captured generation dirty; newer writes survive older success.
    // Returns an empty vector if no fd is fsync-able (e.g. MemoryMedium or
    // SyncMode::kSkip).
    [[nodiscard]] std::vector<int> dirty_fds() const;

    [[nodiscard]] std::vector<BlockSyncTarget> sync_targets() const;
    using BlockSyncTargets = std::vector<BlockSyncTarget, MetadataAllocator<BlockSyncTarget>>;
    [[nodiscard]] BlockSyncTargets sync_targets(const std::shared_ptr<AllocationBudget> &budget) const;

    // Returns fds of all live, non-deleted extents (array-of-blocks) or the
    // single medium's fd (single-file mode), for fd registration with
    // DiskIOUring at startup. Returns an empty vector if no fd is usable
    // (e.g. MemoryMedium).
    [[nodiscard]] std::vector<int> all_extent_fds() const;

    // Number of live block files in an array-of-blocks store (0 for
    // single-medium).
    [[nodiscard]] size_t num_extents() const;

  private:
    friend struct detail::BlockAccountAccess;
    BlockPageStore(std::shared_ptr<detail::AllocationAccount> allocations, std::shared_ptr<BlockPageStoreMedium> medium,
                   uint32_t iu_size, bool is_block_device, std::shared_ptr<BackendReleaseService> release = {});
    BlockPageStore(std::shared_ptr<detail::AllocationAccount> allocations, const std::string &dir, uint32_t store_id,
                   uint32_t group_id, uint64_t block_size, uint32_t iu_size,
                   std::shared_ptr<BackendReleaseService> release);

    [[nodiscard]] int fd() const;    // returns -1 if no FileMedium
    uint64_t          capacity_ = 0; // probed fixed device size; 0 = unknown

    // Array-of-blocks management
    using BlockPath = std::basic_string<char, std::char_traits<char>, MetadataAllocator<char>>;
    [[nodiscard]] BlockPath block_path(uint32_t index, const std::shared_ptr<AllocationBudget> &budget = {}) const;
    Status                  write_at_extents(uint64_t off, const uint8_t *buf, size_t len,
                                             const std::shared_ptr<AllocationBudget> &budget);
    Status                  read_at_extents(uint64_t off, uint8_t *buf, size_t len,
                                            const std::shared_ptr<AllocationBudget> &budget) const;

    std::shared_ptr<detail::AllocationAccount> allocations_;
    std::shared_ptr<BackendReleaseService>     release_;
    std::shared_ptr<BlockPageStoreMedium>      medium_; // single-medium mode
    uint32_t                                   iu_size_;
    bool                                       is_block_device_;

    // Array-of-blocks mode
    uint64_t                                               block_size_ = 0;
    uint32_t                                               store_id_   = 0;
    uint32_t                                               group_id_   = 0;
    BlockPath                                              dir_;
    std::unique_ptr<BlockDirectory, BlockDirectoryDeleter> blocks_;
};

// Dump utility: annotated hex dump of a block file. Parses known structures
// at fixed offsets (anchor at offset 0, page blob envelopes) and renders them
// as human-readable text. Unknown regions shown as hex.
Status dump_block_file(const std::string &path, uint32_t iu_size, std::string *out);

} // namespace crowdb::tree
