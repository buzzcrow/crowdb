// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "crowdb-tree/backend/block_page_store.h"

#include "backend_metadata.h"
#include "block_directory.h"
#include "btree/allocation_account.h"
#include "crowdb-tree/backend/backend_release_service.h"

#include <sys/stat.h>

#include <cerrno>
#include <cstring>
#include <iomanip>
#include <sstream>
#ifdef __linux__
#    include <linux/fs.h>
#    include <sys/ioctl.h>
#endif

namespace crowdb::tree
{
BlockPageStore::BlockPageStore(std::shared_ptr<detail::AllocationAccount> allocations,
                               std::shared_ptr<BlockPageStoreMedium> medium, uint32_t iu_size, bool is_block_device,
                               std::shared_ptr<BackendReleaseService> release)
    : allocations_(std::move(allocations)),
      release_(std::move(release)),
      medium_(std::move(medium)),
      iu_size_(iu_size),
      is_block_device_(is_block_device)
{
}

BlockPageStore::BlockPageStore(std::shared_ptr<detail::AllocationAccount> allocations, const std::string &dir,
                               uint32_t store_id, uint32_t group_id, uint64_t block_size, uint32_t iu_size,
                               std::shared_ptr<BackendReleaseService> release)
    : allocations_(std::move(allocations)),
      release_(std::move(release)),
      iu_size_(iu_size),
      is_block_device_(false),
      block_size_(block_size),
      store_id_(store_id),
      group_id_(group_id),
      dir_(dir.data(), dir.size(), MetadataAllocator<char>(allocations_))
{
    MetadataAllocator<BlockDirectory> allocator(allocations_);
    auto                             *directory = allocator.allocate(1);
    try {
        std::construct_at(directory, allocations_, release_);
    }
    catch (...) {
        allocator.deallocate(directory, 1);
        throw;
    }
    blocks_ = {directory, BlockDirectoryDeleter{allocations_}};
}

BlockPageStore::~BlockPageStore()
{
    blocks_.reset();
    medium_.reset();
    if (release_) {
        release_->stop_admission();
        if (!release_->externally_driven()) {
            // Standalone store destruction is an explicit host boundary. It
            // drains only queued work; retained readers keep their paid nodes.
            while (release_->snapshot().pending != 0) {
                (void)release_->pump(64);
            }
        }
    }
}

void BlockPageStore::operator delete(void *pointer) noexcept
{
    detail::BackendObjectStorage::destroy(pointer);
}

int BlockPageStore::fd() const
{
    auto *fm = dynamic_cast<FileMedium *>(medium_.get());
    return (fm != nullptr) ? fm->fd() : -1;
}

Status BlockPageStore::open(const std::string &path, uint32_t iu_size, std::unique_ptr<BlockPageStore> *out)
{
    std::shared_ptr<detail::AllocationAccount> allocations;
    auto                                       s = detail::AllocationAccount::create_unbound(&allocations);
    if (!s.ok()) {
        return s;
    }
    std::shared_ptr<BackendReleaseService> release;
    s = BackendReleaseService::create(allocations, SIZE_MAX / 2, {}, nullptr, nullptr, &release);
    if (!s.ok()) {
        return s;
    }

    struct OpenDrain
    {
        std::shared_ptr<BackendReleaseService> service;

        ~OpenDrain()
        {
            while (service->snapshot().pending != 0) {
                (void)service->pump(64);
            }
        }
    } open_drain{release};

    std::shared_ptr<FileMedium> medium;
    s = FileMedium::open_owned(path.c_str(), true, allocations, &medium, release);
    if (!s.ok()) {
        return s;
    }

    struct stat st{};
    if (::fstat(medium->fd(), &st) < 0) {
        return Status::io_error(std::string("fstat: ") + std::strerror(errno));
    }
    bool     is_block     = S_ISBLK(st.st_mode);
    uint32_t effective_iu = iu_size == 0 ? 4096 : iu_size;
    uint64_t probed_bytes = 0;

#ifdef __linux__
    if (is_block) {
        uint64_t bytes = 0;
        if (::ioctl(medium->fd(), BLKGETSIZE64, &bytes) == 0) {
            probed_bytes = bytes;
        }
        uint32_t sector_size = 0;
        if (::ioctl(medium->fd(), BLKSSZGET, &sector_size) == 0 && sector_size > 0) {
            effective_iu = sector_size;
        }
    }
#endif

    detail::BackendObjectStorage storage(sizeof(BlockPageStore), alignof(BlockPageStore), allocations);
    auto                        *store = ::new (storage.get())
        BlockPageStore(allocations, std::move(medium), effective_iu, is_block, std::move(release));
    (void)storage.release();
    store->capacity_ = probed_bytes;
    out->reset(store);
    return Status::Ok();
}

Status BlockPageStore::open_mem(uint32_t iu_size, std::unique_ptr<BlockPageStore> *out)
{
    std::shared_ptr<detail::AllocationAccount> allocations;
    auto                                       status = detail::AllocationAccount::create_unbound(&allocations);
    if (!status.ok()) {
        return status;
    }
    auto           medium       = std::allocate_shared<MemoryMedium>(MetadataAllocator<MemoryMedium>(allocations));
    const uint32_t effective_iu = iu_size == 0 ? 1 : iu_size;
    detail::BackendObjectStorage storage(sizeof(BlockPageStore), alignof(BlockPageStore), allocations);
    auto *store = ::new (storage.get()) BlockPageStore(allocations, std::move(medium), effective_iu, false);
    (void)storage.release();
    out->reset(store);
    return Status::Ok();
}

// ── Dump utility ───────────────────────────────────────────────────

Status dump_block_file(const std::string &path, uint32_t iu_size, std::string *out)
{
    std::unique_ptr<FileMedium> fm;
    Status                      s = FileMedium::open(path, false, &fm);
    if (!s.ok()) {
        return s;
    }

    uint64_t           file_size = fm->size();
    std::ostringstream oss;
    oss << "=== Block file: " << path << " ===\n";
    oss << "Size: " << file_size << " bytes, IU: " << iu_size << "\n\n";

    // Dump first 512 bytes as hex (anchor region)
    size_t               dump_len = std::min(static_cast<uint64_t>(512), file_size);
    std::vector<uint8_t> buf(dump_len, 0);
    size_t               got = 0;
    s                        = fm->pread_partial(0, buf.data(), dump_len, &got);
    if (!s.ok()) {
        return s;
    }

    oss << "--- First " << got << " bytes (anchor region) ---\n";
    for (size_t i = 0; i < got; i += 16) {
        oss << std::setfill('0') << std::hex << std::setw(8) << i << ": ";
        for (size_t j = 0; j < 16 && i + j < got; ++j) {
            oss << std::setw(2) << static_cast<unsigned>(buf[i + j]) << ' ';
        }
        oss << '\n';
    }
    oss << std::dec;

    *out = oss.str();
    return Status::Ok();
}

} // namespace crowdb::tree
