// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#pragma once

#include "chunk_page_store.h"

namespace crowdb::tree::detail
{

[[nodiscard]] uint32_t reference_segment_checksum(const ChunkReferenceSegmentImage &segment);
[[nodiscard]] uint32_t manifest_checksum(const ChunkManifest &manifest);

[[nodiscard]] std::vector<uint8_t> encode_reference_segment(const ChunkReferenceSegmentImage &segment);
[[nodiscard]] std::shared_ptr<const ChunkReferenceSegmentImage>
decode_reference_segment(const std::vector<uint8_t> &bytes);
[[nodiscard]] std::vector<uint8_t>                 encode_manifest(const ChunkManifest &manifest);
[[nodiscard]] std::shared_ptr<const ChunkManifest> decode_manifest(const std::vector<uint8_t> &bytes);

} // namespace crowdb::tree::detail
