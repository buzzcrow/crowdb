<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R191: access server — Protocol-owned chunk storage

#### Problem

The combined `crowdb-access-server` starts S3 and Iceberg in one process, but their object writes both allocate `Repo` chunks. `crowdb-chunk-client` hardcodes that type in small-write allocation and large-write prefetch. The shared `small_write` configuration also makes protocol-specific admission and EC settings unclear. Runtime storage wiring and some Iceberg file handling live in the application crate. This obscures ownership when S3 and table traffic have different scaling and placement needs. See [access architecture](../design/access-server/design-crowdb-access-server.md), [chunk IO](../design/chunkio/design-crowdb-chunkio.md), and [chunk ID layout](../design/chunkdb/design-crowdb-chunkdb.md).

#### Solution

The access executable owns process configuration, listener startup, logging, health, and shutdown. S3 and Iceberg each own their metadata, chunk client construction, write admission, and file/object storage behavior in `crowdb-access-s3` and `crowdb-access-iceberg`. Both run in the same process, but foreground small-write pools and large-write preparation are independent. They may share protocol-neutral transport facilities only where this does not couple admission, failure, or shutdown.

1. Extend the canonical chunk type in `crowdb-protocol`, its FlatBuffer schema, Rust/C++ mappings, and `crowdb-chunkdb` allocation/validation with distinct S3 and Iceberg table values. Preserve all existing numeric values and reads of legacy `Repo` chunks. The chunk ID prefix and the stored `chunk_type` field must agree; an invalid combination fails allocation without publishing a chunk.
2. Make `crowdb-chunk-client` small-write allocation and large-write prefetch take the owning protocol's chunk type. Keep an independently elastic small-write pool per protocol. The type is fixed for one pool or prepared large-write session, including on-demand allocation, rotation, mirror-to-EC conversion, repair, and cleanup.
3. Move S3 foreground client wiring and policy selection from `app/crowdb-access-server` into `crowdb-access-s3`. Move Iceberg foreground client wiring and file-storage policy selection into `crowdb-access-iceberg`. Keep S3 metadata in its S3 library and table/catalog metadata in its Iceberg library. Preserve the separate Iceberg GC client pool when GC is enabled.
4. Give S3 and Iceberg their own small-write and large-write EC, memory, and prefetch settings. Do not require the two EC schemes to match. Large-write EC remains a policy of each write/strip; this requirement does not force all future strips in a chunk to use one EC scheme. Keep existing configuration usable with explicit migration/default rules.
5. Keep the default executable and container startup as one process with both listeners. A failure in either listener or its owned storage path must terminate the combined service and drain both pools. Monitor health must cover both listeners.

#### Dependencies

- Builds on the combined access process and container profile. R189's ecosystem tests can continue against the existing `Repo` type until this change lands.
- Uses existing chunk ID prefix and per-strip EC support. If protocol-specific types cannot yet be allocated, retain `Repo` writes and do not claim type isolation.
- R168 and R169 reclamation must accept the new S3 type and historical `Repo` objects; Iceberg GC must likewise recognize the Iceberg type and historical records.

#### Acceptance

- Given historical `Repo` S3 and Iceberg references, start the updated service and read both without migration; existing IDs and stored type values remain valid. Integration test.
- Given S3 small and large writes, allocate, rotate, read, and reclaim chunks; every new chunk ID prefix and stored type is S3, including the on-demand and conversion paths. Integration test.
- Given Iceberg small and large file writes, allocate, rotate, read, and reclaim chunks; every new chunk ID prefix and stored type is Iceberg table, including the on-demand and conversion paths. Integration test.
- Given mismatched prefix and stored type, submit an allocation; it fails without a durable chunk. Integration test.
- Given S3 load while Iceberg is idle, scale S3's small-write pipelines out and back in; Iceberg's pool count and admission budget remain independent, and the reverse holds. Integration test.
- Given a protected production deployment with different S3 and Iceberg EC, prefetch, and memory settings, start both listeners and write/read both small and large objects; each allocation uses its own settings. E2E test.
- Given the explicit single-node test deployment, start both listeners and write/read both small and large objects; both protocols use one-copy mirror strips while retaining separate pools and chunk types. E2E test.
- Given one listener or storage path fails, the combined process exits, drains both owned pools, and the monitor reports the service unhealthy. E2E test.
- Given an Iceberg GC run while foreground S3 and Iceberg writes continue, GC retains its separately budgeted client and cannot consume their pool admission. Integration test.

Run `pixi run rs-fmt-check`, `pixi run cargo clippy -p crowdb-access-server -p crowdb-access-s3 -p crowdb-access-iceberg -p crowdb-chunk-client -p crowdb-protocol --all-targets -- -D warnings`, `pixi run cargo test -p crowdb-chunk-client`, `pixi run cargo test -p crowdb-access-server`, and `pixi run test-single-node-container` (or the repository's current container acceptance task). Run `pixi run tree-lint` and `pixi run test-cpp` for changed C++ mappings.
