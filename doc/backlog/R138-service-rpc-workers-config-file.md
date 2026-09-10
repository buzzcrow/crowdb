<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R138: ops — Per-service rpc_workers in config files

## Status

Open. Investigation complete; implementation not started.

## Problem

**Current behavior + impact**

Each service's own `rpc_workers` (the crowdb-rpc I/O worker count for the
server's RPC listener) is CLI-only. It is not in any config file:

- `crowdb-kv-server`: `--rpc-workers` CLI (default 2). The TOML
  `ServerConfig` (`lib/crowdb-kv/src/common/config.rs:70`) has
  `peer_pool_size`, `enable_nagle`, `quickack`, `event_write`,
  `send_queue_capacity` — but no `rpc_workers`. The CLI arg flows into
  `KvStoreRegistry::rpc_workers` (`store_registry.rs:52`) and is applied
  to each `PxKvStore` at construction.
- `crowdb-diskdb`: `--rpc-workers` CLI (default 2). The TOML
  `ServerConfig` (`app/crowdb-diskdb/src/ddb_config.rs:41`) has
  `kv_pool_size` and `kv_rpc_workers` (both for the KV *client*), but the
  server's own `rpc_workers` is CLI-only.
- `crowdb-chunkdb`: `--rpc-workers` CLI (default 2). Same pattern — TOML
  `ServerConfig` (`app/crowdb-chunkdb/src/chunkdb_config.rs:222`) has
  `kv_rpc_workers` and `diskdb_rpc_workers` (both client-side), but the
  server's own `rpc_workers` is CLI-only.
- `crowdb-diskio`: `--rpc-workers` CLI (default 4). No config file at
  all — `DioConfig` (`app/crowdb-diskio/src/dio_config.h:34`) is a C++
  struct populated purely from `parse_args`.

Operators cannot set `rpc_workers` via config file; they must pass CLI
flags. The local-deploy path (`cluster local-deploy`) passes
`--rpc-workers` / `--diskio-rpc-workers` through, but a bare binary
started from a config file has no way to override the default without
CLI args.

**Client-side rpc_workers (already in config files, for reference):**

- `crowdb-diskdb` TOML: `server.kv_rpc_workers` (KV client workers)
- `crowdb-chunkdb` TOML: `server.kv_rpc_workers`, `server.diskdb_rpc_workers`
- `crowdb-kv-server` TOML: `server.peer_pool_size` (inter-server RPC
  pool, not worker count)

These are client/peer transport workers, distinct from the server's own
listener worker count. They are out of scope for R138.

## Solution

Add `rpc_workers` to each service's config file schema, with the
service's own code default as fallback when the field is absent:

- `crowdb-kv-server`: add `rpc_workers` to `ServerConfig` in
  `lib/crowdb-kv/src/common/config.rs`. Default 2. The CLI `--rpc-workers`
  overrides the file value (existing CLI-override pattern).
- `crowdb-diskdb`: add `rpc_workers` to `ServerConfig` in
  `app/crowdb-diskdb/src/ddb_config.rs`. Default 2. CLI overrides.
- `crowdb-chunkdb`: add `rpc_workers` to `ServerConfig` in
  `app/crowdb-chunkdb/src/chunkdb_config.rs`. Default 2. CLI overrides.
- `crowdb-diskio`: add a TOML config file loader to the C++ app. Default
  4. CLI overrides. This is the largest piece — diskio has no config file
  today; adding one means a new `--config` flag, a TOML parser in C++
  (or a small Rust helper), and a `conf/crowdb_diskio_config.toml`
  template tracked under `app/crowdb-diskio/conf/`.

Update the tracked config templates:

- `app/crowdb-diskdb/conf/crowdb_diskdb_config.toml` — add `rpc_workers`
  to `[server]`.
- The chunkdb config is generated inline by `lifecycle.rs:1152`; add
  `rpc_workers` there.
- `app/crowdb-kv-server/conf/` does not exist today; the tracked-config
  test (`lib/crowdb-kv/src/common/config.rs:657`) references it but the
  directory is absent. Either create the template or document that
  kv-server config is optional/first-boot only.
- `app/crowdb-diskio/conf/crowdb_diskio_config.toml` — new file.

Update `lifecycle.rs` deploy paths to copy the tracked template into
the workspace `conf/` dir (diskio) and to write `rpc_workers` into the
generated chunkdb TOML.

## Scope

- Add `rpc_workers` to 3 Rust config structs + 1 C++ config.
- Add a config file to diskio (new `--config` flag, TOML parse, template).
- Update tracked config templates and inline-generated configs.
- Update deploy paths in `lifecycle.rs` to copy/write the field.
- Update tests that assert on config defaults/templates.
- No lib changes to load files — lib only receives config structs; the
  app calls `crowdb_common::config::load_from_file`.

## Out of scope

- Client-side rpc_workers (kv_rpc_workers, diskdb_rpc_workers) — already
  in config files.
- peer_pool_size — already in config files.
- Live reload of rpc_workers — it is a static bind-at-startup field.

## Dependencies

None. Pure config plumbing.
