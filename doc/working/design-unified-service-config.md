<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Unified File-Backed Service Configuration (R138)

This draft implements
[`R138`](../backlog/R138-service-rpc-workers-config-file.md) consistently with
the indexed KV server, diskdb, chunkdb, and diskio root designs. It creates the
local serialized contract needed by deferred
[`R139`](../backlog/R139-group0-service-config.md); group-0 publication is not
part of this implementation.

## 1. Configuration Contract

Each daemon owns one typed root config and accepts TOML at startup. Common
section names describe process roles rather than implementation language:

- `[server]`: listener identity and server-side RPC resources.
- Client connection pools remain named by dependency, such as
  `kv_pool_size`, `kv_rpc_workers`, and `diskdb_rpc_workers`.
- Domain sections remain owned by their service (`[paxos]`, `[wal]`,
  `[storage]`, `[placement]`, `[engine]`, and similar).

The merge is ordered and performed once before listener construction:

```text
typed compiled defaults -> TOML values -> explicitly present CLI options
                         -> complete validation -> runtime construction
```

CLI fields that participate in this merge use `Option<T>` or explicit
presence tracking. A parser default is not an explicit override.

Unknown TOML keys are tolerated by the Rust serde loaders and diskio parser for
forward compatibility. Known keys with a wrong type, malformed syntax, or an
invalid value reject the whole file.

## 2. Rust Services

`crowdb-kv::common::config::ServerConfig`,
`crowdb_diskdb::ddb_config::ServerConfig`, and
`crowdb_chunkdb::chunkdb_config::ServerConfig` gain:

```rust
pub rpc_workers: u32
```

The service-specific default remains 2. Each `BaseConfig::validate`
implementation rejects zero. The binary's `--rpc-workers` becomes optional;
when absent, listener construction reads `config.server.rpc_workers`.

KV keeps its root-derived paths and optional `--config`. diskdb and chunkdb
keep their required file paths and watcher behavior. `rpc_workers` is static:
watchers may log it as changed, but the active listener retains its startup
value until restart.

## 3. Diskio TOML Loader

`DioConfig::load_file(path, out, err)` uses toml++ and overlays a validated
TOML document on a default `DioConfig`. `parse_args` first locates and loads the
single optional `--config` file, then performs its existing left-to-right CLI
parse so explicit flags override file values regardless of argument order.

The schema is:

```toml
[server]
bind_address = "127.0.0.1"
listen_port = 13000
rpc_workers = 4
node_id = 0
dummy_disk_type = "null"
o_direct = true

[engine]
thread_pool_size = 4
sq_entries = 256

[group0]
kv_seeds = []
instance_id = 0
rack_id = 0
disk_group_id = 0
sync_interval_ms = 5000
auto_discover_disks = false

[metrics]
log_dir = "log"
interval_secs = 5

[[disk]]
id = "1"
path = ""
zone_capacity = 1099511627776
```

Disk identifiers stay strings because their existing syntax supports a
128-bit `high:low` representation. One TOML disk entry creates the same single
zone currently produced by one `--disk`; repeated `[[disk]]` entries preserve
order. Fault injection uses optional `fault_latency_min_ms`,
`fault_latency_max_ms`, and `fault_error_rate` keys under `[server]`.

Loading is transactional: parsing occurs into a temporary config, and `out` is
assigned only after the document has been completely decoded. Final
`DioConfig::validate` remains the authority for cross-field constraints.

## 4. Templates and Deployment

Tracked templates live at:

- `app/crowdb-kv-server/conf/crowdb_kv_server_config.toml`
- `app/crowdb-diskdb/conf/crowdb_diskdb_config.toml`
- `app/crowdb-chunkdb/conf/crowdb_chunkdb_config.toml`
- `app/crowdb-diskio/conf/crowdb_diskio_config.toml`

Console local deployment continues generating node-specific files where
addresses and group-0 seeds vary. Generated chunkdb and diskio files include
`server.rpc_workers`; launch specs pass `--config`. An explicit deploy request
worker setting may remain a CLI override and therefore wins.

## 5. Failures and Fallback

Missing explicit files, malformed TOML, wrong known-field types, duplicate
fields rejected by TOML, and failed validation terminate startup before bind.
When `--config` is absent on KV or diskio, compiled defaults remain usable.
There is no fallback from an invalid named file to defaults because that would
hide operator mistakes.

## Scope

- `lib/crowdb-kv/src/common/config.rs`: KV server field, default, validation,
  and template assertion.
- `app/crowdb-kv-server/src/{cli.rs,main.rs,store_registry.rs}`: precedence
  and listener wiring.
- `app/crowdb-diskdb/src/{ddb_config.rs,main.rs}`: field, validation,
  precedence, and wiring.
- `app/crowdb-chunkdb/src/{chunkdb_config.rs,main.rs}`: field, validation,
  precedence, and wiring.
- `app/crowdb-diskio/{CMakeLists.txt,src/dio_config.*,conf/*,tests/*}`: TOML
  dependency, loader, CLI merge, template, and tests.
- `app/*/conf/*.toml`: tracked examples.
- `lib/crowdb-console-shared/src/lifecycle.rs`: generated file and launch
  wiring.
- `doc/design/config/*`, component root designs, `doc/doc_index.md`, and the
  user guide: permanent configuration contract and operations.

## Complexity

Medium. Rust changes are direct typed plumbing. Diskio requires a complete,
transactional TOML mapping across C++ types and careful preservation of CLI
behavior. Deployment tests span generated files and restart launch specs.

## Test Design

1. Rust config unit tests deserialize omitted, explicit, and zero
   `rpc_workers`; assert defaults, values, and validation respectively.
2. Rust CLI/integration tests merge a file with absent and explicit CLI worker
   options; assert the final listener input follows precedence.
3. Diskio C++ unit tests load a full file and assert every scalar, optional,
   vector, disk ID, and zone; then cover malformed input, wrong types, zero
   workers, missing files, and transactional output preservation.
4. Diskio C++ CLI tests combine file and CLI values in both argument orders;
   assert explicit CLI always wins while untouched file fields survive.
5. Template tests load all four tracked files and assert worker defaults.
6. Console lifecycle tests inspect generated chunkdb/diskio TOML and persisted
   restart arguments; assert config paths and worker precedence survive.

Each test preserves the invariant that only a complete validated candidate can
reach runtime construction.

## Module Structure

```text
doc/design/config/
  design-crowdb-config.md        # cross-service configuration contract
app/
  crowdb-*/conf/*.toml           # typed startup examples
  crowdb-diskio/src/dio_config.* # C++ TOML load + CLI overlay
lib/
  crowdb-common/rust/src/config.rs # shared Rust mechanics
  crowdb-kv/src/common/config.rs   # KV-owned domain schema
  crowdb-console-shared/src/lifecycle.rs # generated node configs
```

## Config Extensions

`server.rpc_workers` is a positive integer. Defaults are KV 2, diskdb 2,
chunkdb 2, and diskio 4. It is static in every service.

## Server Wiring

Every server constructs its RPC listener only after the final merged config is
validated. Listener constructors receive the merged `server.rpc_workers`; no
second default exists at the registry or wiring layer.
