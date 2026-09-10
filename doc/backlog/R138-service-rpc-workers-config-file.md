<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R138: ops — Unified file-backed service configuration

## Problem

The four server processes do not share one configuration contract. KV,
diskdb, and chunkdb deserialize TOML through `crowdb-common`, while diskio is
configured only by a hand-written C++ CLI parser. Their server-side
`rpc_workers` setting is CLI-only, so an operator cannot express the complete
startup configuration in a file. Some CLI defaults also overwrite file values
even when the operator did not explicitly pass the option.

This fragments deployment tooling, makes precedence surprising, and leaves no
stable serialized input for a future group-0 configuration publisher. The
service architecture is described by the KV server, diskdb, chunkdb, and
diskio root designs indexed in `doc/doc_index.md`.

Concrete failures include starting any Rust server with only its TOML and
silently receiving the CLI worker default, and being unable to start diskio
from a reviewed configuration artifact at all.

## Solution

Use TOML as the startup configuration format for every server, with one merge
rule: compiled defaults < file < explicitly supplied CLI values.

1. Add server-side `rpc_workers` to the KV, diskdb, and chunkdb `[server]`
   schemas, validate it as non-zero, and wire the merged value to each RPC
   listener.
2. Add a complete diskio TOML loader and `--config` option. Organize its
   existing fields into `[server]`, `[engine]`, `[group0]`, `[metrics]`, and
   `[[disk]]`; load the file before applying CLI options.
3. Track one valid template beside each server and make local deployment write
   or pass those files without changing explicit CLI override behavior.
4. Document configuration ownership: libraries own reusable domain config
   types; binaries own process startup merging; `crowdb-common` owns Rust TOML
   loading, validation, watching, and diffing.
5. Keep all settings startup-bound in this requirement. A watcher may report
   changes, but a changed static value takes effect only after restart.

## Dependencies

None. The resulting serialized schema is an input to the deferred group-0
configuration requirement; this work does not depend on group 0.

## Acceptance

- Given each Rust service config omits `server.rpc_workers`, loading it uses
  that service's existing default; given the field is present, startup wires
  that value; given an explicit CLI flag, the CLI value wins. Invariant:
  default < file < explicit CLI. Integration test.
- Given `server.rpc_workers = 0`, each service rejects configuration before
  opening a listener. Invariant: every RPC server has at least one worker.
  Unit test.
- Given a complete diskio TOML, loading maps every supported section and disk
  entry; malformed TOML, wrong types, and invalid values fail with a useful
  error. Invariant: diskio never starts from a partially interpreted invalid
  file. Unit test.
- Given a diskio file plus explicit CLI options, startup retains file values
  for untouched fields and uses CLI values for touched fields. Invariant:
  merge precedence is identical across services. Unit test.
- Given each tracked template, its service loader accepts it and observes the
  documented worker default. Invariant: shipped examples match executable
  schemas. Unit test.
- Given local deployment for diskio and chunkdb, the generated launch spec
  includes a valid config path and the generated file includes
  `server.rpc_workers`. Invariant: deploy and restart use the same serialized
  configuration. Integration test.

Required gates:

- `pixi run -- cargo fmt --all -- --check`
- `pixi run rs-lint`
- `pixi run tree-lint`
- `pixi run test-common`
- `pixi run test-kv-core`
- `pixi run test-kv-server`
- `pixi run test-diskdb`
- `pixi run test-chunkdb`
- `pixi run test-diskio-ct`
- `pixi run test-console-shared`
