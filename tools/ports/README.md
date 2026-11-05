<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Default ports

All service port values are defined in
[ports.rs](../../lib/crowdb-protocol/src/port/ports.rs). Rust callers import
its constants or `ServicePort`. Explicit deployment inputs override defaults.

After editing the defaults, run:

```sh
pixi run python tools/ports/generate.py
pixi run python tools/ports/generate.py --check
```

The generator produces Python, TypeScript and C++ constants, plus container
profiles, Compose examples, Dockerfile and sample configuration. Edit the
adjacent `*.ports.in` source for those files; `@WEB_BASE@` references a constant
and `@WEB_BASE+1@` adds a node offset. Generated files are checked in so they
can be used directly. OCI staging and the packaging tests reject stale outputs.

Each active service listener owns a 1000-port range. Three host-network nodes
with one listener of each kind use offsets 0, 1 and 2. Multiple Paxos-KV stores
need distinct RPC ports from the node's configured pool; offsets must account
for that pool. No range is reserved for a nonexistent listener.
