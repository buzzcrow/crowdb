<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Dataset v1 contract fixtures

- The OpenAPI source is `doc/design/access-server/dataset/openapi-crowdb-dataset-v1.yaml`.
  It uses JSON syntax, which is valid YAML and requires no YAML library to inspect.
- Control fixtures use fixed synthetic IDs; they do not identify deployed data.
  `publication.json` stores `hello` in the `value` field of opaque ID `sample`.
- `batch.bin` contains the same projected bytes in the binary batch protocol.
  Truncated, corrupted and unsupported-version variants must never produce
  successful completion. The Rust SDK tests every transport split position.
- Fixtures are test inputs, not production dependencies. Server and SDK encode
  and decode their own models independently; the SDK never imports authority
  or persisted bincode models.
- No previous released public Dataset wire version exists yet. These are the
  first frozen fixtures; released-server compatibility remains pending.
