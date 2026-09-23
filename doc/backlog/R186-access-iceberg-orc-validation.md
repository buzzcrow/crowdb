<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R186: access server / Iceberg — Selected ORC validation

## Status

Deferred by user decision until the Parquet catalog path is functional. ORC is a
standard Iceberg file format, but server-side ORC decoding is not a prerequisite
for implementing the REST Catalog. Initial selected-file validation rejects ORC
explicitly rather than representing an unchecked file as validated.

## Problem

Native FileIO can retain immutable ORC bytes, but container recognition does not
prove selected schema, row counts or delete semantics. Accepting a container hint
as proof would violate the canonical-file contract in
[the root design](../design/access-server/iceberge/design-crowdb-iceberg.md).
An official client configured to write ORC needs a distinct, tested validation
capability rather than an implicit fallback to Parquet checks.

## Solution

1. Extend `crowdb-access-iceberg::file` with bounded canonical ORC decoding. Use
   the official ORC protobuf and pinned Iceberg mappings and SDK as authorities;
   decode compression framing correctly and reject unsupported codecs or
   encryption explicitly. Independently limit encoded/decoded bytes, protobuf
   work, type depth/count and stripe count.
2. Extend `crowdb-access-iceberg::manifest` selected-file validation with field-ID,
   historical schema, row-count and supported delete checks. Bind the manifest's
   semantic kind without changing immutable file authority. Never trust hints
   instead of canonical bytes.
3. Integrate ORC into complete selected snapshot validation only after its format
   gates pass. Failure or cancellation publishes no validation result or head.
   Keep unsupported selected ORC explicit before this capability lands; ordinary
   immutable upload is not a table-selection or commit proof.

## Dependencies

- R180 supplies immutable files and canonical range reads.
- R181/R182 supply trusted table metadata and selected snapshot validation.
- R184 adds ORC to its tested capability profile after this requirement passes;
  initial Parquet-only acceptance does not depend on this requirement.
- R185 caches are optional; uncached canonical reads remain correct.

## Acceptance

- Given official SDK ORC fixtures and supported compression variants, when
  canonical metadata is decoded, assert schemas and row counts match the writer.
  Invariant: canonical authority. Integration test.
- Given malformed framing, excessive expansion, deep types, invalid stripes,
  encryption or unsupported codecs, when decoded, assert bounded failure with no
  successful proof. Invariant: independent resource limits. Unit test.
- Given data and delete manifests with historical schemas and false counts, when
  selected, assert compatible files pass and mismatched kind, identity, fields or
  counts fail. Invariant: manifest-to-file binding. Integration test.
- Given a complete snapshot containing ORC and a failure or cancellation during
  validation, when publication is attempted, assert no head advances; before ORC
  support, assert selection fails explicitly. Invariant: fail-closed publication.
  Integration test.

Required gates:

- `pixi run -- cargo test -p crowdb-access-iceberg --all-targets`
- `pixi run -- cargo test -p crowdb-access-server --all-targets`
- `pixi run rs-fmt-check`
- `pixi run rs-lint`
