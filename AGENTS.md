<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# CROWDB

Distributed storage platform: Paxos consensus, per-key slots, WAL durability, crowdb-tree engine, and a disk-block allocator. Rust workspace with C++ storage and transport exposed through FFI.

## Terminology

- **Repo chunk** is the discussion term for any user-data chunk, including IcebergTable, future Dataset, and S3 chunks. It excludes system chunks such as WAL and tree storage chunks.
- Implementations distinguish these user-data uses with specific chunk types; do not retain or introduce a generic `Repo` chunk type. The discussion term does not imply a shared implementation type.

## Project rules

- Run every build, test, lint, and executable through `pixi run`.
- `unsafe_code = deny` by default. Keep unsafe confined to existing scoped FFI or low-level modules; raise any new exception before adding it.
- Keep hot paths lock-free. Before adding a lock, stop and ask the user with the contention, ordering, and complexity trade-off.
- Add Rust tests under each crate's `tests/`; crates with `test-util` enable it for their own tests through self dev-dependencies.
- Preserve existing work. Never hard-reset, revert, or restore files from an older commit without explicit approval. Use stash or a temporary branch.
- Ordinary interactive work is committed only when asked, as one coherent commit.
- Commit messages are single-line subjects without bodies, trailers, doc references, or requirement numbers. Code comments also omit those references.
- Before committing, run the relevant gate through pixi: Rust fmt and clippy, changed C++ format/tree-lint, and affected tests. Fix ordinary failures up to three times; report confirmed pre-existing failures.
- Playwright uses an installed system browser. Do not install one locally.
- Use 60-second shell timeouts by default. Start hang-prone commands in the background and poll. Show complete output; do not hide errors with filters.
- On test or CI failure, print the failing case and test error to the console. Enable `CROWDB_E2E_PRINT_SERVICE_ERRORS=1` in CI or when debugging to also print container service file errors. This extra output defaults off locally; artifacts retain complete logs in either mode.
- Markdown is primarily read raw. Prefer bullets; use tables for real comparisons only. `doc/design/README.md` always uses tables. When a table is used, pad columns with spaces so the raw markdown aligns visually.
- Console UI changes follow the agreed UI behavior spec. Before changing visible behavior, update the UI spec and its ASCII layout after agreement with the user; only then implement the UI change. Do not unilaterally change the spec or introduce UI behavior that is absent from the agreed spec. Each visible UI change needs focused test or E2E coverage.

## Documentation

- Present CROWDB access models in the order Iceberg, Dataset, S3 in prose, lists, navigation, and diagrams. When only a subset is relevant, preserve that relative order.
- Keep each prose paragraph and list item on one source line; do not hard-wrap at a fixed column width. Let the editor or renderer wrap to the available width. Preserve blank lines between paragraphs and structural line breaks in tables, code blocks, and nested lists.
- Use `doc/design/README.md` to locate public documentation.
- `doc/design/` explains overall architecture, capabilities, and design advantages for users and AI readers. Update it only when explicitly requested; routine development does not automatically refresh these overviews.
- Public overviews do not replace detailed behavior specs. Keep component usage and operational instructions in the owning README. Public documentation must be independently usable from this repository.
- Keep a single component article directly under `doc/design/`; create a subdirectory only when several related articles need it.
- Store shared diagrams, screenshots, and other documentation media in `doc/assets/`. README and design articles reference the same assets. Prefer editable SVG for technical diagrams; use real UI screenshots for product views. Public design diagrams do not need ASCII equivalents.
- Write public design articles in English. Explain concrete mechanisms and user benefits with established terminology; keep detailed implementation flows and development progress out of these overviews.
