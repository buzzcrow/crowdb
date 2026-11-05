<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# CROWDB agent instructions

CROWDB is distributed storage for Iceberg tables, Dataset samples, and S3 objects. The codebase combines Rust services and libraries with C++ storage and transport through FFI.

## Finding the relevant code

- Start with the owning project and its tests: libraries in `lib/`, services and console applications in `app/`, deployment and monitor code in `container/`, and portable SDKs in `sdk/`. Follow direct dependencies as needed.
- Use [CONTRIBUTING.md](CONTRIBUTING.md) for language conventions and contribution workflow, [pixi.toml](pixi.toml) for executable tasks, and [tools/README.md](tools/README.md) for test selection and tooling. Read the relevant sections for the task.
- Use [doc/design/README.md](doc/design/README.md) for architecture context. Follow the agreed behavior spec for the task; use interfaces and tests to check its implementation. Report discrepancies rather than changing the spec to match existing code.

## Implementation constraints

- Rust denies unsafe code by default. Keep unsafe confined to existing scoped FFI or low-level modules; raise any new exception before adding it.
- Keep hot paths lock-free. Before adding a lock, obtain the user's agreement on contention, ordering, and complexity trade-offs. An existing explicit approval for the same change does not need to be repeated.
- Add Rust tests under each crate's `tests/`; crates with `test-util` enable it for their own tests through self dev-dependencies.
- For visible console changes, agree on the behavior with the user, update the UI spec and its ASCII layout, then implement. Do not introduce behavior outside the agreed spec. Each visible change needs focused test or E2E coverage.

## Commands and verification

- Run every build, test, lint, and executable through `pixi run`. Use a 60-second shell timeout by default; run longer or hang-prone work in the background and poll it.
- Before committing, run the relevant gates: Rust formatting and clippy, formatting and `tree-lint` for changed C++, and affected tests. Use the tasks in `pixi.toml`; preserve the normal commit hooks. Fix ordinary failures up to three times and report confirmed pre-existing failures.
- Show complete command output without filtering away errors. On test or CI failure, print the failing case and its error. Enable `CROWDB_E2E_PRINT_SERVICE_ERRORS=1` in CI or debugging to include container service file errors; it defaults off locally, and artifacts retain complete logs in either mode.
- Playwright uses an installed system browser. Do not install one locally.

## Working tree and commits

- Preserve existing work. Inspect the diff and stage only changes belonging to the task. Never hard-reset, revert, or restore files from an older commit without explicit approval; use a stash or temporary branch when isolation is needed.
- Commit interactive work only when the user asks, as one coherent commit. Keep unrelated changes out of it.
- Commit messages are single-line subjects without bodies, trailers, document references, or requirement numbers. Code comments also omit document references and requirement numbers.

## Documentation

- Public design articles live in `doc/design/`, use English, and explain architecture, capabilities, design choices, and advantages. Update them only when explicitly requested. Keep detailed implementation flows and development progress out of these overviews; keep usage and operational instructions in the owning README.
- Public documents must be independently usable with repository files or public sources. Use actual project names and established technical terms. Explain mechanisms and benefits directly; remove generic praise, repeated conclusions, and invented terminology. Claims about implemented features or measured performance need evidence.
- Present access models in the order Iceberg, Dataset, S3, preserving that relative order for subsets. Apply this to prose, lists, navigation, and diagrams.
- Keep a single component article directly under `doc/design/`; create a subdirectory only for several related articles. Update the public index when articles move, appear, or disappear.
- Share diagrams and screenshots through `doc/assets/`. Prefer editable SVG for technical diagrams and real UI screenshots for product views. Public diagrams do not need ASCII equivalents.
- Keep paragraphs and list items on one source line and let the editor wrap them. Preserve blank lines and structural breaks in lists, tables, and code. Use bullets for parallel rules and tables for comparisons or scope lookup; the public index uses tables. Align table columns in raw Markdown.

## Chunk terminology

- **Repo chunk** is a discussion term for user-data chunks, including IcebergTable, Dataset, and S3 chunks. It excludes system chunks such as WAL and tree storage.
- Code uses the specific chunk types. Do not introduce or retain a generic `Repo` chunk type based on this discussion term.
