<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R96: chunkdb — Console/CLI Integration

**Problem**: chunkdb has no management interface for operators. There is no way to view chunk status, query chunk distribution, or manage chunk lifecycle via web UI or CLI.

**Solution**: Add chunkdb panel to crowdb-web console with chunk overview, distribution visualization, and query capabilities. Implement crowdb-cli chunkdb subcommands for chunk management operations. Integrate with group-0 for topology display.

**Scope**: The complete Web UI, including Chunk listing and Strip placement,
is specified by [Console UI specification](../design/console/design-crowdb-console-ui.md). This item retains CLI
integration and shared operation reuse; do not implement a second Web panel.
