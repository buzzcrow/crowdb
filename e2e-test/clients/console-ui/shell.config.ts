// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
import { defineConfig } from "@playwright/test";
import bridge from "./bridge.config";

if (!process.env.CROWDB_WEB_E2E_BASE_URL || !process.env.CROWDB_WEB_E2E_OUTPUT) {
  throw new Error("Shell browser requires owned release Web endpoints and artifacts");
}

export default defineConfig({ ...bridge, testMatch: ["**/00-shell.spec.ts", "**/10-cluster-rack-node.spec.ts", "**/13-todo-ui-behavior.spec.ts"] });
