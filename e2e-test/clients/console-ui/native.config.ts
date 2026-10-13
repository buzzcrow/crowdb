// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
import { defineConfig } from '@playwright/test';
import shell from './shell.config';

export default defineConfig({
  ...shell,
  metadata: { fileBackedSync: true },
  timeout: Number(process.env.CROWDB_WEB_E2E_TEST_TIMEOUT_MS || 180_000),
  testMatch: [
    '**/92-three-node-data.spec.ts',
    '**/22-kv-topology.spec.ts',
    '**/13-node-service-plans.spec.ts',
    '**/01-shell-ui-behaviors.spec.ts',
    '**/11-cluster-server-lifecycle.spec.ts',
    '**/11-cluster-diskdb-lifecycle.spec.ts',
    '**/71-s3-native.spec.ts', '**/51-chunk-capacity-disk.spec.ts',
    '**/51-chunk-capacity-inspector.spec.ts',
    '**/50-chunk-capacity-disk-group.spec.ts',
    '**/52-chunk-capacity-zone.spec.ts', '**/53-chunk-capacity-canvas.spec.ts',
    '**/53-chunk-ownership.spec.ts', '**/54-chunk-layout.spec.ts',
    '**/55-chunk-kv-catalog.spec.ts', '**/60-iceberg-catalog.spec.ts',
    '**/70-s3-object.spec.ts', '**/91-navigation-state.spec.ts',
  ],
});
