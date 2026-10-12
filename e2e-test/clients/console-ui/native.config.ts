// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
import { defineConfig } from '@playwright/test';
import shell from './shell.config';

export default defineConfig({
  ...shell,
  metadata: { fileBackedSync: true },
  timeout: 180_000,
  testMatch: ['**/71-s3-native.spec.ts'],
});
