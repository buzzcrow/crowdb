// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
import config from './realBackend.config';
export default {
  ...config,
  globalSetup: undefined,
  globalTeardown: undefined,
  webServer: undefined,
  testMatch: '**/72-managed-native.spec.ts',
  testIgnore: [],
  use: { ...config.use, baseURL: process.env.CROWDB_WEB_E2E_BASE_URL },
};
