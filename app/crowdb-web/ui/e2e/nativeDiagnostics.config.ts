// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
import config from './managedNative.config';
export default {
  ...config,
  grepInvert: undefined,
  use: { ...config.use, screenshot: 'on' as const },
  testMatch: ['**/55-chunk-kv-catalog.spec.ts', '**/52-chunk-capacity-zone.spec.ts'],
  grep: /native diagnostics/,
};
