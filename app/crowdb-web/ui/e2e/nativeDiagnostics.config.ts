// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
import config from './managedNative.config';
export default {
  ...config,
  grepInvert: undefined,
  use: { ...config.use, screenshot: 'on' as const },
  testMatch: ['**/91-navigation-state.spec.ts', '**/53-chunk-ownership.spec.ts', '**/11-cluster-server-lifecycle.spec.ts', '**/55-chunk-kv-catalog.spec.ts', '**/52-chunk-capacity-zone.spec.ts', '**/60-iceberg-catalog.spec.ts', '**/71-s3-native.spec.ts'],
  grep: /native diagnostics|S3 native multipart/,
};
