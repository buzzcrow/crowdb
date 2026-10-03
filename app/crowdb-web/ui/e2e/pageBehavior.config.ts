// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
import config from './realBackend.config';

// Deterministic domain rendering, pagination and failure contracts. Native
// deployment/data acceptance remains in realBackend and managedNative.
export default {
  ...config,
  testMatch: ['**/54-chunk-layout.spec.ts', '**/55-chunk-kv-catalog.spec.ts',
    '**/60-iceberg-catalog.spec.ts', '**/70-s3-object.spec.ts'],
};
