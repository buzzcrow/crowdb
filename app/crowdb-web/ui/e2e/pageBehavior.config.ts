// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
import config from './realBackend.config';

// Historical configuration retained while scenarios migrate to native fixtures.
// The shared fixture forbids interception; these cases cannot pass using mocks.
export default {
  ...config,
  testMatch: ['**/54-chunk-layout.spec.ts', '**/55-chunk-kv-catalog.spec.ts',
    '**/60-iceberg-catalog.spec.ts', '**/70-s3-object.spec.ts'],
};
