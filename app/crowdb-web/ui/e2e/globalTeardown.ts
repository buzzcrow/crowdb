// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
import { consoleBaseURL } from './fixtures/realBackend';
import { resetAll } from './fixtures/consoleSetup';

export default async function globalTeardown() {
  const start = Date.now();
  try {
    await resetAll(consoleBaseURL());
  } finally {
    console.log(`[TEARDOWN] owned console resources: ${Date.now() - start}ms`);
  }
}
