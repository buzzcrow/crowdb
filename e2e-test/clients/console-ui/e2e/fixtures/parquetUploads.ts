// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
import { execFile } from 'node:child_process';
import { promisify } from 'node:util';
import { resolve } from 'node:path';
import { step } from './stepTimer';

export async function parquetUploads() {
  const result = await step('Iceberg: generate local Parquet upload inputs', () => promisify(execFile)('pixi', ['run', '-e', 'iceberg-e2e', 'python', resolve('e2e/fixtures/parquetFiles.py')]));
  if (result.stderr) console.log(result.stderr);
  const inputs = JSON.parse(result.stdout) as Record<string, string>;
  return (name: string, key: string) => ({ name, mimeType: 'application/octet-stream', buffer: Buffer.from(inputs[key], 'base64') });
}
