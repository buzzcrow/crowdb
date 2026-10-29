// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
import { icebergWorkflow } from '../fixtures/managedData/iceberg';
import { s3Workflow } from '../fixtures/managedData/s3';
import { datasetWorkflow } from '../fixtures/managedData/dataset';
import { capacityWorkflow } from '../fixtures/managedData/capacity';
import { existsSync, readFileSync, renameSync, writeFileSync } from 'node:fs';
import { randomUUID } from 'node:crypto';
import { join } from 'node:path';
import { test, expect } from '../fixtures/realBackend';
import { step } from '../fixtures/stepTimer';

test.use({ actionTimeout: 3000 });

// Baseline: 67.5s (2026-10-29), including field parameter, focus, scroll and Draft recovery assertions.
// Prior measurement: 41.6s (2026-10-27), shared CRUD/Parquet/recovery coverage.
// Previously 17.9s before the added namespace, S3 and Dataset authoring assertions.
test('Managed native KV, Iceberg, S3, Dataset and Chunk operations retain hardware boundaries', async ({ page, request }) => {
  await step('managed node authority and editing capabilities', async () => {
    await page.goto('/');
    await expect.poll(async () => (await (await request.get('/api/node/status')).json()).phase, { intervals: [100] }).toBe('active');
    await expect(page.getByRole('alert').filter({ hasText: 'Group 0 unavailable' })).toHaveCount(0);
    const capabilities = await request.get('/api/mode');
    expect(capabilities.ok(), await capabilities.text()).toBeTruthy();
    const actual = await capabilities.json();
    expect(actual).toEqual({ mode: 'node', readonly: false });
    await expect(page.getByLabel('Management token')).toHaveCount(0);
  });
  await step('managed user KV read', async () => {
    await page.getByTestId('domain-kv').click();
    await page.getByTestId('kv-store-select').selectOption('0');
    await page.getByTestId('kv-group-select').selectOption('1');
    const seeded = await request.post('/api/stores/0/groups/1/kv/put', { data: { key: 'console_native_ui_demo', value: 'native-value' } });
    expect(seeded.ok(), await seeded.text()).toBeTruthy();
    await page.getByLabel('Query key').fill('console_native_ui_demo');
    await page.getByRole('button', { name: 'Query', exact: true }).click();
    await expect(page.getByTestId('kv-get-result')).toHaveText('native-value', { timeout: 3000 });
    const deleted = await request.post('/api/stores/0/groups/1/kv/delete', { data: { key: 'console_native_ui_demo' } });
    expect(deleted.ok(), await deleted.text()).toBeTruthy();
  });

  await capacityWorkflow(page, request);
  await icebergWorkflow(page, request);
  await s3Workflow(page, request);
  await datasetWorkflow(page, request);
});

test('Node authority outage rejects mutations and marks retained observations stale', async ({ page, request }) => {
  const pid = Number(process.env.CROWDB_NATIVE_KV_PID);
  expect(pid, 'Run the owned release-container managed runner').toBeGreaterThan(0);
  await page.goto('/');
  await expect.poll(async () => (await (await request.get('/api/node/status')).json()).phase, { intervals: [100] }).toBe('active');
  await expect(page.getByRole('alert').filter({ hasText: 'Group 0 unavailable' })).toHaveCount(0);
  await page.clock.install();
  await signalService(pid, 'STOP');
  try {
    const observed = page.waitForResponse(response => response.url().endsWith('/api/node/status')
      && response.status() === 200, { timeout: 10000 });
    await page.clock.runFor(3001);
    expect((await (await observed).json()).phase).toBe('authority_unavailable');
    await expect(page.getByRole('alert').filter({ hasText: 'Group 0 unavailable' })).toHaveText('Group 0 unavailable · confirmed view is stale');
    // The API fixture covers the server's bounded authority check.
    const mutation = await request.post('/api/racks', { data: { id: 2, name: 'must-not-commit' } });
    expect(mutation.status()).toBe(502);
    await expect(page.getByRole('complementary', { name: 'Cluster tree sidebar' }).getByRole('button', { name: 'Candidate nodes', exact: true })).toBeVisible();
    await expect(page.getByRole('complementary', { name: 'Cluster tree sidebar' })
      .getByRole('button', { name: 'N-1', exact: true })).toBeVisible();
  } finally {
    await signalService(pid, 'CONT');
    await page.clock.resume();
  }
  await expect.poll(async () => (await (await request.get('/api/node/status')).json()).phase, { intervals: [100] }).toBe('active');
  await expect(page.getByRole('alert').filter({ hasText: 'Group 0 unavailable' })).toHaveCount(0);
  const racks = await request.get('/api/racks');
  expect(racks.ok(), await racks.text()).toBe(true);
  expect((await racks.json()).some((rack: { id: number }) => rack.id === 2)).toBe(false);

});

async function signalService(pid: number, signal: 'STOP' | 'CONT') {
  const directory = process.env.CROWDB_E2E_CONTROL;
  expect(directory, 'Owned container control is required').toBeTruthy();
  const id = randomUUID();
  const request = join(directory!, `${id}.request.json`);
  writeFileSync(`${request}.tmp`, JSON.stringify({ action: 'signal-service', node: 0, service: 'kv', pid, signal }));
  renameSync(`${request}.tmp`, request);
  const response = join(directory!, `${id}.response.json`);
  await expect.poll(() => existsSync(response), { intervals: [100] }).toBe(true);
  const result = JSON.parse(readFileSync(response, 'utf8'));
  expect(result.ok, JSON.stringify(result)).toBe(true);
  expect(result.result).toEqual({ pid, signal });
}
