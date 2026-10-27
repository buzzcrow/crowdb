// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
import { resourceAction } from '../fixtures/resourceMenus';
import { existsSync, readFileSync, renameSync, writeFileSync } from 'node:fs';
import { randomUUID } from 'node:crypto';
import { join } from 'node:path';
import { test, expect } from '../fixtures/realBackend';
import { step } from '../fixtures/stepTimer';

test.use({ actionTimeout: 3000 });

// Baseline: 6.0s (2026-10-04); actual monitor and all native services.
test('Managed native KV, Iceberg, S3 and Chunk operations retain hardware boundaries', async ({ page, request }) => {
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

  await step('managed Iceberg schema and owned cleanup', async () => {
    await page.getByTestId('domain-iceberg').click();
    await expect(page.getByLabel('Catalog write token')).toHaveCount(0);
    const namespace = `console_demo_${Date.now()}`;
    const tree = page.getByRole('navigation', { name: 'Iceberg tree' });
    await resourceAction(page, tree.getByRole('button', { name: 'Catalog', exact: true }), 'Create namespace…');
    await page.getByLabel('Namespace name', { exact: true }).fill(namespace);
    await page.getByRole('button', { name: 'Create namespace', exact: true }).click();
    await tree.getByRole('button', { name: namespace, exact: true }).click();
    await resourceAction(page, tree.getByRole('button', { name: namespace, exact: true }), 'Create table…');
    await page.getByLabel('Table name', { exact: true }).fill('example');
    await page.getByRole('button', { name: 'Create table', exact: true }).click();
    await tree.getByRole('button', { name: 'example', exact: true }).click();
    await expect(page.getByRole('button', { name: 'Refresh table' })).toBeVisible({ timeout: 3000 });
    await page.getByRole('button', { name: 'Schema', exact: true }).click();
    await expect(page.getByRole('table', { name: 'Table schema', exact: true })).toContainText('long');
    await resourceAction(page, tree.getByRole('button', { name: 'example', exact: true }), 'Drop table…');
    await page.getByRole('button', { name: 'Drop table', exact: true }).click();
    await resourceAction(page, tree.getByRole('button', { name: namespace, exact: true }), 'Drop namespace…');
    await page.getByRole('button', { name: 'Drop namespace', exact: true }).click();
    await expect(tree).not.toContainText(namespace);
  });

  await step('managed S3 preview and actual block placement', async () => {
    await page.getByTestId('domain-s3').click();
    await expect(page.getByLabel('Access key', { exact: true })).toHaveCount(0);
    await expect(page.getByLabel('Secret key', { exact: true })).toHaveCount(0);
    const demoBucket = `console-demo-${Date.now()}`;
    const tree = page.getByRole('navigation', { name: 'S3 buckets' });
    await resourceAction(page, tree.getByRole('button', { name: 'S3', exact: true }), 'Create bucket…');
    await page.getByLabel('New bucket', { exact: true }).fill(demoBucket);
    await page.getByRole('button', { name: 'Create bucket', exact: true }).click();
    await tree.getByRole('button', { name: demoBucket, exact: true }).click();
    await resourceAction(page, tree.getByRole('button', { name: demoBucket, exact: true }), 'Upload object…');
    await page.getByLabel('Object key', { exact: true }).fill('example.txt');
    await page.getByLabel('Object file').setInputFiles({ name: 'example.txt', mimeType: 'text/plain', buffer: Buffer.from('CROWDB console demo\n') });
    await page.getByRole('button', { name: 'Upload object', exact: true }).click();
    await page.getByRole('table', { name: 'S3 objects' }).getByRole('button', { name: 'example.txt', exact: true }).click();
    await resourceAction(page, page.getByRole('heading', { level: 1 }), 'Preview first 4 KiB');
    await expect(page.getByLabel('Object preview', { exact: true })).toContainText('CROWDB console demo', { timeout: 3000 });
    const locationsResponse = await request.get(`/api/access/s3-inspect/locations?bucket=${demoBucket}&key=example.txt&limit=20`);
    expect(locationsResponse.ok(), await locationsResponse.text()).toBeTruthy();
    const locations = await locationsResponse.json();
    expect(locations.locations).toHaveLength(1);
    const chunkId = locations.locations[0].chunk_id;
    const response = await request.get(`/api/chunks/${chunkId}`);
    expect(response.ok(), await response.text()).toBeTruthy();
    const detail = await response.json();
    const strip = [...detail.chunk.strips].sort((left, right) => left.strip_sequence - right.strip_sequence)[0];
    expect(strip).toBeDefined();
    const segment = (strip.strip.MirrorStrip?.segments ?? strip.strip.EcStrip?.segments)[0];
    const diskId = BigInt(segment.disk_id.high).toString(16).padStart(16, '0') + BigInt(segment.disk_id.low).toString(16).padStart(16, '0');
    const placement = detail.placements.find((value: { disk_id: string }) => value.disk_id.replace(/-/g, '').toLowerCase() === diskId);
    expect(placement).toBeDefined();

    await page.getByTestId('domain-chunk').click();
    await page.getByLabel('Chunk type').selectOption('5');
    await page.getByLabel('Exact Chunk ID', { exact: true }).fill(chunkId);
    await page.getByRole('button', { name: 'Lookup ID', exact: true }).click();
    const card = page.getByLabel('Chunk strips').getByTestId('chunk-strip').filter({ has: page.getByRole('button', { name: new RegExp(`^Sequence ${strip.strip_sequence} ·`) }) });
    await card.getByRole('button', { name: /^(Mirror 1|Data 0)( · unavailable)?$/, exact: true }).click();
    const properties = page.getByLabel('Chunk properties');
    for (const [field, expected] of [['Rack', placement.rack_id], ['Node', placement.node_id], ['Diskgroup', placement.disk_group_id], ['Disk', diskId]]) {
      await expect(properties.locator('dt').filter({ hasText: new RegExp(`^${field}$`) }).locator('..').locator('dd')).toHaveText(String(expected));
    }
    await page.getByTestId('domain-s3').click();
    await resourceAction(page, page.getByRole('heading', { level: 1 }), 'Delete object…');
    await page.getByRole('button', { name: 'Delete object', exact: true }).click();
    await resourceAction(page, tree.getByRole('button', { name: demoBucket, exact: true }), 'Delete empty bucket…');
    await page.getByRole('button', { name: 'Delete empty bucket', exact: true }).click();
    await expect(tree).not.toContainText(demoBucket);
  });
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
