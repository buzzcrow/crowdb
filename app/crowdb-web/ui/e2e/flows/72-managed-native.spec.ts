// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
import { test, expect } from '../fixtures/realBackend';
import { step } from '../fixtures/stepTimer';

test.use({ actionTimeout: 3000 });

// Baseline: 6.0s (2026-10-04); actual monitor and all native services.
test('Managed native KV, Iceberg, S3 and Chunk operations retain hardware boundaries', async ({ page, request }) => {
  await step('managed hardware capability rejection', async () => {
    await page.goto('/');
    await expect(page.getByTestId('managed-monitor-phase')).toContainText('ready', { timeout: 3000 });
    await expect(page.getByRole('button', { name: 'Add Rack' })).toHaveCount(0);
    await expect(page.getByLabel('Management token')).toHaveCount(0);
    for (const path of ['/api/racks', '/api/cluster/init', '/internal/reset', '/api/diskdb/scan', '/api/diskdb/recalc', '/api/diskdb/compact', '/api/diskdb/rebuild']) {
      const response = await request.post(path, { data: {} });
      expect(response.status(), path).toBe(503);
    }
  });
  await step('managed user KV mutation and read', async () => {
    await page.getByTestId('domain-kv').click();
    await page.getByText(/^KV actions · Store/).click();
    await page.getByTestId('kv-store-select').selectOption('0');
    await page.getByTestId('kv-group-select').selectOption('1');
    await page.getByLabel('Put key').fill('console_native_ui_demo');
    await page.getByLabel('Put value').fill('native-value');
    const put = page.waitForResponse(response => response.url().endsWith('/kv/put'));
    await page.getByRole('button', { name: 'Put', exact: true }).click();
    expect((await put).ok()).toBeTruthy();
    await page.getByLabel('Get key').fill('console_native_ui_demo');
    await page.getByRole('button', { name: 'Get', exact: true }).click();
    await expect(page.getByTestId('kv-get-result')).toHaveText('native-value', { timeout: 3000 });
    const deleted = await request.post('/api/stores/0/groups/1/kv/delete', { data: { key: 'console_native_ui_demo' } });
    expect(deleted.ok(), await deleted.text()).toBeTruthy();
  });

  await step('managed Iceberg schema and owned cleanup', async () => {
    await page.getByTestId('domain-iceberg').click();
    await expect(page.getByLabel('Catalog write token')).toHaveCount(0);
    await page.getByText('Catalog actions', { exact: true }).click();
    await page.getByRole('button', { name: 'Create metadata demo', exact: true }).click();
    await expect(page.getByRole('status').filter({ hasText: 'Demo namespace and table created' })).toBeVisible({ timeout: 3000 });
    await page.getByRole('navigation', { name: 'Iceberg tree' }).getByRole('button', { name: /^console_demo_/ }).click();
    await page.getByRole('navigation', { name: 'Iceberg tree' }).getByRole('button', { name: 'example', exact: true }).click();
    await expect(page.getByRole('button', { name: 'Refresh table' })).toBeVisible({ timeout: 3000 });
    await page.getByRole('button', { name: 'Schema', exact: true }).click();
    await expect(page.getByRole('table', { name: 'Table schema', exact: true })).toContainText('long');
    page.on('dialog', dialog => dialog.accept());
    await page.getByRole('navigation', { name: 'Iceberg breadcrumbs' }).getByRole('button', { name: 'Catalog', exact: true }).click();
    await page.getByText('Catalog actions', { exact: true }).click();
    await page.getByRole('button', { name: 'Clean metadata demo', exact: true }).click();
    await expect(page.getByRole('navigation', { name: 'Iceberg tree' })).not.toContainText('console_demo_');
  });

  await step('managed S3 preview and actual block placement', async () => {
    await page.getByTestId('domain-s3').click();
    await expect(page.getByLabel('Access key', { exact: true })).toHaveCount(0);
    await expect(page.getByLabel('Secret key', { exact: true })).toHaveCount(0);
    await page.getByText('S3 actions', { exact: true }).click();
    await page.getByRole('button', { name: 'Create object demo', exact: true }).click();
    await expect(page.getByRole('status').filter({ hasText: 'Demo bucket and object created' })).toBeVisible({ timeout: 3000 });
    const demoScope = await page.getByText(/^Demo scope: console-demo-/).textContent();
    const demoBucket = demoScope!.match(/console-demo-[a-f0-9]+/)![0];
    await page.getByRole('navigation', { name: 'S3 buckets' }).getByRole('button', { name: demoBucket, exact: true }).click();
    await page.getByRole('table', { name: 'S3 objects' }).getByRole('button', { name: 'example.txt', exact: true }).click();
    await page.getByText('Object actions', { exact: true }).click();
    await page.getByRole('button', { name: 'Preview first 4 KiB' }).click();
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
    await page.getByRole('table', { name: 'Chunks' }).getByRole('button', { name: chunkId, exact: true }).click();
    const card = page.getByLabel('Chunk strips').getByTestId('chunk-strip').filter({ has: page.getByRole('button', { name: new RegExp(`^Sequence ${strip.strip_sequence} ·`) }) });
    await card.getByRole('button', { name: /^(Mirror 1|Data 0)( · unavailable)?$/, exact: true }).click();
    const properties = page.getByLabel('Chunk properties');
    for (const [field, expected] of [['Rack', placement.rack_id], ['Node', placement.node_id], ['Diskgroup', placement.disk_group_id], ['Disk', diskId]]) {
      await expect(properties.locator('dt').filter({ hasText: new RegExp(`^${field}$`) }).locator('..').locator('dd')).toHaveText(String(expected));
    }
    await page.getByTestId('domain-s3').click();
    await page.getByRole('navigation', { name: 'S3 breadcrumbs' }).getByRole('button', { name: 'S3', exact: true }).click();
    await page.getByText('S3 actions', { exact: true }).click();
    await page.getByRole('button', { name: 'Clean object demo', exact: true }).click();
    await expect(page.getByRole('navigation', { name: 'S3 buckets' })).not.toContainText(demoBucket);
  });
});

test('Managed native authority outage hides stale topology and retains monitor observation', async ({ page }) => {
  const pid = Number(process.env.CROWDB_NATIVE_KV_PID);
  expect(pid, 'Run the owned host-native managed runner').toBeGreaterThan(0);
  await page.goto('/');
  await expect(page.getByTestId('managed-source')).toHaveText('Source: Group 0');
  await page.clock.install();
  process.kill(pid, 'SIGSTOP');
  try {
    // One 3-second UI poll plus the backend's 3-second authority budget.
    const failed = page.waitForResponse(response => response.url().endsWith('/api/preview') && response.status() === 503, { timeout: 10000 });
    await page.clock.runFor(3001);
    const response = await failed;
    const body = await response.json();
    expect(body.reason).toBe('group0_unavailable');
    expect(body.monitor.services.kv.pid).toBe(pid);
    await expect(page.getByTestId('managed-unavailable')).toContainText('Group 0 is unavailable');
    await expect(page.getByRole('complementary', { name: 'Cluster tree sidebar' }).getByRole('button', { name: 'N-1', exact: true })).toHaveCount(0);
    await expect(page.getByRole('region', { name: 'Monitor status' })).toBeVisible();
  } finally {
    process.kill(pid, 'SIGCONT');
  }
  await page.clock.runFor(3001);
  await expect(page.getByTestId('managed-unavailable')).toHaveCount(0);
  await expect(page.getByRole('complementary', { name: 'Cluster tree sidebar' })).toBeVisible();
});
