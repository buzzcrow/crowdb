// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
import { test, expect } from '../fixtures/realBackend';

test.use({ actionTimeout: 3000 });

test('Managed native KV, Iceberg, S3 and Chunk operations retain hardware boundaries', async ({ page, request }) => {
  const token = process.env.CROWDB_ICEBERG_MANAGE_TOKEN!;
  expect(token).toMatch(/^[a-f0-9]{64}$/);
  await page.goto('/');
  await expect(page.getByTestId('managed-monitor-phase')).toContainText('ready', { timeout: 3000 });
  await expect(page.getByRole('button', { name: 'Add Rack' })).toHaveCount(0);
  await page.getByLabel('Management token').fill(token);
  await page.getByRole('button', { name: 'Authorize', exact: true }).click();
  await expect(page.getByText('Logical operations enabled')).toBeVisible({ timeout: 3000 });
  for (const path of ['/api/racks', '/api/cluster/init', '/internal/reset']) {
    const response = await request.post(path, { headers: { Authorization: `Bearer ${token}` }, data: {} });
    expect(response.status(), path).toBe(503);
  }
  await page.getByTestId('domain-kv').click();
  await page.getByTestId('kv-view-data').click();
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

  await page.getByTestId('domain-iceberg').click();
  await page.getByText('Catalog write authorization', { exact: true }).click();
  await page.getByLabel('Catalog write token', { exact: true }).fill(process.env.CROWDB_ICEBERG_WRITE_TOKEN!);
  await page.getByRole('button', { name: 'Use catalog write token', exact: true }).click();
  await page.getByRole('button', { name: 'Create metadata demo', exact: true }).click();
  await expect(page.getByRole('status').filter({ hasText: 'Demo namespace and table created' })).toBeVisible({ timeout: 3000 });
  await page.getByRole('navigation', { name: 'Iceberg namespaces' }).getByRole('button', { name: /^console_demo_/ }).click();
  await page.getByRole('navigation', { name: 'Iceberg tables' }).getByRole('button', { name: 'example', exact: true }).click();
  await expect(page.getByRole('button', { name: 'Refresh table' })).toBeVisible({ timeout: 3000 });
  await page.getByRole('button', { name: 'Schema', exact: true }).click();
  await expect(page.getByRole('navigation', { name: 'Table sections' }).locator('..')).toContainText('schema-id');
  page.on('dialog', dialog => dialog.accept());
  await page.getByRole('button', { name: 'Clean metadata demo', exact: true }).click();
  await expect(page.getByRole('navigation', { name: 'Iceberg namespaces' })).not.toContainText('console_demo_');

  await page.getByTestId('domain-s3').click();
  await page.getByLabel('Access key', { exact: true }).fill(process.env.AWS_ACCESS_KEY_ID!);
  await page.getByLabel('Secret key', { exact: true }).fill(process.env.AWS_SECRET_ACCESS_KEY!);
  await page.getByRole('button', { name: 'Create object demo', exact: true }).click();
  await expect(page.getByRole('status').filter({ hasText: 'Demo bucket and object created' })).toBeVisible({ timeout: 3000 });
  const demoScope = await page.getByText(/^Demo scope: console-demo-/).textContent();
  const demoBucket = demoScope!.match(/console-demo-[a-f0-9]+/)![0];
  await page.getByRole('navigation', { name: 'S3 buckets' }).getByRole('button', { name: demoBucket, exact: true }).click();
  await page.getByRole('table', { name: 'S3 objects' }).getByRole('button', { name: 'example.txt', exact: true }).click();
  await page.getByRole('button', { name: 'Preview first 4 KiB' }).click();
  await expect(page.locator('main aside pre')).toContainText('CROWDB console demo', { timeout: 3000 });

  await page.getByTestId('domain-chunk').click();
  await page.getByLabel('Chunk type').selectOption('5');
  await page.getByRole('button', { name: 'Query chunks', exact: true }).click();
  const chunks = page.getByRole('table', { name: 'Chunks' }).getByRole('button');
  await expect(chunks).not.toHaveCount(0, { timeout: 3000 });
  await chunks.nth(0).click();
  const strips = page.getByLabel('Chunk strips').getByRole('button');
  await expect(strips).not.toHaveCount(0, { timeout: 3000 });
  await strips.nth(0).click();
  await expect(page.locator('main aside').filter({ hasText: 'Strip sequence' })).toContainText('Rack 1 / Node 1 / DG 101', { timeout: 3000 });
  await page.getByTestId('domain-s3').click();
  await page.getByRole('button', { name: 'Clean object demo', exact: true }).click();
  await expect(page.getByRole('navigation', { name: 'S3 buckets' })).not.toContainText(demoBucket);
});
