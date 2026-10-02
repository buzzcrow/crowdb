// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
import { execFileSync, spawnSync } from 'node:child_process';
import { test, expect } from '../fixtures/realBackend';

const name = `crowdb-console-s3-${process.pid}`;
const docker = (...args: string[]) => execFileSync('docker', args, { encoding: 'utf8', timeout: 10000 }).trim();
let origin: string;
test.beforeAll(async ({ request }) => {
  docker('run', '-d', '--rm', '--name', name, '-p', '127.0.0.1::9000',
    '-e', 'MINIO_ROOT_USER=console-test', '-e', 'MINIO_ROOT_PASSWORD=console-test-secret',
    'minio/minio:RELEASE.2025-09-07T16-13-09Z-cpuv1', 'server', '/data');
  origin = `http://${docker('port', name, '9000/tcp')}`;
  await expect.poll(() => {
    const result = spawnSync('docker', ['logs', name], { encoding: 'utf8', timeout: 10000 });
    if (result.status !== 0) throw new Error(result.stderr || String(result.error));
    return result.stdout + result.stderr;
  }, { timeout: 3000, intervals: [100] }).toContain('API:');
  expect((await request.get(`${origin}/minio/health/ready`)).status()).toBe(200);
  const response = await request.post('/api/access/connections', { data: { protocol: 's3', origin } });
  expect(response.ok(), await response.text()).toBeTruthy();
});
test.afterAll(() => { docker('rm', '-f', name); });

test('S3 real SigV4 bucket CRUD and multipart object round trip', async ({ page }) => {
  await page.goto('/?domain=S3');
  await page.getByLabel('Access key', { exact: true }).fill('console-test');
  await page.getByLabel('Secret key', { exact: true }).fill('console-test-secret');
  await page.getByRole('button', { name: 'Create object demo', exact: true }).click();
  await expect(page.getByRole('status').filter({ hasText: 'Demo bucket and object created' })).toBeVisible({ timeout: 3000 });
  const buckets = page.getByRole('navigation', { name: 'S3 buckets' });
  const bucket = buckets.getByRole('button', { name: /^console-demo-/ });
  await expect(bucket).toHaveCount(1);
  await bucket.click();
  await expect(page.getByRole('table', { name: 'S3 objects' })).toContainText('example.txt');
  await page.getByLabel('Object key', { exact: true }).fill('multipart a/中文.txt');
  await page.getByLabel('Object file').setInputFiles({ name: 'multipart.txt', mimeType: 'text/plain', buffer: Buffer.alloc(9 * 1024 * 1024, 'x') });
  await page.getByRole('button', { name: 'Upload', exact: true }).click();
  await expect(page.getByRole('table', { name: 'S3 objects' }).getByRole('button', { name: 'multipart a/中文.txt', exact: true })).toBeVisible({ timeout: 3000 });
  await page.getByRole('table', { name: 'S3 objects' }).getByRole('button', { name: 'multipart a/中文.txt', exact: true }).click();
  await page.getByRole('button', { name: 'Preview first 4 KiB' }).click();
  await expect(page.getByRole('status').filter({ hasText: 'Loaded first 4 KiB' })).toBeVisible({ timeout: 3000 });
  expect(JSON.parse((await page.locator('main aside pre').textContent())!).preview).toBe('x'.repeat(4096));
  page.on('dialog', dialog => dialog.accept());
  await page.getByRole('button', { name: 'Delete object', exact: true }).click();
  await expect(page.getByRole('table', { name: 'S3 objects' })).not.toContainText('multipart a/中文.txt');
  await page.getByRole('button', { name: 'Clean object demo', exact: true }).click();
  await expect(buckets.getByRole('button', { name: /^console-demo-/ })).toHaveCount(0, { timeout: 3000 });
});
