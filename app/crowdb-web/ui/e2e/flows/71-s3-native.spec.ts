// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
import { test, expect } from '../fixtures/realBackend';
import { step } from '../fixtures/stepTimer';

// Large transfer acceptance is separate from routine page behavior tests.
// Baseline: 3.0s (2026-10-04); actual CROWDB multipart acceptance.
test('S3 native multipart upload, HEAD, bounded preview and full round trip', async ({ page, request }) => {
  const bucket = `console-multipart-${process.pid}-${Date.now()}`;
  const key = 'multipart a/中文.txt';
  const bucketPath = `/api/access/s3/${bucket}`;
  const objectPath = `${bucketPath}/multipart%20a/%E4%B8%AD%E6%96%87.txt`;
  const bytes = Buffer.alloc(9 * 1024 * 1024, 'x');
  await step('native bucket setup', async () => {
    const response = await request.put(bucketPath);
    expect(response.status(), await response.text()).toBe(200);
  });
  try {
    await step('native S3 DOM setup', async () => {
      await page.goto('/?domain=S3');
      await expect(page.getByLabel('Access key', { exact: true })).toHaveCount(0);
      await page.getByRole('navigation', { name: 'S3 buckets' }).getByRole('button', { name: bucket, exact: true }).click();
      await page.getByText('Bucket actions', { exact: true }).click();
      await page.getByLabel('Object key', { exact: true }).fill(key);
      await page.getByLabel('Object file').setInputFiles({ name: 'multipart.txt', mimeType: 'text/plain', buffer: bytes });
    });
    await step('native 9 MiB multipart mutation and DOM refresh', async () => {
      await page.getByRole('button', { name: 'Upload', exact: true }).click();
      await expect(page.getByRole('table', { name: 'S3 objects' }).getByRole('button', { name: key, exact: true })).toBeVisible();
    });
    await step('native object HEAD and bounded preview', async () => {
      await page.getByRole('table', { name: 'S3 objects' }).getByRole('button', { name: key, exact: true }).click();
      await expect(page.getByLabel('Object metadata')).toContainText(String(bytes.length));
      await page.getByText('Object actions', { exact: true }).click();
      await page.getByRole('button', { name: 'Preview first 4 KiB' }).click();
      await expect(page.getByLabel('Object preview', { exact: true })).toHaveText('x'.repeat(4096));
    });
    await step('native full object byte verification', async () => {
      const response = await request.get(objectPath);
      expect(response.status()).toBe(200);
      expect((await response.body()).equals(bytes)).toBe(true);
    });
    await step('native object delete and DOM refresh', async () => {
      page.once('dialog', dialog => dialog.accept());
      await page.getByRole('button', { name: 'Delete object', exact: true }).click();
      await expect(page.getByRole('table', { name: 'S3 objects' })).not.toContainText(key);
    });
  } finally {
    await step('native owned-resource teardown', async () => {
      const object = await request.delete(objectPath);
      expect(object.status(), await object.text()).toBe(204);
      const response = await request.delete(bucketPath);
      expect(response.status(), await response.text()).toBe(204);
    });
  }
});
