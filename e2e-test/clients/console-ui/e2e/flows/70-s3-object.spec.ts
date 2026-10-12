// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
import { test, expect } from '../fixtures/realBackend';
import { step } from '../fixtures/stepTimer';

// Baseline: new actual bucket/part windows (2026-10-04).
test('native diagnostics: S3 bucket windows and multipart markers replace rows', async ({ page, request }) => {
  const prefix = `windows-${process.pid}-${Date.now()}-`;
  const buckets = Array.from({ length: 21 }, (_, index) => `${prefix}${String(index).padStart(2, '0')}`);
  const path = `/api/access/s3/${buckets[20]}/pending.bin`;
  let upload = '';
  const created: string[] = [];
  try {
    await step('native 21 bucket setup', async () => {
      for (let start = 0; start < buckets.length; start += 4) {
        await Promise.all(buckets.slice(start, start + 4).map(async bucket => {
          const response = await request.put(`/api/access/s3/${bucket}`);
          expect(response.status(), await response.text()).toBe(200); created.push(bucket);
        }));
      }
    });
    const response = await request.post(`${path}?uploads`);
    expect(response.ok(), await response.text()).toBeTruthy();
    upload = (await response.text()).match(/<UploadId>([^<]+)<\/UploadId>/)![1];
    await step('native 101 small pending parts', async () => {
      for (let first = 1; first <= 101; first += 4) {
        const responses = await Promise.all(Array.from({ length: Math.min(4, 102 - first) }, (_, index) => request.put(`${path}?uploadId=${upload}&partNumber=${first + index}`, { data: Buffer.alloc(1024, first + index) })));
        for (const response of responses) expect(response.ok(), await response.text()).toBeTruthy();
      }
    });
    await page.goto('/?domain=S3');
    await page.getByLabel('Filter loaded buckets', { exact: true }).fill(prefix);
    const table = page.getByRole('table', { name: 'S3 bucket list', exact: true });
    await expect(table.getByRole('row')).toHaveCount(21);
    expect(await table.getByRole('button').allTextContents()).toEqual(buckets.slice(0, 20));
    await page.getByRole('button', { name: 'Next buckets', exact: true }).click();
    await expect(table.getByRole('row')).toHaveCount(2);
    await expect(table).toContainText(buckets[20]);
    await page.goBack(); await expect(table.getByRole('row')).toHaveCount(21);
    await page.goForward(); await expect(table.getByRole('row')).toHaveCount(2);
    await page.getByRole('navigation', { name: 'S3 buckets', exact: true }).getByRole('button', { name: buckets[20], exact: true }).click();
    await page.getByText('Bucket actions', { exact: true }).click();
    await page.getByRole('button', { name: 'List multipart uploads', exact: true }).click();
    await page.getByRole('button', { name: 'Inspect parts', exact: true }).click();
    const parts = page.getByRole('region', { name: 'Object metadata', exact: true });
    await expect(parts.getByRole('table', { name: 'Multipart parts', exact: true }).getByRole('row')).toHaveCount(101);
    await page.getByRole('button', { name: 'Next parts page', exact: true }).click();
    await expect(parts).toContainText('101');
    await expect(page.getByRole('button', { name: 'Next parts page', exact: true })).toHaveCount(0);
    await page.goBack();
    await expect(page.getByRole('button', { name: 'Next parts page', exact: true })).toBeVisible();
    expect((await request.delete(`${path}?uploadId=${upload}`)).status()).toBe(204);
    await page.getByRole('button', { name: 'Next parts page', exact: true }).click();
    await expect(page.getByRole('alert').filter({ hasText: '404' })).toBeVisible();
    await expect(parts.getByRole('table', { name: 'Multipart parts', exact: true }).getByRole('row')).toHaveCount(1);
    await expect(page.getByRole('button', { name: 'Next parts page', exact: true })).toHaveCount(0);
    await expect(page.getByRole('heading', { level: 1 })).toHaveText('pending.bin');
    await page.getByRole('button', { name: 'Refresh parts', exact: true }).click();
    await expect(page.getByRole('alert').filter({ hasText: '404' })).toBeVisible();
    await expect(parts.getByRole('table', { name: 'Multipart parts', exact: true }).getByRole('row')).toHaveCount(1);
  } finally {
    if (upload) expect((await request.delete(`${path}?uploadId=${upload}`)).status()).toBe(204);
    for (let start = 0; start < created.length; start += 4) {
      const responses = await Promise.all(created.slice(start, start + 4).map(bucket => request.delete(`/api/access/s3/${bucket}`)));
      for (const response of responses) expect(response.status(), await response.text()).toBe(204);
    }
  }
});
